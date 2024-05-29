use std::{fs::File, io::{Read, Seek, SeekFrom}, sync::{Arc, Condvar, Mutex}};

use ringbuf::{traits::{Producer as _, Consumer as _}, CachingProd, CachingCons, StaticRb};
use crossbeam_utils::atomic::AtomicCell;

const RINGBUF_LEN: usize = 5;
type Ringbuf<T> = StaticRb<T, RINGBUF_LEN>;

type RbProducer<T> = CachingProd<Arc<Ringbuf<T>>>;
type RbConsumer<T> = CachingCons<Arc<Ringbuf<T>>>;

enum State {
    Downloading,
    Done(Option<std::io::Error>),
}

struct Writer<T: AsRef<[u8]>> {
    file: File,
    state: AtomicCell<State>,
    condvar: Arc<Condvar>,
}

struct Reader<T: AsRef<[u8]>, File: Read + Seek> {
    file: File,
    state: Arc<Mutex<State<T>>>,
    position: u64,
    condvar: Arc<Condvar>,
}

impl<T: AsRef<[u8]>, File: Read + Seek> std::io::Read for Reader<T, File> {
    fn read(&mut self, mut out_buf: &mut [u8]) -> std::io::Result<usize> {
        let mut total_out = 0usize;
        let mut state = self.state.lock().unwrap();
        if let State::Downloading(ringbuf) = &mut *state {
            while let Some((pos, buf)) = ringbuf.first() {
                let buf = buf.as_ref();
                if *pos + buf.len() as u64 <= self.position {
                    ringbuf.try_pop();
                    continue;
                }
                if *pos <= self.position {
                    let start_idx = (*pos - self.position) as usize;
                    let buf2 = &buf[start_idx..];
                    let count = buf2.len().min(out_buf.len());
                    out_buf[..count].copy_from_slice(&buf2[..count]);
                    total_out += count;
                    out_buf = &mut out_buf[count..];
                    self.position += count as u64;
                    
                    if *pos + buf.len() as u64 <= self.position {
                        ringbuf.try_pop();
                    }
                } else {
                    break;
                }
            }
        }
        
        if total_out != 0 {
            return Ok(total_out);
        }
        let res = self.file.seek(SeekFrom::Start(self.position)).and_then(|_| self.file.read(&mut out_buf));
        if let Ok(0) = res {
            if let State::Done(opt) = &mut *state {
                return match opt.take() {
                    Some(e) => Err(e),
                    None => Ok(0),
                };
            }
            std::mem::drop(state);
        }
        else if let Ok(count) = res {
            self.position += count as u64;
        }
        res
    }
}

impl<T: AsRef<[u8]>, File: Read+Seek> Seek for Reader<T, File> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => self.position=pos,
            SeekFrom::Current(offset) => {
                self.position = self.position
                    .checked_add_signed(offset)
                    .ok_or_else(|| std::io::Error::other(format!("Attempt to seek with overflow (position was {}, offset was {})", self.position, offset)))?;
            },
            SeekFrom::End(_) => {
                let mut guard = self.state.lock().unwrap();
                loop {
                    if let State::Done(error) = &mut *guard {
                        if let Some(e) = error.take() {
                            return Err(e);
                        }
                        // XXX can we trust the end position of the file here?
                        self.position = self.file.seek(pos)?;
                        return Ok(self.position);
                    }
                    guard = self.condvar.wait(guard).unwrap();
                }
            }
        }
        Ok(self.position)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.position)
    }
}

#[cfg(test)]
mod test {
    use ringbuf::traits::Producer;

    use super::*;
    struct FakeFile {
        position: u64,
    }

    impl std::io::Read for FakeFile {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            buf[..8].copy_from_slice(&self.position.to_ne_bytes());
            Ok(8)
        }
    }

    impl std::io::Seek for FakeFile {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            match pos {
                SeekFrom::Start(pos) => self.position=pos,
                SeekFrom::End(pos) => self.position=(pos+999) as u64,
                SeekFrom::Current(offset) => self.position = self.position.checked_add_signed(offset).unwrap(),
            }
            Ok(self.position)
        }
    }

    #[test]
    fn test_reader() {
        let mut reader = Reader {
            file: FakeFile {position:0},
            state: Arc::new(Mutex::new(State::Downloading(LocalRb::<Heap<(u64, &'static [u8])>>::new(5)))),
            condvar: Arc::new(Condvar::new()),
            position:0,
        };
        {
            let State::Downloading(ref mut ringbuf) = *reader.state.lock().unwrap() else {
                panic!("state should not have been changed to done");
            };
            ringbuf.try_push((0,&[0,1,2,3])).unwrap();
            ringbuf.try_push((4,&[4,5,6,7])).unwrap();
        }

        let mut buf = [0u8;8];
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, [0,1,2,3,4,5,6,7]);
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, 8u64.to_ne_bytes());
    }
}
