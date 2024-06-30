#![feature(seek_stream_len)]
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Condvar, Mutex};

use ringbuf::{storage::Array, traits::{Consumer as _, RingBuffer as _}, LocalRb};
//use crossbeam_utils::atomic::AtomicCell;

const RINGBUF_LEN: usize = 5;
type Ringbuf<T> = LocalRb<Array<(u64, T), RINGBUF_LEN>>;

// Unfortunately we cannot use the ringbuf crate's lock free features because they do not allow for
// overwrites (as those require changing both cursors at once).

//type RbProducer<T> = CachingProd<Arc<Ringbuf<T>>>;
//type RbConsumer<T> = CachingCons<Arc<Ringbuf<T>>>;

pub mod cloneable;

enum State {
    Downloading,
    Done(Option<std::io::Error>),
}

struct Shared<T> {
    state: State,
    ringbuf: Ringbuf<T>,
    total_length: Option<u64>,
}

pub struct Writer<T: AsRef<[u8]>, File: Write> {
    file: File,
    shared: Arc<Mutex<Shared<T>>>,
    condvar: Arc<Condvar>,
    position: u64,
}

pub struct Reader<T: AsRef<[u8]>, File: Read + Seek> {
    file: File,
    shared: Arc<Mutex<Shared<T>>>,
    position: u64,
    condvar: Arc<Condvar>,
}

pub fn create_pair<R: Read + Seek, W: Write, B: AsRef<[u8]>>(backing_reader: R, backing_writer: W, initial_write_pos: u64) -> (Reader<B, R>, Writer<B, W>) {
    let shared = Arc::new(Mutex::new(Shared {
        state: State::Downloading,
        ringbuf: Ringbuf::default(),
        total_length: None,
    }));
    let condvar = Arc::new(Condvar::new());
    (Reader {
        file: backing_reader,
        shared: shared.clone(),
        condvar: condvar.clone(),
        position: 0,
    },
    Writer {
        file: backing_writer,
        shared,
        condvar,
        position: initial_write_pos,
    })
}

impl<T: AsRef<[u8]>, File: Write> Writer<T, File> {
    pub fn write(&mut self, data: T) -> std::io::Result<()> {
        let d2 = data.as_ref();
        let len = d2.len();
        self.file.write_all(d2)?;
        self.shared.lock().unwrap().ringbuf.push_overwrite((self.position, data));
        self.condvar.notify_one();
        self.position+=len as u64;
        Ok(())
    }

    pub fn notify_eof(&self, error: Option<std::io::Error>) {
        self.shared.lock().unwrap().state = State::Done(error);
        self.condvar.notify_all();
    }

    pub fn notify_stream_len(&self, len: u64) {
        self.shared.lock().unwrap().total_length = Some(len);
        self.condvar.notify_all();
    }

    pub fn get_pos(&self) -> u64 {
        self.position
    }
}

impl<T: AsRef<[u8]>, File: Read + Seek> Reader<T, File> {
    pub fn get_total_length(&self) -> Option<u64> {
        self.shared.lock().unwrap().total_length
    }
}

impl<T: AsRef<[u8]>, File: Read + Seek> std::io::Read for Reader<T, File> {
    fn read(&mut self, mut out_buf: &mut [u8]) -> std::io::Result<usize> {
        let mut mutex = self.shared.lock().unwrap();
        loop {
            let mut total_out = 0usize;

            while let Some((pos, buf)) = mutex.ringbuf.first() {
                let pos=*pos;
                let buf = buf.as_ref();
                if pos + buf.len() as u64 <= self.position {
                    mutex.ringbuf.try_pop();
                    continue;
                }
                if pos <= self.position {
                    let start_idx = (self.position - pos) as usize;
                    let buf2 = &buf[start_idx..];
                    let count = buf2.len().min(out_buf.len());
                    out_buf[..count].copy_from_slice(&buf2[..count]);
                    total_out += count;
                    out_buf = &mut out_buf[count..];
                    self.position += count as u64;

                    if pos + buf.len() as u64 <= self.position {
                        mutex.ringbuf.try_pop();
                    }

                    if out_buf.is_empty() {
                        break;
                    }
                } else {
                    break;
                }
            }


            if total_out != 0 {
                return Ok(total_out);
            }
            let res = self.file.seek(SeekFrom::Start(self.position)).and_then(|_| self.file.read(&mut out_buf));
            if let Ok(0) = res {
                if let State::Done(res) = &mut mutex.state {
                    return match res.take() {
                        None => Ok(0),
                        Some(e) => Err(e),
                    };
                } 
                mutex = self.condvar.wait(mutex).unwrap();
                continue;
            }
            if let Ok(count) = res {
                self.position += count as u64;
            }
            return res;
        }
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
            SeekFrom::End(offset) => {
                // XXX should this just be computed from stream_len() instead of replicating the
                // logic?
                let mut guard = self.shared.lock().unwrap();
                loop {
                    if let Some(stream_len) = guard.total_length {
                        self.position = stream_len
                            .checked_add_signed(offset)
                            .ok_or_else(|| std::io::Error::other(format!("Attempt to seek with overflow (position was {}, offset was {})", self.position, offset)))?;
                        return Ok(self.position);
                    }
                    if let State::Done(error) = &mut guard.state {
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

    fn stream_len(&mut self) -> std::io::Result<u64> {
        let mut mutex = self.shared.lock().unwrap();
        loop {
            if let Some(len) = mutex.total_length {
                return Ok(len);
            }
            if let State::Done(e) = &mut mutex.state {
                match e.take() {
                    Some(e) => return Err(e),
                    None => return self.file.stream_len()
                }
            }
            mutex = self.condvar.wait(mutex).unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use ringbuf::traits::Producer;
    #[test]
    fn test_reader() {
        struct FakeFile {
            eof: bool,
            position: u64,
        }

        impl std::io::Read for FakeFile {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.eof {
                    Ok(0)
                } else {
                    buf[..8].copy_from_slice(&self.position.to_ne_bytes());
                    Ok(8)
                }
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

        let mut reader = Reader {
            file: FakeFile {position:0, eof: false},
            shared: Arc::new(Mutex::new(Shared {ringbuf: Ringbuf::default(), state: State::Downloading, total_length: None})),
            condvar: Arc::new(Condvar::new()),
            position:0,
        };
        {
            let ringbuf = &mut reader.shared.lock().unwrap().ringbuf;
            ringbuf.try_push((0,[0,1,2,3].as_slice())).unwrap();
            ringbuf.try_push((4,[4,5,6,7].as_slice())).unwrap();
            ringbuf.try_push((8,[8,9,10,11,12,13,14,15,16].as_slice())).unwrap();
            ringbuf.try_push((17,[17,18].as_slice())).unwrap();
        }

        let mut buf = [0u8;8];
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, [0,1,2,3,4,5,6,7]);
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, [8,9,10,11,12,13,14,15]);
        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(buf[..3], [16,17,18]);
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, 19u64.to_ne_bytes());

        {
            let mut shared = reader.shared.lock().unwrap();
            shared.ringbuf.try_push((27,[1,2,3,4,5].as_slice())).unwrap();
            shared.state=State::Done(None);
        }
        assert_eq!(reader.read(&mut buf).unwrap(), 5);
        assert_eq!(buf[..5], [1,2,3,4,5]);
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, 32u64.to_ne_bytes());
        reader.file.eof=true;
        {
            let mut shared = reader.shared.lock().unwrap();
            shared.ringbuf.try_push((0,[0,0,0,0,0,0,0].as_slice())).unwrap();
            shared.ringbuf.try_push((40,[1,2,3,4,5].as_slice())).unwrap();
            shared.ringbuf.try_push((80,[99,99,99,99,99,99,99,99].as_slice())).unwrap();
        }
        assert_eq!(reader.read(&mut buf).unwrap(), 5);
        assert_eq!(buf[..5], [1,2,3,4,5]);
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_reader_and_writer() {
        struct ReadWrapper{
            data: Arc<Mutex<Vec<u8>>>,
            position: usize,
            read_call_count: usize,
            eof: Arc<AtomicBool>,
        }
        impl Read for ReadWrapper {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.read_call_count+=1;
                let data = &self.data.lock().unwrap()[self.position..];
                let count = data.len().min(buf.len());
                buf[..count].copy_from_slice(&data[..count]);
                Ok(count)
            }
        }

        impl Seek for ReadWrapper {
            fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
                let pos = match pos {
                    std::io::SeekFrom::Start(pos) => pos,
                    std::io::SeekFrom::Current(offset) => {(self.position as u64).checked_add_signed(offset).ok_or(std::io::Error::other("attempted seek past beginning of file"))?},
                    std::io::SeekFrom::End(offset) => {
                        if !self.eof.load(Ordering::Relaxed) {
                            return Err(std::io::Error::other("Should not have seeked relative to end before the writer finished!"));
                        }
                        (self.data.lock().unwrap().len() as u64).checked_add_signed(offset).ok_or(std::io::Error::other("attempted seek past beginning of file"))?
                    }
                } as usize;
                if pos > self.data.lock().unwrap().len() {
                    return Err(std::io::Error::other("attempt seek past end of file"));
                }
                self.position = pos;
                Ok(self.position as u64)
            }
        }
        
        struct WriteWrapper(Arc<Mutex<Vec<u8>>>);
        impl Write for WriteWrapper {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.write_all(data)?;
                Ok(data.len())
            }
            fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
                self.0.lock().unwrap().extend(data);
                Ok(())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let backing_store = Arc::new(Mutex::new(Vec::new()));

        let backing_reader = ReadWrapper{
            data: backing_store.clone(),
            position: 0,
            read_call_count: 0,
            eof: Arc::new(AtomicBool::new(false)),
        };

        let backing_writer = WriteWrapper(backing_store);

        let (mut reader, mut writer) = create_pair::<_, _, &'static [u8]>(backing_reader, backing_writer, 0);

        writer.write(&[1,2,3,4,5]).unwrap();
        writer.write(&[6,7,8,9,10]).unwrap();
        writer.write(&[11,12,13,14,15]).unwrap();
        writer.write(&[16,17,18,19,20]).unwrap();
        writer.write(&[21,22,23,24,25]).unwrap();
        writer.write(&[26,27,28,29,30]).unwrap();
        writer.write(&[31,32,33,34,35]).unwrap();

        let mut buf = [0u8;15];

        assert_eq!(reader.read(&mut buf).unwrap(), 15);
        assert_eq!(reader.file.read_call_count, 1);
        assert_eq!(buf, [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]);

        assert_eq!(reader.read(&mut buf).unwrap(), 15);
        assert_eq!(reader.file.read_call_count, 1);
        assert_eq!(buf, [16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]);

        let eof = reader.file.eof.clone();

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(1));
            eof.store(true, Ordering::Relaxed);
            writer.notify_eof(None);
        });

        assert!(reader.seek(std::io::SeekFrom::End(-10)).is_ok());

        assert_eq!(reader.read(&mut buf).unwrap(), 10);
        assert_eq!(reader.file.read_call_count, 2);
        assert_eq!(buf[..10], [26,27,28,29,30,31,32,33,34,35]);
        
    }
}
