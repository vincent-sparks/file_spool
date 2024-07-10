#![feature(seek_stream_len)]
#![feature(new_uninit)]
use std::io::{Read, Seek, SeekFrom, Write}; use std::ops::ControlFlow;
use std::sync::{Arc, Condvar, Mutex};

//use crossbeam_utils::atomic::AtomicCell;

struct RingBuf<T> {
    ring: Arc<Box<[Mutex<Option<T>>]>>,
    write_idx: usize,
}

struct RingBufReader<T> {
    ring: Arc<Box<[Mutex<Option<T>>]>>,
}

impl<T> Clone for RingBufReader<T> {
    fn clone(&self)->Self {
        RingBufReader {ring: self.ring.clone()}
    }
}

impl<T> RingBuf<T> {
    fn new(length: usize) -> Self {
        let mut ring = Box::new_uninit_slice(length);
        for slot in ring.iter_mut() {
            slot.write(Default::default());
        }
        let ring = unsafe {ring.assume_init()};
        Self {
            ring: Arc::new(ring),
            write_idx: 0,
        }
    }
    fn write(&mut self, val: T) {
        *(self.ring[self.write_idx].lock().unwrap()) = Some(val);
        self.write_idx += 1;
        self.write_idx %= self.ring.len();
    }

    fn new_reader(&self) -> RingBufReader<T> {
        RingBufReader {ring: self.ring.clone()}
    }
}

impl<T> RingBufReader<T> {
    fn iterate<R>(&self, mut f: impl FnMut(&T) -> ControlFlow<R, ()>) -> Option<R> {
        for slot in self.ring.iter() {
            if let Some(e) = slot.lock().unwrap().as_ref() {
                match f(e) {
                    ControlFlow::Continue(()) => {},
                    ControlFlow::Break(r) => return Some(r),
                }
            }
        }
        // cycle twice in case the ring buffer contains 4,5,1,2,3 and the callback can only consume
        // values in order.
        for slot in self.ring.iter() {
            if let Some(e) = slot.lock().unwrap().as_ref() {
                match f(e) {
                    ControlFlow::Continue(()) => {},
                    ControlFlow::Break(r) => return Some(r),
                }
            }
        }
        None
    }
}

// TODO cloneable I/O errors
#[derive(Default)]
enum State {
    #[default]
    Downloading,
    Success,
    Error(Option<std::io::Error>),
}

#[derive(Default)]
struct Shared {
    total_length: Option<u64>,
    state: State,
}

pub struct Writer<T: AsRef<[u8]>, WriteFile: Write> {
    file: WriteFile,
    shared: Arc<Mutex<Shared>>,
    ringbuf: RingBuf<(u64, T)>,
    position: u64,
    condvar: Arc<Condvar>,
}

pub struct Reader<T: AsRef<[u8]>, File: Read + Seek> {
    file: File,
    ringbuf: RingBufReader<(u64, T)>,
    shared: Arc<Mutex<Shared>>,
    position: u64,
    condvar: Arc<Condvar>,
}

pub struct ReaderCreator<T: AsRef<[u8]>, CreateReadFile> {
    create_read_file: CreateReadFile,
    ringbuf: RingBufReader<(u64, T)>,
    shared: Arc<Mutex<Shared>>,
    condvar: Arc<Condvar>,
}

impl<T: AsRef<[u8]>, WriteFile: Write> Writer<T, WriteFile> {
    pub fn new(ringbuf_len: usize, write_file: WriteFile, initial_write_pos: u64) -> Self {
        Self {
            file: write_file,
            ringbuf: RingBuf::new(ringbuf_len),
            position: initial_write_pos,
            condvar: Default::default(),
            shared: Default::default(),
        }
    }
    pub fn write(&mut self, data: T) -> std::io::Result<()> {
        let d2 = data.as_ref();
        let len = d2.len();
        self.file.write_all(d2)?;
        self.ringbuf.write((self.position, data));
        self.condvar.notify_one();
        self.position+=len as u64;
        Ok(())
    }

    pub fn notify_eof(&self) {
        self.shared.lock().unwrap().state = State::Success;
        self.condvar.notify_all();
    }

    pub fn notify_error(&self, error: std::io::Error) {
        self.shared.lock().unwrap().state = State::Error(Some(error));
        self.condvar.notify_all();
    }

    pub fn notify_stream_len(&self, len: u64) {
        self.shared.lock().unwrap().total_length = Some(len);
        self.condvar.notify_all();
    }

    pub fn get_pos(&self) -> u64 {
        self.position
    }

    pub fn create_reader<R: Read+Seek>(&self, file: R) -> Reader<T, R> {
        Reader {
            file,
            condvar: self.condvar.clone(),
            position: 0,
            ringbuf: self.ringbuf.new_reader(),
            shared: self.shared.clone()
        }
    }

    pub fn create_reader_creator<C>(&self, reader_creator: C) -> ReaderCreator<T, C> {
        ReaderCreator {
            create_read_file: reader_creator,
            condvar: self.condvar.clone(),
            ringbuf: self.ringbuf.new_reader(),
            shared: self.shared.clone()
        }
    }
}

impl<T: AsRef<[u8]>, File: Read + Seek, CreateFile: FnMut() -> File> ReaderCreator<T, CreateFile>  {
    pub fn create_reader_mut(&mut self) -> Reader<T, File> {
        Reader {
            file: (self.create_read_file)(),
            condvar: self.condvar.clone(),
            position: 0,
            ringbuf: self.ringbuf.clone(),
            shared: self.shared.clone()
        }
    }
}

impl<T: AsRef<[u8]>, File: Read + Seek, CreateFile: Fn() -> File> ReaderCreator<T, CreateFile>  {
    pub fn create_reader(&self) -> Reader<T, File> {
        Reader {
            file: (self.create_read_file)(),
            condvar: self.condvar.clone(),
            position: 0,
            ringbuf: self.ringbuf.clone(),
            shared: self.shared.clone()
        }
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

            self.ringbuf.iterate(|(pos, buf)| {
                    let pos=*pos;
                    let buf = buf.as_ref();
                    if pos + buf.len() as u64 <= self.position {
                        return ControlFlow::Continue(());
                    }
                    if pos <= self.position {
                        let start_idx = (self.position - pos) as usize;
                        let buf2 = &buf[start_idx..];
                        let count = buf2.len().min(out_buf.len() - total_out);
                        out_buf[total_out..count+total_out].copy_from_slice(&buf2[..count]);
                        total_out += count;
                        self.position += count as u64;

                        if out_buf.is_empty() {
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                });

            if total_out != 0 {
                return Ok(total_out);
            }
            let res = self.file.seek(SeekFrom::Start(self.position)).and_then(|_| self.file.read(&mut out_buf));
            if let Ok(0) = res {
                match &mut mutex.state {
                    State::Success => return Ok(0),
                    State::Error(e) => return Err(e.take().unwrap_or_else(|| std::io::Error::other("error message has already been consumed, no idea what it was"))),
                    State::Downloading => {
                        mutex = self.condvar.wait(mutex).unwrap();
                        continue;
                    },
                }
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
                    match &mut guard.state {
                        State::Downloading => {
                            guard = self.condvar.wait(guard).unwrap();
                            continue;
                        },
                        State::Success => {
                            return Ok(self.position);
                        },
                        State::Error(e) => {
                            return Err(e.take().unwrap_or_else(|| std::io::Error::other("someone else already took the error :/")));
                        },
                    }
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
            match &mut mutex.state {
                State::Downloading => {
                    mutex = self.condvar.wait(mutex).unwrap();
                    continue;
                },
                State::Success => {
                    return self.file.stream_len();
                },
                State::Error(e) => {
                    return Err(e.take().unwrap_or_else(|| std::io::Error::other("someone else already took the error :/")));
                },
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
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

        let mut ringbuf = RingBuf::new(5);

        let mut reader = Reader {
            file: FakeFile {position:0, eof: false},
            shared: Arc::new(Mutex::new(Shared {state: State::Downloading, total_length: None})),
            ringbuf: ringbuf.new_reader(), 
            condvar: Arc::new(Condvar::new()),
            position:0,
        };
        {
            ringbuf.write((0,[0,1,2,3].as_slice()));
            ringbuf.write((4,[4,5,6,7].as_slice()));
            ringbuf.write((8,[8,9,10,11,12,13,14,15,16].as_slice()));
            ringbuf.write((17,[17,18].as_slice()));
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
            ringbuf.write((27,[1,2,3,4,5].as_slice()));
            shared.state=State::Success;
        }
        assert_eq!(reader.read(&mut buf).unwrap(), 5);
        assert_eq!(buf[..5], [1,2,3,4,5]);
        assert_eq!(reader.read(&mut buf).unwrap(), 8);
        assert_eq!(buf, 32u64.to_ne_bytes());
        reader.file.eof=true;
        {
            ringbuf.write((0,[0,0,0,0,0,0,0].as_slice()));
            ringbuf.write((40,[1,2,3,4,5].as_slice()));
            ringbuf.write((80,[99,99,99,99,99,99,99,99].as_slice()));
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

        let backing_reader = ReadWrapper {
            data: backing_store.clone(),
            position: 0,
            read_call_count: 0,
            eof: Arc::new(AtomicBool::new(false)),
        };

        let backing_writer = WriteWrapper(backing_store);

        let mut writer = Writer::new(5, backing_writer, 0);
        let mut reader = writer.create_reader(backing_reader);

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
            writer.notify_eof();
        });

        assert!(reader.seek(std::io::SeekFrom::End(-10)).is_ok());

        assert_eq!(reader.read(&mut buf).unwrap(), 10);
        //assert_eq!(reader.file.read_call_count, 2);
        assert_eq!(buf[..10], [26,27,28,29,30,31,32,33,34,35]);
    }
}
