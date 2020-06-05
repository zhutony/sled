//! experimental next-gen sled storage engine
//!
//! * shared-nothing
//! * no memory barriers
//! * no async, no threads
//! * no allocations, fixed mmaps
//! * io_uring
//! * hierarchical page cache
//! * sled-style variable page sizes
//! * swizzled pointers
//! * ARIES logging
//!
//! # Motivation
//!
//! sled's architecture is extremely fast for tiny data sets.
//! but it faces challenges with:
//!
//! * transactions are limited
//! * race conditions slow down dev
//! * memory usage gets out of control
//! * storage usage gets out of control
//!
//! # Observations
//!
//! # Algorithms and Data Structures
//!
//! * Atomic Commit - 2PC + ARIES
//! * Indexing - B+ Tree w/ prefix encoding & suffix truncation
//! * Cache Management - hierarchical + w-TinyLFU

#![feature(const_fn)]
#![feature(const_transmute)]
#![feature(const_ptr_offset)]
#![allow(unused)]

// pages are variable size
const SIZE_CLASSES: usize = 6;

type PageId = u64;
type TxId = u64;
type Lsn = u64;

use {
    crc32fast::Hasher,
    libc::{mmap, munmap},
    std::{
        convert::TryFrom,
        fs::{File, OpenOptions},
        io::Write,
        path::Path,
    },
};

struct Pointer([u8; 8]);

struct Leaf<'a> {
    keys: &'a [&'a [u8]],
    values: &'a [&'a [u8]],
}

struct Index<'a> {
    keys: &'a [&'a [u8]],
    children: &'a [&'a Pointer],
}

#[derive(Debug)]
struct Page {
    // [header | key lengths | value lengths | keys | values]
    //
    // header: {
    //  is leaf: 1 bit,
    //  number of children: 15 bits,
    data: [u8],
}

#[derive(Debug)]
struct PageView<'a> {
    is_leaf: bool,
    child_count: usize,
    lo: &'a [u8],
    hi: &'a [u8],
    keys: &'a [&'a [u8]],
    values: &'a [&'a [u8]],
}

const LEAF_MASK: u8 = 0b1111_1110;

impl Page {
    const fn view(&self) -> PageView<'_> {
        let is_leaf = self.data[0] ^ LEAF_MASK != 0;

        // does not account for lo and hi keys
        let child_count =
            u16::from_le_bytes([self.data[0] & LEAF_MASK, self.data[1]])
                as usize;

        let key_length_base = self.data.as_ptr().add(2);
        let val_length_base = key_length_base.add((2 * 2) + (child_count * 2));
        let keys_base = val_length_base.add(child_count * 4);

        let key_lengths: &[u16] = unsafe {
            std::mem::transmute((key_length_base as *mut u16, child_count + 2))
        };

        let val_lengths: &[u32] = unsafe {
            std::mem::transmute((val_length_base as *mut u32, child_count))
        };

        let val_base = keys_base.add(key_lengths.iter().sum::<u16>() as usize);

        let lo_len = key_lengths[0] as usize;
        let hi_len = key_lengths[1] as usize;

        let lo = &[];
        let hi = &[];
        let keys = &[];
        let values = &[];

        PageView {
            is_leaf,
            child_count,
            hi,
            lo,
            keys,
            values,
        }
    }

    /*
    const fn key_lengths(&self) -> &[u16] {
        let child_count = self.child_count();
        let base = self.data.as_ptr().add(2);

        &[]
    }

    const fn keys(&self) -> &[&[u8]] {
        todo!()
    }

    // this is the first key in the keys array
    const fn lo(&self) -> &[u8] {
        let child_count = self.child_count();
        let ptr = self.data.as_ptr() as usize;
        let len = 5;
        todo!()
    }

    // this is the second key in the keys array
    const fn hi(&self) -> &[u8] {
        todo!()
    }
    */

    fn values(&self) -> &[&[u8]] {
        todo!()
    }
    fn next(&self) -> PageId {
        todo!()
    }
    fn traverse(&self) -> Result<&[u8], PageId> {
        todo!()
    }
    fn insert(&self) -> &[u8] {
        todo!()
    }
    fn remove(&self) -> &[u8] {
        todo!()
    }
}

enum PageUpdate<'a> {
    Set { key: &'a [u8], value: &'a [u8] },
    Del { key: &'a [u8] },
}

enum LogRecord<'a> {
    Update {
        lsn: Lsn,
        tx: TxId,
        pid: PageId,
        redo: PageUpdate<'a>,
        undo: PageUpdate<'a>,
        previous_lsn: Lsn,
    },
    Commit {
        tx: TxId,
        last_lsn: Lsn,
    },
}

#[derive(Debug)]
struct BufferPool {
    next_tx: TxId,
    next_page: PageId,
    free_pages: Vec<PageId>,
    log: File,
    heap: File,
    page_pointers: Vec<usize>,
    buffer_pool_size: usize,
    buffer_pool_pointers: [*mut libc::c_void; SIZE_CLASSES],
}

#[derive(Debug)]
struct Db {
    buffer_pool: BufferPool,
}

impl Db {
    fn set(&mut self, key: &[u8], value: &[u8]) {
        todo!()
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        todo!()
    }

    fn traverse(&self, key: &[u8]) -> &'static Page {
        todo!()
    }
}

impl BufferPool {
    fn open(cache_size_in_bytes: usize) -> BufferPool {
        let buffer_pool_size =
            std::cmp::max(64 * 1024, cache_size_in_bytes.next_power_of_two());

        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("log")
            .unwrap();

        // TODO todo!("find the lowest stable point in the log");

        let heap = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("heap")
            .unwrap();

        // TODO todo!("replay the log into the heap");

        let mut buffer_pool_pointers = [std::ptr::null_mut(); SIZE_CLASSES];

        for idx in 0..buffer_pool_pointers.len() {
            let ptr = unsafe {
                mmap(
                    std::ptr::null_mut(),
                    buffer_pool_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if ptr.is_null() || ptr == libc::MAP_FAILED {
                let err = std::io::Error::last_os_error();
                panic!("failed to unmap memory: {:?}", err);
            }
            buffer_pool_pointers[idx] = ptr;
        }

        dbg!(BufferPool {
            next_tx: 0,
            next_page: 0,
            page_pointers: vec![],
            free_pages: vec![],
            log,
            heap,
            buffer_pool_size,
            buffer_pool_pointers,
        })
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        self.log.sync_all();
        self.heap.sync_all();
        for ptr in &self.buffer_pool_pointers {
            let ret = unsafe { munmap(*ptr, self.buffer_pool_size) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("failed to unmap memory: {:?}", err);
            }
        }
    }
}

fn open(cache_size_in_bytes: usize) -> Db {
    let buffer_pool = BufferPool::open(cache_size_in_bytes);
    Db { buffer_pool }
}

fn main() {
    let mut db = open(1024 * 1024);

    db.set(b"a", b"a");
    assert_eq!(db.get(b"a").unwrap(), vec![b'a']);
}
