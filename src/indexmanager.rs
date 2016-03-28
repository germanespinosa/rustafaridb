use rustc_serialize::json;
use std::collections::HashMap;
use std::io::BufReader;
use std::io::prelude::*;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::mpsc;
use std::mem;
use std::hash::{Hash, SipHasher, Hasher};
/*

key : 64
file_name : 32 chars
off_set : 8
size : 8
*/


pub struct IndexPersistance
{
    pub sender:Sender<(IndexEntry, String)>,
}

impl IndexPersistance
{
    fn start( path:&String, buffersize: usize)->Sender<(IndexEntry, String)>
    {
        let (tx, rx): (Sender<(IndexEntry,String)>, Receiver<(IndexEntry,String)>) = mpsc::channel();
        let limit = buffersize;
        let path = path.clone();

        thread::spawn(move|| 
        {
            let mut buffers:HashMap<String,Vec<u8>> = HashMap::new();

            loop
            {
                // many assumptions here... 
                let (ie,col)=rx.recv().unwrap();
                if let Occupied(mut b) = buffers.entry(col.clone())
				{
					let buffer = b.get_mut();
                    let bytes = IndexPersistance::get_bytes(ie);
                }
            }
        });
        tx
    }
    fn get_bytes(ie:IndexEntry)->[u8;48]
    {
        let i:[u8;48]=[0;48];
        
        i
    }
    fn stop ()
    {
    
    }
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct IndexEntry
{
	pub key:String,
	pub file:String,
	pub off_set:u64,
	pub size:usize,
}

pub struct Index
{
	index:HashMap<String,IndexEntry>,
	lock:Mutex<usize>,
}

impl Index
{
	pub fn from_file<R:Read>(file : R)->Self
	{
		let mut lines = BufReader::new(file).lines();
		let mut im = Index 
		{
			index:HashMap::new(),
			lock:Mutex::new(0),
		};
		while let Some(Ok(line)) = lines.next()
		{
			let ie: IndexEntry = json::decode(&line).unwrap();
			im.fill_entry (ie);
		}
		im
	}
	pub fn new()->Self
	{
		Index 
		{
			index:HashMap::new(),
			lock:Mutex::new(0),
		}
	}
    //nonblocking search
	pub fn find_entry(&self, key:&String)->Option<&IndexEntry>
	{
		match self.index.get(key)
		{
			Some(ec)=>Some(&ec),
			None=>None
		}
	}
	//nonblocking not thread safe, only used to populate the index 
	//before the indexmanager is copied
	fn fill_entry(&mut self, index_entry:IndexEntry) 
	{
		let key=index_entry.key.clone();
		self.index.insert(key,index_entry);
	}
	pub fn insert_entry(&mut self, index_entry:IndexEntry) -> Result<(),usize>
	{
		let key= index_entry.key.clone();
		let guard = self.lock.lock();
		match self.index.entry(key) {
			Vacant(entry) => 
			{ 
				entry.insert(index_entry);
				Ok(())
			},
			Occupied(_) => 
			{ 
				Err(409)
			},
		}
	}
	pub fn update_entry(&mut self, index_entry:IndexEntry) -> Result<(),usize>
	{
		let key=index_entry.key.clone();
		match self.index.entry(key) {
			Vacant(_) => 
			{ 
				Err(404)
			},
			Occupied(mut e) => 
			{ 
				let mut ec = e.get_mut();
				ec.file = index_entry.file;
				ec.off_set = index_entry.off_set;
				ec.size = index_entry.size;
				Ok(())
			},
		}
	}
	pub fn remove_entry(&mut self, key:String) -> Result<(),usize>
	{
		self.lock.lock();
		if let Some(_) = self.index.remove(&key)
		{
			Ok(())
		}
		else
		{
			Err(404)
		}
	}
}

#[cfg(test)]
mod test_index_manager
{
	use super::{IndexEntry, Index};
	use stringstream::StringStream;
	use rustc_serialize::json;
    use std::mem;
    use std::hash::{Hash, SipHasher, Hasher};
    
    fn my_hash<T>(obj: T) -> u64
    where T: Hash
    {
        let mut hasher = SipHasher::new();
        obj.hash(&mut hasher);
        hasher.finish()
    }
    
    #[test]
    fn test_hash()
    {
        let a:u64=16167057210370256274;
        assert_eq!(my_hash("string".to_owned()),16167057210370256274);
    }
    
    #[test]
    fn test_string_to_bytes()
    {
        let s = String::from("hello");
        let bytes = s.into_bytes();
        
        assert_eq!(&[104, 101, 108, 108, 111][..], &bytes[..]);
    }
    
    #[test]
    fn test_transmute()
    {
        assert_eq!(mem::size_of::<IndexEntry>(),64);        
        let b = unsafe 
        {
            let a = IndexEntry
            {
                key:"test".to_owned(),
                file:"file".to_owned(),
                off_set:100,
                size:200,
            } ;
            
            mem::transmute::<IndexEntry, [u8;64]>(a)
        };
        let a = unsafe 
        {
            mem::transmute::<[u8;64],IndexEntry>(b)
        };
    }

	#[test]
	fn test_get_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let im = Index::from_file(ss);
		let e = im.find_entry(&"key1".to_owned()).unwrap();
		assert_eq!(e.key,"key1".to_owned());
		assert_eq!(e.file,"file1".to_owned());
		assert_eq!(e.off_set,100);
		assert_eq!(e.size,100);
		let e = im.find_entry(&"key2".to_owned()).unwrap();
		assert_eq!(e.key,"key2".to_owned());
		assert_eq!(e.file,"file2".to_owned());
		assert_eq!(e.off_set,120);
		assert_eq!(e.size,200);
	}
	#[test]
	fn test_update_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::from_file(ss);
		assert_eq!(im.update_entry(IndexEntry{key:"key2".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Ok(()));
		let e = im.find_entry(&"key1".to_owned()).unwrap();
		assert_eq!(e.key,"key1".to_owned());
		assert_eq!(e.file,"file1".to_owned());
		assert_eq!(e.off_set,100);
		assert_eq!(e.size,100);
		let e = im.find_entry(&"key2".to_owned()).unwrap();
		assert_eq!(e.key,"key2".to_owned());
		assert_eq!(e.file,"file3".to_owned());
		assert_eq!(e.off_set,1000);
		assert_eq!(e.size,10);
	}
	#[test]
	fn test_update_entry_not_found()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::from_file(ss);
		assert_eq!(im.update_entry(IndexEntry{key:"key3".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Err(404));
		let e = im.find_entry(&"key1".to_owned()).unwrap();
		assert_eq!(e.key,"key1".to_owned());
		assert_eq!(e.file,"file1".to_owned());
		assert_eq!(e.off_set,100);
		assert_eq!(e.size,100);
		let e = im.find_entry(&"key2".to_owned()).unwrap();
		assert_eq!(e.key,"key2".to_owned());
		assert_eq!(e.file,"file2".to_owned());
		assert_eq!(e.off_set,120);
		assert_eq!(e.size,200);
	}
	#[test]
	fn test_insert_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::from_file(ss);
		assert_eq!(im.insert_entry(IndexEntry{key:"key3".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Ok(()));
	}
	#[test]
	fn test_insert_entry_conflict()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::from_file(ss);
		assert_eq!(im.insert_entry(IndexEntry{key:"key2".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Err(409));
	}
	#[test]
	fn test_remove_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::from_file(ss);
		im.remove_entry("key2".to_owned());
		let e = im.find_entry(&"key1".to_owned()).unwrap();
		assert_eq!(e.key,"key1".to_owned());
		assert_eq!(e.file,"file1".to_owned());
		assert_eq!(e.off_set,100);
		assert_eq!(e.size,100);
		let e = im.find_entry(&"key2".to_owned());
		assert_eq!(e.is_some(),false);
	}
	#[test]
	fn test_json_encode()
	{
		let ie = IndexEntry{key:"key".to_owned(),file:"file".to_owned(),off_set:100,size:100};
		let encoded = json::encode(&ie).unwrap();
		assert_eq!(encoded,"{\"key\":\"key\",\"file\":\"file\",\"off_set\":100,\"size\":100}")
	}
	#[test]
	fn test_json_decode()
	{
		let encoded = "{\"key\":\"key\",\"file\":\"file\",\"off_set\":100,\"size\":100}";
		let ie: IndexEntry = json::decode(&encoded).unwrap();
		assert_eq!("key".to_owned(),ie.key);
		assert_eq!("file".to_owned(),ie.file);
		assert_eq!(100,ie.off_set);
		assert_eq!(100,ie.size);
	}
}

