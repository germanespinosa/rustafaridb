use rustc_serialize::json;
use std::collections::HashMap;
use std::io::BufReader;
use std::io::prelude::*;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::Mutex;

#[derive(RustcDecodable, RustcEncodable)]
pub struct IndexEntry
{
	pub key:String,
	pub file:String,
	pub off_set:u64,
	pub size:usize,
}

pub struct IndexEntryContainer
{
	pub entry:IndexEntry,
	pub lock:Mutex<usize>,
}

pub struct Index
{
	index:HashMap<String,IndexEntryContainer>,
	lock:Mutex<usize>,
}

impl IndexEntryContainer
{
	pub fn new(entry:IndexEntry)->Self
	{
		IndexEntryContainer 
		{
			entry : entry,
			lock : Mutex::new(0)
		}
	}
}

impl Index
{
	pub fn new<R:Read>(file : R)->Self
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
			let ic = IndexEntryContainer::new (ie);
			im.fill_entry (ic);
		}
		im
	}
    //nonblocking search
	pub fn find_entry(&self, key:&String)->Option<&IndexEntry>
	{
		match self.index.get(key)
		{
			Some(ec)=>Some(&ec.entry),
			None=>None
		}
	}
	//nonblocking not thread safe, only used to populate the index 
	//before the indexmanager is copied
	fn fill_entry(&mut self, index_entry:IndexEntryContainer) 
	{
		let key=index_entry.entry.key.clone();
		self.index.insert(key,index_entry);
	}
	pub fn insert_entry(&mut self, index_entry:IndexEntry) -> Result<(),usize>
	{
		let mut ec = IndexEntryContainer::new(index_entry);
		let key= ec.entry.key.clone();
		let guard = self.lock.lock();
		match self.index.entry(key) {
			Vacant(entry) => 
			{ 
				entry.insert(ec);
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
			Occupied(mut container) => 
			{ 
				let mut ec = container.get_mut();
				let guard = ec.lock.lock();
				ec.entry.file = index_entry.file;
				ec.entry.off_set = index_entry.off_set;
				ec.entry.size = index_entry.size;
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

	#[test]
	fn test_get_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let im = Index::new(ss);
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
		let mut im = Index::new(ss);
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
		let mut im = Index::new(ss);
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
		let mut im = Index::new(ss);
		assert_eq!(im.insert_entry(IndexEntry{key:"key3".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Ok(()));
	}
	#[test]
	fn test_insert_entry_conflict()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::new(ss);
		assert_eq!(im.insert_entry(IndexEntry{key:"key2".to_owned(),file:"file3".to_owned(),off_set:1000,size:10}),Err(409));
	}
	#[test]
	fn test_remove_entry()
	{
		let ss = StringStream::new_reader("{\"key\":\"key1\",\"file\":\"file1\",\"off_set\":100,\"size\":100}\n{\"key\":\"key2\",\"file\":\"file2\",\"off_set\":120,\"size\":200}");
		let mut im = Index::new(ss);
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

