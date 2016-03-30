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
use std::vec::Vec;
use persistencemanager::PersistenceManager;
use std::fs;
use std::path::Path;
use std::fs::File;

pub struct IndexPersistance
{
    pub sender:Sender<Option<(IndexEntry, String)>>,
}

impl IndexPersistance
{   
    fn load_indexes(path : &Path) -> HashMap<String,Index>
    {
        let mut col_indexes : HashMap<String,Index>=HashMap::new();

        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap().path();
            
            let extension = entry.extension().unwrap().to_str().unwrap();
            
            if extension == "ix"
            {	
                let file_name = entry.file_stem().unwrap().to_str().unwrap();
                /*
                let file = match File::open(&entry) {
                    Err(why) => panic!("couldn't open {}: {}", entry.to_str().unwrap(),why),
                    Ok(file) => file,
                };
                */
                let mut im = Index::from_file(&entry.to_str().unwrap().to_owned());
                println!("loading collection {}",&file_name);
                col_indexes.insert(file_name.to_owned(),im);
            }
        }
        col_indexes
    }


    fn start( path:&String, buffersize: usize)->(Sender<Option<(IndexEntry, String)>>, HashMap<String,Index>)
    {
        let (tx, rx): (Sender<Option<(IndexEntry,String)>>, Receiver<Option<(IndexEntry,String)>>) = mpsc::channel();
        let limit = buffersize;
        let path = path.clone();

        thread::spawn(move|| 
        {
            let mut buffers:HashMap<String,Vec<u8>> = HashMap::new();

            loop
            {
                // many assumptions here... 
                if let Some((ie,col))=rx.recv().unwrap()
                {
                    let file_path = format!("{}/{}.ix",path,col);
                    match buffers.entry(col.clone())
                    {
                        Occupied(mut b)=>
                        {
                            let buffer = b.get_mut();
                            let bytes = IndexPersistance::get_bytes(ie);
                            buffer.extend_from_slice(&bytes);
                            if bytes.len()>=limit
                            {
                                if let Ok(o)=PersistenceManager::write_data(&file_path, &buffer)
                                {
                                    buffer.truncate(0);
                                }
                            }
                        }
                        Vacant(buffer_entry)=>
                        {
                            let buffer:Vec<u8>=Vec::with_capacity(limit+24);
                            buffer_entry.insert(buffer);
                        }
                    }
                }
                else // we need to write down all the buffers
                {
                    for (col, buffer) in buffers.iter_mut() 
                    {
                        let file_path = format!("{}/{}.ix",path,col);
                        if let Ok(o)=PersistenceManager::write_data(&file_path, &buffer)
                        {
                            buffer.truncate(0);
                        }
                    }
                }
            }
        });
        (tx,HashMap::new())
    }
    fn get_bytes(ie:IndexEntry)->[u8;24]
    {
        unsafe 
        {
            mem::transmute::<IndexEntry,[u8;24]>(ie)
        }  
    }
    fn stop (index_maintenance:Sender<Option<(IndexEntry, String)>>)
    { 
        index_maintenance.send(None);
    }
}

pub struct IndexEntry
{
	pub key_size:u8,
	pub file:u8,
	pub off_set:u64,
	pub size:u32,
}

pub struct Index
{
	index:HashMap<u64,IndexEntry>,
}

impl Index
{
    fn get_hash<T>(obj: T) -> u64
    where T: Hash
    {
        let mut hasher = SipHasher::new();
        obj.hash(&mut hasher);
        hasher.finish()
    }
    pub fn from_file(file_path:&String)->Self
    {
        let mut ix = Index::new();
        let mut more = true;
        while more
        {
        }
        ix
    }
	pub fn new()->Self
	{
		Index 
		{
			index:HashMap::new(),
		}
	}
    //nonblocking search
	pub fn find_entry(&self, key:&String)->Option<&IndexEntry>
	{
        let h=Index::get_hash(key.clone());
		self.index.get(&h)
	}
	//nonblocking not thread safe, only used to populate the index 
	//before the indexmanager is copied
	fn fill_entry(&mut self, index_entry:IndexEntry, key:u64) 
	{
        let h=Index::get_hash(key.clone());
		self.index.insert(h,index_entry);
	}
	pub fn insert_entry(&mut self, index_entry:IndexEntry, key:String) -> Result<(),usize>
	{
        let h=Index::get_hash(key);
		match self.index.entry(h) {
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
	pub fn update_entry(&mut self, index_entry:IndexEntry, key:String) -> Result<(),usize>
	{
        let key_size=key.len();
        let h=Index::get_hash(key);
		match self.index.entry(h) {
			Vacant(_) => 
			{ 
				Err(404)
			},
			Occupied(mut e) => 
			{ 
				let mut ec = e.get_mut();
                ec.key_size = key_size as u8;
				ec.file = index_entry.file;
				ec.off_set = index_entry.off_set;
				ec.size = index_entry.size;
				Ok(())
			},
		}
	}
	pub fn remove_entry(&mut self, key:String) -> Result<(),usize>
	{
        let h=Index::get_hash(key.clone());
		if let Some(_) = self.index.remove(&h)
		{
			Ok(())
		}
		else
		{
			Err(404)
		}
	}
}
/*
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

*/