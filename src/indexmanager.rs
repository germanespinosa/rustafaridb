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

const INDEX_ENTRY_SIZE: usize = 32;

pub struct IndexPersistence
{
    pub sender:Sender<Option<(IndexEntry, String)>>,
}

impl IndexPersistence
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
                
                let mut im = Index::from_file(&entry.to_str().unwrap().to_owned(), file_name.to_owned());
                println!("loading collection {}",&file_name);
                col_indexes.insert(file_name.to_owned(),im);
            }
        }
        col_indexes 
    }


    pub fn start( path:String, buffer_size: usize)->(Sender<Option<(IndexEntry, String)>>, HashMap<String,Index>)
    {
        let (tx, rx): (Sender<Option<(IndexEntry,String)>>, Receiver<Option<(IndexEntry,String)>>) = mpsc::channel();
        let limit = buffer_size;
        let path = path.clone();
        
        let ix_col = IndexPersistence::load_indexes(Path::new(&path));

        thread::spawn(move|| 
        {
            let mut buffers:HashMap<String,Vec<u8>> = HashMap::new();

            loop
            {
                // many assumptions here... 
                if let Some((ie,col))=rx.recv().unwrap()
                {
                    let bytes = IndexPersistence::get_bytes(ie);

                    match buffers.entry(col.clone())
                    {
                        Occupied(mut b)=>
                        {
                            let buffer = b.get_mut();
                            println!("bytes in {} before!",buffer.len());
                            
                            buffer.extend_from_slice(&bytes);
                            println!("{} bytes in {} limit {}!",buffer.len(),&col,limit);
                               
                            if buffer.len()>=limit-INDEX_ENTRY_SIZE
                            {
                                let file_path = format!("{}/{}.ix",path,col);
                                println!("saving index entry to {}!",file_path);

                                if let Ok(o)=PersistenceManager::write_data(&file_path, &buffer)
                                {
                                    buffer.truncate(0);
                                }
                            }
                        }
                        Vacant(buffer_entry)=>
                        {
                            let mut buffer:Vec<u8>=Vec::with_capacity(limit);
                            buffer.extend_from_slice(&bytes);
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
        (tx,ix_col)
    }
    fn get_bytes(ie:IndexEntry)->[u8;INDEX_ENTRY_SIZE]
    {
        unsafe 
        {
            mem::transmute::<IndexEntry,[u8;INDEX_ENTRY_SIZE]>(ie)
        }  
    }
    fn stop (index_maintenance:Sender<Option<(IndexEntry, String)>>)
    { 
        index_maintenance.send(None);
    }
}

pub struct IndexEntry // 24 bytes
{
	pub key_size:u8, // limit 255 bytes
	pub file:u8, // limit 255 files per collection
	pub off_set:u64, // huge, I mean huge data files 
	pub size:u32, // 4GB per record limit
    pub key_hash:u64, 
}

pub struct Index
{
	index:HashMap<u64,IndexEntry>,
    col_name:String,
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
    pub fn from_file(file_path:&String, col_name:String)->Self
    {
        let mut ix = Index::new(col_name.clone());
        let mut more = true;
        let mut off_set=0;
        let size = (INDEX_ENTRY_SIZE * 1000) as u32;
        let mut buf=[0;8192];
        while more
        {
            match PersistenceManager::read_data(&file_path,off_set,size,&mut buf)
            {
                Ok(red)=>
                {
                    let records_red = red / INDEX_ENTRY_SIZE;
                    for i in 0..records_red
                    {
                        let mut r=[0;INDEX_ENTRY_SIZE];
                        for n in 0..INDEX_ENTRY_SIZE
                        {
                            r[n] = buf[(i*INDEX_ENTRY_SIZE)+n];
                        }
                        let ie= unsafe
                        {
                            mem::transmute::<[u8;INDEX_ENTRY_SIZE],IndexEntry>(r)
                        };
                        ix.index.insert(ie.key_hash,ie);
                    }
                    if records_red == 0
                    {
                        more=false;
                    }
                    else
                    {
                        off_set+=red as u64;
                    }
                }
                Err(e)=>
                {
                    panic!("Something went wrong");
                }
            }            
        }
        ix
    }
	pub fn new(col_name:String)->Self
	{
		Index 
		{
			index:HashMap::new(),
            col_name:col_name,
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
    fn copy_index_entry(ie:&IndexEntry)->IndexEntry
    {
        IndexEntry{
            key_size:ie.key_size, // limit 255 bytes
            file:ie.file, // limit 255 files per collection
            off_set:ie.off_set, // huge, I mean huge data files 
            size:ie.size, // 4GB per record limit 
            key_hash:ie.key_hash,
        }
    }
	pub fn insert_entry(&mut self, mut index_entry:IndexEntry, key:String, sender: &Sender<Option<(IndexEntry, String)>>) -> Result<(),usize>
	{
        let h=Index::get_hash(key.clone());
        index_entry.key_hash = h;
		match self.index.entry(h) 
        {
			Vacant(entry) => 
			{ 
				entry.insert(Index::copy_index_entry(&index_entry));
                sender.send (Some((index_entry, self.col_name.clone())));
				Ok(())
			},
			Occupied(_) => 
			{ 
				Err(409)
			},
		}
	}
	pub fn update_entry(&mut self, index_entry:IndexEntry, key:String, sender: &Sender<Option<(IndexEntry, String)>>) -> Result<(),usize>
	{
        let key_size=key.len();
        let h=Index::get_hash(key.clone());
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
                sender.send (Some((index_entry, self.col_name.clone())));
				Ok(())
			},
		}
	}
	pub fn remove_entry(&mut self, key:String, sender: &Sender<Option<(IndexEntry, String)>>) -> Result<(),usize>
	{
        let h=Index::get_hash(key.clone());
		if let Some(mut e) = self.index.remove(&h)
		{
            e.size = 0;
            sender.send (Some((e, self.col_name.clone())));
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