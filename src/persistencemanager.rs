use indexmanager;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use stringstream::StringStream;
use std::io::prelude::*;
use std::str::from_utf8;

pub struct PersistenceManager
{
	pub files:Mutex<HashMap<String,Vec<String>>>
}

impl PersistenceManager
{
	pub fn new()-> Self
	{
		PersistenceManager
		{
			files:Mutex::new(HashMap::new())
		}
	}
	pub fn write (&mut self, col_name:String, value: &[u8])->(String,usize) //File, offset
	{
		let mut f = self.files.lock().unwrap();
		let r = from_utf8(&value[..]).unwrap();
		let mut col=f.entry(col_name.clone()).or_insert(vec![]);
		col.push(r.to_owned());
		(col_name,col.len()-1)
	}
	pub fn read(&mut self,file:String, offset:usize, size:usize) -> StringStream
	{
		let mut f = self.files.lock().unwrap();
		let mut col=f.entry(file.clone()).or_insert(vec![]);
		StringStream::new_reader(&col[offset][0..size])
	}
}


#[cfg(test)]
mod test_persistence_manager
{
	use super::{PersistenceManager};
	use std::str::from_utf8;

	#[test]
	fn test_write()
	{
		let mut pm = PersistenceManager::new();
		let (f,o) = pm.write("collection1".to_owned(),b"testing");
		assert_eq!(pm.files.lock().unwrap().len(),1);
		assert_eq!(f,"collection1".to_owned());
		assert_eq!(o,0);
	}
	#[test]
	fn test_read()
	{
		let mut pm = PersistenceManager::new();
		let (f,o) = pm.write("collection1".to_owned(),b"testing");
		assert_eq!(pm.files.lock().unwrap().len(),1);
		assert_eq!(f,"collection1".to_owned());
		assert_eq!(o,0);
		let v = pm.read(f,o,b"testing".len());
		//let r = from_utf8(&v[..]).unwrap();
		//assert_eq!("testing".to_owned(),r);
	}
}