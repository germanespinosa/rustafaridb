use std::sync::{Mutex};
use std::collections::HashMap;
use stringstream::StringStream;
use std::str::from_utf8;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;
use std::fs::OpenOptions;
 
 
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
	pub fn write (&mut self, col_name:&String, value: &[u8])->Result<(String,u64),String> //File, offset
	{
		let file = format!("./{}.dat",col_name);
		println!("file name:{}",file);
		if let Ok(mut f) = OpenOptions::new().create(true).write(true).append(true).open(&file)
		{
			println!("file opened");
			if let Ok(offset) = f.seek(SeekFrom::End(0))
			{
				println!("offset:{}",offset);
				f.write(&value); //move the request to a byte_array
				Ok((file,offset))
			}
			else
			{
				Err("Something went wrong!".to_owned())
			}
		}
		else
		{
			Err("Something went wrong!".to_owned())
		}
	}
	pub fn read(&mut self,file:&String, offset:u64, size:usize) -> Option<StringStream>
	{
		if let Ok(mut f) = File::open(&file)
		{
			let mut buf=[0;8192];
			// move the cursor 42 bytes from the start of the file
			if let Ok(offset) = f.seek(SeekFrom::Start(offset))
			{
				let mut red = match f.read(&mut buf) //move the request to a byte_array
				{
					Ok(i)=>	
					{ 
						&buf[0..i]
					}
					Err(_)=>
					{
						panic!("boom!");
					}
				};
				Some(StringStream::new(&red[0..size]))
			}
			else
			{
				None
			}
		}
		else
		{
			None
		}
	}

}


#[cfg(test)]
mod test_persistence_manager
{
	use super::{PersistenceManager};

	#[test]
	fn test_write_read()
	{
		let mut pm = PersistenceManager::new(); 
		let (f,o) = pm.write(&"collection1".to_owned(),b"testing").unwrap();
		assert_eq!(f,"./collection1.dat".to_owned());
		assert_eq!(o,0);
		let v = pm.read(&f,o,b"testing".len());
		//let r = from_utf8(&v[..]).unwrap();
		//assert_eq!("testing".to_owned(),r);
	}
}