use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use stringstream::StringStream;
use std::str::from_utf8;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;
use std::fs::OpenOptions;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::Sender;
use std::thread;

pub struct PersistenceManager
{
	writer_Sender:SyncSender<(String,Vec<u8>,SyncSender<Result<(String,u64),String>>)>,
}

impl PersistenceManager
{	

//as Receiver<Result<StringStream,()>>
	pub fn start (parallelism:usize) -> SyncSender<(String,Vec<u8>,SyncSender<Result<(String,u64),String>>)>
	{
		let (tx, rx) = sync_channel::<(String,Vec<u8>,SyncSender<Result<(String,u64),String>>)>(0);
		let mb = Arc::new(Mutex::new(rx));
		for i in 0..parallelism //limit the number of threads 
		{
			let mb=mb.clone();
			thread::spawn(move|| 
			{
				loop  
				{
					let writereq=mb.lock();
					let writereq=writereq.unwrap();
					let (col_name, value, call_back)=writereq.recv().unwrap();
					println!("--------------PersistenceManager {} to the rescue",i);
					let file = format!("{}_{}",col_name,i);
					let file_path = format!("./{}.dat",file);
					call_back.send(match PersistenceManager::write_data(&file_path,&value) 
                    {
                        Ok(w)=> Ok((file,w)),
                        Err(e)=> Err(e),  
                    });
				}
			});		
		}
		tx

	}
	pub fn new(writer_Sender:SyncSender<(String,Vec<u8>,SyncSender<Result<(String,u64),String>>)>)-> Self
	{
		PersistenceManager
		{
			writer_Sender:writer_Sender,
		}
	} 
	pub fn write (&mut self, col_name:&String, value: &[u8])->Result<(String,u64),String> //File, offset
	{ 
		let (tx, rx) = sync_channel(0);
		self.writer_Sender.send((col_name.clone(),value.to_owned(),tx));
		rx.recv().unwrap()
	}
	pub fn write_data (file_path:&String, value: &[u8])->Result<u64,String> //File, offset
	{
		println!("file name:{}",file_path);
		if let Ok(mut f) = OpenOptions::new().create(true).write(true).append(true).open(&file_path)
		{
			println!("file opened");
			if let Ok(offset) = f.seek(SeekFrom::End(0))
			{
				println!("offset:{}",offset);
				f.write(&value); //move the request to a byte_array
				Ok(offset)
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
	pub fn read(&mut self,file:&String, offset:u64, size:usize) -> Result<StringStream,()>
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
						return Err(());
					}
				};
				Ok(StringStream::new(&red[0..size]))
			}
			else
			{
				Err(())
			}
		}
		else
		{
			Err(())
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
		let sender = PersistenceManager::start(1);
		let mut pm = PersistenceManager::new(sender); 
		let (f,o) = pm.write(&"collection1".to_owned(),b"testing").unwrap();
		assert_eq!(f,"collection1_0".to_owned());
		let v = pm.read(&f,o,b"testing".len());
		//let r = from_utf8(&v[..]).unwrap();
		//assert_eq!("testing".to_owned(),r);
	}
}