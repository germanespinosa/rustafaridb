use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use stringstream::StringStream;
use std::str::from_utf8;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;
use std::fs::OpenOptions;
use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver,Sender};


pub struct PersistenceManager
{
	writer_Sender:Sender<(String,String,Vec<u8>,Sender<Result<(u8,u64),String>>)>,
}

impl PersistenceManager
{	

//as Receiver<Result<StringStream,()>>
	pub fn start (parallelism:u8) -> Sender<(String,String,Vec<u8>,Sender<Result<(u8,u64),String>>)>
	{
		let (tx, rx) = channel::<(String,String,Vec<u8>,Sender<Result<(u8,u64),String>>)>();
		let mb = Arc::new(Mutex::new(rx));
		for i in 0..parallelism //limit the number of threads 
		{
			let mb=mb.clone();
            let processor = i as u8;
			thread::spawn(move|| 
			{
				loop  
				{
					let writereq=mb.lock();
					let writereq=writereq.unwrap();
					let (col_name, key, value, call_back)=writereq.recv().unwrap();
					println!("--------------PersistenceManager {} to the rescue",processor);
					let file = format!("{}_{}",col_name,processor);
					let file_path = format!("./{}.dat",file);
                    match PersistenceManager::write_data(&file_path,&key.into_bytes()) 
                    {
                        Ok (wk) => 
                        {
                            call_back.send(match PersistenceManager::write_data(&file_path,&value) 
                            {
                                Ok(w)=> Ok((processor,(wk))),
                                Err(e)=> Err(e),  
                            });
                        }
                        Err(e) => 
                        {
                            call_back.send(Err(e));
                        }
                    }
				}
			});		
		}
		tx

	}
	pub fn new(writer_Sender:Sender<(String,String, Vec<u8>,Sender<Result<(u8,u64),String>>)>)-> Self
	{
		PersistenceManager
		{
			writer_Sender:writer_Sender,
		}
	} 
	pub fn write (&mut self, col_name:&String, key:&String, value: &[u8])->Result<(u8,u64),String> //File, offset
	{ 
		let (tx, rx) = channel();
		self.writer_Sender.send((col_name.clone(),key.clone(),value.to_owned(),tx));
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
    pub fn read (&self, col_name:String, file_number:u8, offset:u64, size:u32 ) -> Result<StringStream,()>
    {
		let file = format!("{}_{}",col_name,file_number);
    	let file_path = format!("./{}.dat",file);
        let mut buf=[0;8192];
        match PersistenceManager::read_data(&file_path,offset,size,&mut buf)
        {
            Ok(red)=>
            {
                Ok(StringStream::new(&buf[0..(size as usize)]))
            }
            Err(e)=>
            {
                Err(e)
            }
        }
	} 
	pub fn read_data(file:&String, offset:u64, size:u32, mut buf: &mut [u8]) -> Result<usize,()>
	{
		if let Ok(mut f) = File::open(&file)
		{
			//
			// move the cursor 42 bytes from the start of the file
			if let Ok(offset) = f.seek(SeekFrom::Start(offset))
			{
				match f.read(&mut buf) //move the request to a byte_array
				{
					Ok(i)=>	
					{ 
						Ok(i)
					}
					Err(_)=>
					{
						Err(())
					}
				}
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

/*
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
		let v = pm.read("collection1".to_owned(),0,o,b"testing".len());
		//let r = from_utf8(&v[..]).unwrap();
		//assert_eq!("testing".to_owned(),r);
	}
}
*/