use httpprocessor::HttpProcessor;
use stringstream::StringStream;
use std::io::prelude::*;
use std::collections::HashMap;
use indexmanager::{IndexEntry,Index};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use persistencemanager::PersistenceManager;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::net::{TcpListener,TcpStream};
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver,Sender};

pub struct RestApi
{
	sender:Sender<(String,String,Vec<u8>,Sender<Result<(String,u64),String>>)>,
}

impl RestApi
{

    pub fn start(parallelism:usize,col_indexes:HashMap<String,Index>,ip_Sender: Sender<Option<(IndexEntry, String)>>,persistence_sender:Sender<(String,String,Vec<u8>,Sender<Result<(u8,u64),String>>)>)-> Sender<TcpStream>
	{
		let (tx, rx) = channel();
		let mb = Arc::new(Mutex::new(rx));
		let mut ixc = Arc::new(RwLock::new(col_indexes));
		for i in 0..parallelism //limit the number of threads to 10
		{
			let mb=mb.clone();
			let mut ix_col = ixc.clone(); 
			let persistence_sender = persistence_sender.clone();
            let mut ip = ip_Sender.clone();
			thread::spawn(move|| 
			{
				let mut pm = PersistenceManager::new(persistence_sender);
				loop
				{
					// many assumptions here... 
					let stream=mb.lock().unwrap().recv().unwrap(); 
					RestApi::handle_client(stream, &ix_col, &ip, &mut pm);
				}
			});		
		}
		tx
	}

	fn get_collection_key(url: &String)->(Option<String>,Option<String>)
	{
		let components: Vec<&str> = url.split('/').collect();
		let mut col : Option<String> = None;
		let mut key : Option<String> = None;
		if components.len()>1
		{
			if components[1].len()>0 
			{
				col=Some(components[1].to_owned());
			}
			if components.len()>2
			{
				if components[2].len()>0 
				{
					key = Some(components[2].to_owned());
				}
			}
		}
		(col,key)
	}

	pub fn handle_client<RW:Read + Write>(mut stream : RW, col_indexes:&Arc<RwLock<HashMap<String,Index>>>, ip_Sender: &Sender<Option<(IndexEntry, String)>>, persistence : &mut PersistenceManager)
	{
		let mut p=HttpProcessor::new();
		if let Err(()) = p.process_request(&mut stream)
		{
			return; 
		}
		let (col,key)=RestApi::get_collection_key(&p.url);
		
		if let None = col {
			p.response_code = 503;
			p.send_response(&mut StringStream::new_reader("Internal Server Error"),&mut stream);
			return;
		}

		let col = col.unwrap();

		if let Some(k) = key 
		{
			// let's operate on the item
			p.response_headers.insert("Server".to_owned(),"RustafariDB 1.0".to_owned());
			p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
			
			match &p.verb[..] 
			{
				"GET" => 
				{
					let col_indexes = col_indexes.read().unwrap(); // lock the index collection for reading 
					
					if let Some(ix) = col_indexes.get(&col.clone())
					{
						if let Some (entry) = ix.find_entry(&k)
						{
							p.response_code = 200;
                            println!("{}-{}-{}-{}-{}", &col.clone(),&entry.file,entry.off_set,entry.key_size,entry.size);
							if let Ok(mut response) = persistence.read(col.clone(),entry.file,entry.off_set + ( entry.key_size as u64 ),entry.size)
							{
								p.send_response(&mut response,&mut stream); 
							}
							else
							{
								p.response_code = 503;
								p.send_response(&mut StringStream::new_reader("Internal Server Error"),&mut stream);
							}
						}
						else
						{
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
						}
					}
					else
					{
						p.response_code = 404;
						p.send_response(&mut StringStream::new_reader("Collection not Found"),&mut stream);
					}
				}
				"POST" => 
				{
					{
						//I perform a non blocking search first to discard the obvious conflictive cases
						let col_indexes = col_indexes.read().unwrap(); // lock the index collection for reading 
						
						if let Some(ix) = col_indexes.get(&col.clone())
						{
							let mut found=false; 
							{if let Some (entry) = ix.find_entry(&k) {found=true};}
							if found
							{
								p.response_code = 409;
								p.send_response(&mut StringStream::new_reader("Conflictive key"),&mut stream);
								return;
							}
						}
						else
						{
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Collection not Found"),&mut stream);
							return;
						}
					}
					// If I didn't find the key so I write the record in the file
					// still not blocking so this record might be discarded, but I don't really care
					if let Ok((file,off_set)) = persistence.write(&col,&k,&p.request_body)
					{
						//record saved to file properly, let's create the index entry
						let mut ie = IndexEntry 
						{
							size:p.request_body.len() as u32,
							file:file,
							off_set:off_set,
                            key_size:k.len() as u8,
                            key_hash:0
						};
						//let's try to update the index this is the only blocking operation
						// lock the index collection for writing 
						let mut col_indexes = col_indexes.write().unwrap(); 

						if let Occupied(mut ix) = col_indexes.entry(col.clone())
						{
							let ix = ix.get_mut();
							if let Ok(container) = ix.insert_entry(ie,k,ip_Sender)
							{
								// the was not duplicated and the index was updated correctly
								p.response_code = 200;
								p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
								p.send_response(&mut StringStream::new_reader("Inserted Successfuly"),&mut stream);
							}
							else
							{
								// some other thread updated the index since we checked...
								// we created an orphan record in the file
								// we don't care... the defrag process will eliminate it
								p.response_code = 409;
								p.send_response(&mut StringStream::new_reader("Conflictive key"),&mut stream);
							}
						}
						else 
						{
							// very odd case 
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Collection not Found"),&mut stream);
							return;
						}
					}
					else //I/O Error
					{
							p.response_code = 503;
							p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
					}
				}
				"PUT" => 
				{
					{
						//I perform a non blocking search first to discard the obvious conflictive cases
						let col_indexes = col_indexes.read().unwrap(); // lock the index collection for reading 
						
						if let Some(ix) = col_indexes.get(&col.clone())
						{
							let mut found=false; 
							{if let Some (entry) = ix.find_entry(&k) {found=true};}
							if !found
							{
								p.response_code = 404;
								p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
								return;
							}
						}
						else
						{
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Collection not Found"),&mut stream);
							return;
						}
					}
					// If I didn't find the key so I write the record in the file
					// still not blocking so this record might be discarded, but I don't really care
					if let Ok((file,off_set)) = persistence.write(&col,&k,&p.request_body)
					{
						//record saved to file properly, let's create the index entry
						let mut ie = IndexEntry 
						{
							size:p.request_body.len() as u32,
							file:file,
							off_set:off_set,
                            key_size:k.len() as u8,
                            key_hash:0,
						};
						//let's try to update the index this is the only blocking operation
						// lock the index collection for writing 
						let mut col_indexes = col_indexes.write().unwrap(); 

						if let Occupied(mut ix) = col_indexes.entry(col.clone())
						{
							let ix = ix.get_mut();
							if let Ok(container) = ix.update_entry(ie,k.clone(),ip_Sender)
							{
								// the was not duplicated and the index was updated correctly
								p.response_code = 200;
								p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
								p.send_response(&mut StringStream::new_reader("Updated Successfuly"),&mut stream);
							}
							else
							{
								// some other thread updated the index since we checked...
								// we created an orphan record in the file
								// we don't care... the defrag process will eliminate it
								p.response_code = 409;
								p.send_response(&mut StringStream::new_reader("Conflictive key"),&mut stream);
							}
						}
						else 
						{
							// very odd case 
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Collection not Found"),&mut stream);
							return;
						}
					}
					else //I/O Error
					{
							p.response_code = 503;
							p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
					}
				}
				"DELETE" => 
				{
					//let's try to update the index this is the only blocking operation
						let mut col_indexes = col_indexes.write().unwrap(); 

						if let Occupied(mut ix) = col_indexes.entry(col.clone())
						{
							let ix = ix.get_mut();
							if let Ok(_) = ix.remove_entry(k,ip_Sender)
							{
								// nobody deleted the record while we were writin the file
								p.response_code = 200;
								p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
								p.send_response(&mut StringStream::new_reader("Removed Ok"),&mut stream);
							}
							else
							{
								// some other thread updated the index since we checked...
								// we created an orphan record in the file
								// we don't care... the defrag process will eliminate it
								p.response_code = 404;
								p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
							}
						}
						else
						{
							// some other thread updated the index since we checked...
							// we created an orphan record in the file
							// we don't care... the defrag process will eliminate it
							p.response_code = 404;
							p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
						}
				}
				_ =>
				{
						p.response_code = 503;
						p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
				}
			}
		}
		else //let's operate in the collection
		{ 
			let mut col_indexes = col_indexes.write().unwrap();
			println!("was here!!");
			match &p.verb[..] 
			{
				"POST" => 
				{
					println!("was here!! POST!");
					if let Vacant(ixmanager) = col_indexes.entry(col.clone())
					{
						ixmanager.insert(Index::new(col.clone()));
						p.response_code = 200;
						p.send_response(&mut StringStream::new_reader("Collection created successfully"),&mut stream);
					}
					else
					{
						p.response_code = 409;
						p.send_response(&mut StringStream::new_reader("Conflictive key"),&mut stream);
					}
				}
				"DELETE" => 
				{
					println!("was here!! DELETE!");
					if let Some(_) = col_indexes.remove(&col)
					{
						p.response_code = 200;
						p.send_response(&mut StringStream::new_reader("Collection deleted successfully"),&mut stream);
					}
					else
					{
						p.response_code = 404;
						p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
					}
				}
				_ =>
				{
						p.response_code = 503;
						p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
				}
			}
		}
	}
	}

#[cfg(test)]
mod test_restapi
{
	use super::RestApi;
		#[test]
	fn test_get_collection_key_with_extra()
	{
		let (col, key)=RestApi::get_collection_key(&"/c/k/1234".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,Some("k".to_owned()));
	}
	#[test]
	fn test_get_collection_key()
	{
		let (col, key)=RestApi::get_collection_key(&"/c/k".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,Some("k".to_owned()));
	}
	#[test]
	fn test_get_collection_key_only_col()
	{
		let (col, key)=RestApi::get_collection_key(&"/c".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_none()
	{
		let (col, key)=RestApi::get_collection_key(&"/".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_none_none()
	{
		let (col, key)=RestApi::get_collection_key(&"//".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_empty()
	{
		let (col, key)=RestApi::get_collection_key(&"".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}

}