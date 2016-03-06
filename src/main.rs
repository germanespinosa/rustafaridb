mod test;
mod stringstream;
mod httpprocessor;
mod indexmanager;
mod persistencemanager;
extern crate rustc_serialize;

use indexmanager::{IndexEntry,Index};
use rustc_serialize::json;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use stringstream::StringStream;
use httpprocessor::HttpProcessor;
use std::path::Path;
use std::fs;
use std::net::{TcpListener,TcpStream};
use std::thread;
use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::mem::size_of;
use persistencemanager::PersistenceManager;
use std::sync::mpsc::{Receiver,Sender};


const CREATE : usize = 0;
const UPDATE : usize = 1;
const DELETE : usize = 2;


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

fn handle_client<RW:Read + Write>(mut stream : RW, col_indexes:&mut HashMap<String,Index>, persistence : &mut PersistenceManager)
{
	let mut p=HttpProcessor::new();
	p.process_request(&mut stream);
	let (col,key)=get_collection_key(&p.url);
	if let Some(k) = key 
	{
		// let's operate on the item
		let col = col.unwrap();
		println!("-->>>let's {} item {} from collection {}", p.verb, k, col);

		p.response_headers.insert("Server".to_owned(),"RustafariDB 1.0".to_owned());
		p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());

		
		if let Occupied(mut ixmanager) = col_indexes.entry(col.clone())
		{
			let mut ix = ixmanager.get_mut();
			match &p.verb[..] 
			{
				"GET" => 
				{
					if let Some (entry) = ix.find_entry(&k)
					{
						p.response_code = 200;
						if let Some(mut response) = persistence.read(&entry.file,entry.off_set,entry.size)
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
				"POST" => 
				{
					//I perform a non blocking search first to discard the obvious conflictive cases
					let mut found=false; 
					{if let Some (entry) = ix.find_entry(&k) {found=true};}
					if found
					{
						p.response_code = 409;
						p.send_response(&mut StringStream::new_reader("Conflictive key"),&mut stream);
					}
					else
					{
						// If I didn't find the key so I write the record in the file
						// still not blocking so this record might be discarded, but I don't really care
						if let Ok((file,off_set)) = persistence.write(&col,&p.request_body)
						{
							//record saved to file properly, let's create the index entry
							let mut ie = IndexEntry 
							{
								key:k,
								size:p.request_body.len(),
								file:file,
								off_set:off_set,
							};
							//let's try to update the index this is the only blocking operation
							if let Ok(container) = ix.insert_entry(ie)
							{
								// the was not duplicated and the index was updated correctly
								p.response_code = 200;
								p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
								p.send_response(&mut StringStream::new_reader("Inserted Ok"),&mut stream);
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
						else //I/O Error
						{
								p.response_code = 503;
								p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
						}
					}
				}
				"PUT" => 
				{
					//I perform a non blocking search first to discard the obvious conflictive cases
					let mut found=false; 
					{if let Some (entry) = ix.find_entry(&k) {found=true};}
					if !found
					{
						p.response_code = 404;
						p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
					}
					else
					{
						// If I didn't find the key so I write the record in the file
						// still not blocking so this record might be discarded, but I don't really care
						if let Ok((file,off_set)) = persistence.write(&col,&p.request_body)
						{
							//record saved to file properly, let's create the index entry
							let mut ie = IndexEntry 
							{
								key:k,
								size:p.request_body.len(),
								file:file,
								off_set:off_set,
							};
							//let's try to update the index this is the only blocking operation
							if let Ok(container) = ix.update_entry(ie)
							{
								// nobody deleted the record while we were writin the file
								p.response_code = 200;
								p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
								p.send_response(&mut StringStream::new_reader("Updated Ok"),&mut stream);
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
						else //I/O Error
						{
								p.response_code = 503;
								p.send_response(&mut StringStream::new_reader("Server error"),&mut stream);
						}
					}
				}
				"DELETE" => 
				{
					//let's try to update the index this is the only blocking operation
					if let Ok(_) = ix.remove_entry(k)
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
				_ =>
				{
				}
			}
		}
		else
		{
		
		}
	}
	else
	{
		// let's operate on the collection
		println!("-->>>let's {} collection {}", p.verb, col.unwrap());
	}
	return;
	let file_name = format!(".{}", p.url);
	if  let Ok(metadata) = fs::metadata(&file_name)
	{
		if metadata.is_dir()
		{
			p.response_code = 200;
			p.response_headers.insert("Server".to_owned(),"Rustafari 1.0".to_owned());
			p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
			let mut response=format!("<html><head></head><body>");

			println!("Listing {:?} ..." , &file_name);
			for entry in fs::read_dir(&Path::new(&file_name)).unwrap() {
				let entry = entry.unwrap().path();
				let file_url = entry.to_str().unwrap();
				let file_description = entry.to_str().unwrap();
				response=response + &format!("<a href=\"/{}\">{}</a><br/>",file_url,file_description);
				println!("{}",&entry.to_str().unwrap());
			}
			response=response + &format!("</body></html>");
			p.response_headers.insert("Content-Length".to_owned(),format!("{}",response.len()));
			p.send_response(&mut StringStream::new_reader(&response),&mut stream);
		}
		else
		{
			println!("Size of {:?} is {} bytes",file_name, metadata.len());
			let mut file = File::open(&Path::new(&file_name)).unwrap();
			p.response_code = 200;
			p.response_headers.insert("Server".to_owned(),"Rustafari 1.0".to_owned());
			p.response_headers.insert("Content-Type".to_owned(),"application/octet-stream".to_owned());
			p.response_headers.insert("Content-Length".to_owned(),format!("{}",metadata.len()));
			p.response_headers.insert("Content-Disposition".to_owned(),"attachment".to_owned());
			p.send_response(&mut file,&mut stream);
		}
	}
	else
	{
		println!("File {:?} not found",file_name);
		p.response_code = 404;
		p.send_response(&mut StringStream::new_reader("Not Found"),&mut stream);
	}
}

fn load_indexes(path : &Path) -> HashMap<String,Index>
{
	let mut col_indexes : HashMap<String,Index>=HashMap::new();

	for entry in fs::read_dir(path).unwrap() {
		let entry = entry.unwrap().path();
		
		let extension = entry.extension().unwrap().to_str().unwrap();
		
		if extension == "rix"
		{	
			let file_name = entry.file_stem().unwrap().to_str().unwrap();
		
			let file = match File::open(&entry) {
				Err(why) => panic!("couldn't open {}: {}", entry.to_str().unwrap(),why),
				Ok(file) => file,
			};
			let mut im = Index::new(file);
			println!("loading collection {}",&file_name);
			col_indexes.insert(file_name.to_owned(),im);
		}
	}
	col_indexes
}


/*
let (tx, rx) = sync_channel(0);
thread::spawn(move|| 
{
	let mut index = im;
	loop
	{
		let (operation, entry)= rx.recv().unwrap();
		match operation
		{
			CREATE=>
			{
				index.insert_entry(entry)
			}
			UPDATE=>
			{
				index.update_entry(entry)
			}
			DELETE=>						
			{
				index.delete_entry(entry)
			}
		}
	}
});		
col_senders.insert(file_name.to_owned(),tx);
*/

fn main() {
	let (tx, rx) = sync_channel(3);
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();

    println!("loading the indexes");
	
    println!("listening started, ready to accept");	

	let mb = Arc::new(Mutex::new(rx));

	let mut col_indexes = load_indexes(Path::new("./data"));
	
	let mut ixc = Arc::new(Mutex::new(col_indexes));

	for i in 0..10 //limit the number of threads to 10
	{
		let mb=mb.clone();
		let mut ixc = ixc.clone();
		thread::spawn(move|| 
		{
			let mut pm = PersistenceManager::new();
			loop
			{
				let stream=mb.lock().unwrap().recv().unwrap();
				println!("Processor {} to the rescue",i);
				handle_client(stream,&mut ixc.lock().unwrap(),&mut pm);
			}
		});		
	}

    for incoming in listener.incoming() {
		match incoming {
			Ok(stream) => {
				let tx= tx.clone();
				thread::spawn(move|| {
					if let Err(_)=tx.send(stream)
					{
						panic!("problems passing the request to the processor");
					}
				});
			}
			Err(_) => { panic!("connection failed!"); }
		}
    }
	drop(listener);
}