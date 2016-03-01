mod test;
mod stringstream;
mod httpprocessor;
mod indexmanager;
mod persistencemanager;
extern crate rustc_serialize;

use indexmanager::IndexManager;
use rustc_serialize::json;
use std::collections::HashMap;
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

fn handle_client<RW:Read + Write>(mut stream : RW, mut col_indexes:&HashMap<String,IndexManager>, persistence : &mut PersistenceManager)
{
	let mut p=HttpProcessor::new();
	p.process_request(&mut stream);
	let (col,key)=get_collection_key(&p.url);
	if let Some(k) = key 
	{
		// let's operate on the item
		let col = col.unwrap();
		println!("-->>>let's {} item {} from collection {}", p.verb, k, col);

		p.response_headers.insert("Server".to_owned(),"Rustafari 1.0".to_owned());

		if let Some(ix) = col_indexes.get(&col)
		{
			match &p.verb[..] 
			{
				"GET" => 
				{
					if let Some (entry) = ix.find_entry(k)
					{
						
						p.response_code = 200;
						p.response_headers.insert("Content-Type".to_owned(),"text/html; charset=utf-8".to_owned());
						let mut response = persistence.read(entry.file.clone(),entry.off_set,entry.size);
						p.send_response(&mut response,&mut stream);
					}
					else
					{
						p.response_code = 404;
					}
				}
				"POST" => 
				{
					if let Some (entry) = ix.find_entry(k)
					{
						p.response_code = 409;
					}
					else
					{
						p.response_code = 200;
					}
				}
				"PUT" => 
				{
					if let Some (entry) = ix.find_entry(k)
					{
						p.response_code = 200;
					}
					else
					{
						p.response_code = 404;
					}
					
				}
				"DELETE" => 
				{
					if let Some (entry) = ix.find_entry(k)
					{
						p.response_code = 200;
					}
					else
					{
						p.response_code = 404;
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

fn load_indexes(path : &Path) -> HashMap<String,IndexManager>
{
	let mut result : HashMap<String,IndexManager>=HashMap::new();
	for entry in fs::read_dir(path).unwrap() {
		let entry = entry.unwrap().path();
		
		let extension = path.extension().unwrap().to_str().unwrap();
		
		if extension == "rix"
		{		
		
			let file_name = path.file_stem().unwrap().to_str().unwrap();
		
			let file = match File::open(&entry) {
				Err(why) => panic!("couldn't open {}: {}", entry.to_str().unwrap(),why),
				Ok(file) => file,
			};
			let im = IndexManager::new(file);
			result.insert(file_name.to_owned(),im);
		}
	}
	result
}

fn main() {
	let (tx, rx) = sync_channel(3);
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();

    println!("loading the indexes");
	
    println!("listening started, ready to accept");	

	let mb = Arc::new(Mutex::new(rx));

	let mut col_indexes = load_indexes(Path::new("./data"));
	let ixc = Arc::new(col_indexes);
	let pm=Arc::new(PersistenceManager::new());

	for i in 0..10 //limit the number of threads to 10
	{
		let mb=mb.clone();
		let mut ixc = ixc.clone();
		let mut pm = pm.clone();
		thread::spawn(move|| 
		{
			loop
			{
				let stream=mb.lock().unwrap().recv().unwrap();
				println!("Processor {} to the rescue",i);
				handle_client(stream,&mut ixc,&mut pm);
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