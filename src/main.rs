mod test;
mod stringstream;
mod httpprocessor;
mod indexmanager;
mod persistencemanager;
mod restapi;

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
use restapi::RestApi;
use std::sync::RwLock;

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
			let mut im = Index::from_file(file);
			println!("loading collection {}",&file_name);
			col_indexes.insert(file_name.to_owned(),im);
		}
	}
	col_indexes
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap(); 

	let mut col_indexes = load_indexes(Path::new("./data"));
	
	let persistence_sender = PersistenceManager::start(10);

	let tx=RestApi::start(10,col_indexes,persistence_sender); 

    for incoming in listener.incoming() {
		match incoming {
			Ok(stream) => {
				let tx= tx.clone();
				thread::spawn(move|| 
				{
					println!("Message received ");
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