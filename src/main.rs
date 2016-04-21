mod test;
mod stringstream;
mod httpprocessor;
mod indexmanager;
mod persistencemanager;
mod restapi;
mod log;

use indexmanager::IndexPersistence;
use httpprocessor::HttpProcessor; 
use persistencemanager::PersistenceManager;
use restapi::RestApi;
use std::io;
use std::io::prelude::*;


fn main() {

	let persistence_sender = PersistenceManager::start(10);
    
    let (ip_sender, col_indexes) = IndexPersistence::start("./indexes".to_owned(), 8192, 10);

	let tx = RestApi::start(10, col_indexes, ip_sender.clone(), persistence_sender); 

    HttpProcessor::start("127.0.0.1".to_owned(),8080,tx);

    println!("Welcome to RustafariDB v1.1");    
    println!("Type 'quit' to finish!");
    let mut command = String::new();
    while command != "quit".to_owned()
    {
        let mut rf = String::new(); 
        print!("!->");
        io::stdout().flush().unwrap();
        io::stdin().read_line(&mut rf)
            .ok()
            .expect("Failed to read line");
        
        command = rf.clone().trim().to_owned();        
    }
    IndexPersistence::stop (ip_sender);

}