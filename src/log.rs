use std::io::prelude::*;
use std::thread;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::sync::Arc;
use std::fs::File;


static mut log_level:u8 = 1; // 1:all 2:err&wrn 3:err 4:nothing
static mut log_sender:Option<Sender<String>> = None;

fn log(log_str:&str, log_type:u8)
{
    if log_type>=log_level
    {
        if let Some(s) = log_sender
        {
            s.send(log_str.to_owned());
        }
    }
}

pub fn start(log_stream: File)
{
    let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();
    log_sender=Some(tx);
    thread::spawn(move|| 
    { 
        loop
        {
            let log_string=rx.recv().unwrap();
            log_stream.write(&log_string.into_bytes());
        }
    });
}
