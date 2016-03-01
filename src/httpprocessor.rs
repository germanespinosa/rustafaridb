use std::str::from_utf8;
use std::collections::HashMap;
use std::io::prelude::*;

pub struct HttpProcessor
{
	pub verb:String,
	pub http_version:String,
	pub url:String,
	pub request_headers:HashMap<String,String>,
	pub request_body:Vec<u8>,
	pub response_headers:HashMap<String,String>,
	pub response_code:usize,
}

impl HttpProcessor 
{
	pub fn new()-> Self
	{
		HttpProcessor 
		{
			url:String::new(),
			verb:String::new(),
			http_version:String::new(),
			request_headers:HashMap::new(),
			request_body:vec![],
			response_headers:HashMap::new(),
			response_code:0,
		}
	}
	pub fn process_request<R:Read>(&mut self, request_stream:&mut R)
	{
		let mut buf=[0;8192]; //the limit fo http request is 8k
		
		let mut red = match request_stream.read(&mut buf) //move the request to a byte_array
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
		println!(">>>>>>>>1 msg");
		let (verb, url, version) = Self::process_start_line(get_line(red));
		println!(">>>>>>>>1.1 msg");
		self.verb=verb;
		self.url=url;
		self.http_version=version;
		println!(">>>>>>>>2 msg");
		red = next_line(red);
		while let Some((h,v))=Self::process_header(get_line(red))
		{
			self.request_headers.insert(h,v);
			red = next_line(red);
		}
		self.request_body = red.to_owned();
	}
	pub fn send_response<W:Write, R:Read>(&mut self, instream : &mut R, mut outstream : &mut W)
	{
		let mut msg = format!("{} {}\r\n",self.http_version, self.response_code);
		for (h,v) in &self.response_headers
		{
			msg = msg + &format!("{}: {}\r\n",h,v);
		}
		msg = msg + &"\r\n";
		//println!("{}",msg);
		outstream.write(msg.as_bytes()).unwrap();
		
		let mut buf=[0;102400];
		let mut byte_count=0;
		if let Ok(b)=instream.read(&mut buf	)
		{
			byte_count=b;
		}
		while byte_count>0
		{
			//println!("read :{}",byte_count);
			if let Ok(_)=outstream.write(&buf[0..byte_count])
			{
				if let Ok(b)=instream.read(&mut buf	)
				{
					byte_count=b;
				}
				else
				{
					byte_count=0;
				}
			}
			else
			{ 
				panic!("Error sending http response");
			}
		}
	}	
	pub fn process_start_line(start_line:&[u8])->(String,String,String)
	{
		if let Some(first_split) = find_char(&start_line,' ')
		{
			let verb = &start_line[..first_split];
			if let Some(second_split) = find_char(&start_line[1 + first_split..],' ')
			{
				let url = &start_line[first_split+1..][..second_split];
				let version=&start_line[first_split+1..][second_split + 1 ..];
				(from_utf8(&verb).unwrap().to_owned(),from_utf8(&url).unwrap().to_owned(),from_utf8(&version).unwrap().to_owned())
			}
			else
			{
				(from_utf8(&verb).unwrap().to_owned(),from_utf8(&start_line[first_split + 1..]).unwrap().to_owned(),"".to_owned())
			}
		}
		else
		{
			(from_utf8(&start_line).unwrap().to_owned(),"".to_owned(),"".to_owned())
		}
	}
	fn process_header(line:&[u8])->Option<(String,String)>
	{
		match find_char(&line,':')
		{
			Some(splitter)=>
			{
				let h = &line[0..splitter];
				let v=&line[splitter+1..];
				Some((from_utf8(&h).unwrap().trim().to_owned(),from_utf8(&v).unwrap().trim().to_owned()))
			}
			None=>
			{
				let sole_header=from_utf8(&line).unwrap().trim();
				if sole_header.len()==0
				{
					None
				}
				else
				{
					Some((sole_header.to_owned(),"".to_owned()))
				}
			}
		}
	}
}

fn find_char(s:&[u8], c:char) -> Option<usize>
{
	let mut i = 0;
	let c= c as u8;
	while i<s.len() 
	{
		if c==s[i] {return Some(i)};
		i+=1;
	}
	None
}
 
fn get_line(s:&[u8]) -> &[u8]
{
	if s.len()==0 {return s};
	match find_char(s,'\n')
	{
		Some(i)=>
		{
			&s[..i-1]
		},
		None=>
		{
			&s
		}
		
	}
}

fn next_line(s:&[u8]) -> &[u8]
{
	match find_char(s,'\n')
	{
		Some(i)=>
		{
			&s[i+1..]
		},
		None=>
		{
			&s[s.len()..]
		}
		
	}
}

#[cfg(test)]
mod test_httpprocessor
{
	use stringstream::StringStream;
	use super::{HttpProcessor,get_line,next_line,find_char};
	use std::str::from_utf8;
	
	#[test]
	fn test_find_char()
	{
		let buf=b"first line\r\nseco1d line\r\nthird line\r\nfourth line";
		let pos = find_char(buf,'1').unwrap();
		assert_eq!(pos,16);
	}

	#[test]
	fn test_find_char_not_found()
	{
		let buf=b"first line\r\nsecond line\r\nthird line\r\nfourth line";
		let pos = find_char(buf,'0');
		assert_eq!(pos,None);
	}

	#[test]
	fn test_get_line()
	{
		let buf=b"first line\r\nsecond line\r\nthird line\r\nfourth line";
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"first line");
	}

	#[test]
	fn test_get_multiple_lines()
	{
		let buf=b"first line\r\nsecond line\r\nthird line\r\nfourth line";
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"first line");
		let buf = next_line(buf);
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"second line");
		let buf = next_line(buf);
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"third line");
		let buf = next_line(buf);
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"fourth line");
	}

	#[test]
	fn test_get_line_end_of_stream()
	{
		let buf=b"first line";
		let line = get_line(buf);
		assert_eq!(from_utf8(&line).unwrap(),"first line");
	}

	#[test]
	fn test_next_line()
	{
		let buf=b"first line\r\nsecond line\r\nthird line\r\nfourth line";
		let buf = next_line(buf);
		assert_eq!(from_utf8(buf).unwrap(),"second line\r\nthird line\r\nfourth line");
	}

	#[test]
	fn test_next_line_end_of_stream()
	{
		let buf=b"first line";
		let buf = next_line(buf);
		assert_eq!(from_utf8(buf).unwrap(),"");
	}
	
	#[test]
	fn test_process_start_line()
	{
		let req = b"GET /test HTTP1.1";
		let (a,b,c) = HttpProcessor::process_start_line(req);
		assert_eq!(a,"GET".to_owned());
		assert_eq!(b,"/test".to_owned());
		assert_eq!(c,"HTTP1.1".to_owned());
	}
	
	#[test]
	fn test_process_request_start_line()
	{
		let req = "GET /test HTTP1.1\r\nHeader1: value\r\nHeader2: value2\r\n\r\n";
		let mut ss=StringStream::new_reader(req);
		let mut ct = HttpProcessor::new();
		ct.process_request(&mut ss);
		assert_eq!(ct.verb,"GET".to_owned());
		assert_eq!(ct.url,"/test".to_owned());
		assert_eq!(ct.http_version,"HTTP1.1".to_owned());
	}

	#[test]
	fn test_process_request()
	{
		let req = "GET /test HTTP1.1\r\nHeader1: value1\r\nHeader2: value2\r\n\r\n";
		let mut ss=StringStream::new_reader(req);
		let mut ct = HttpProcessor::new();
		ct.process_request(&mut ss);
		let v =ct.request_headers.get(&"Header1".to_owned());
		assert_eq!(v,Some(&"value1".to_owned()));
		let v =ct.request_headers.get(&"Header2".to_owned());
		assert_eq!(v,Some(&"value2".to_owned()));
		assert_eq!(ct.verb,"GET".to_owned());
		assert_eq!(ct.url,"/test".to_owned());
		assert_eq!(ct.http_version,"HTTP1.1".to_owned());
	}

	#[test]
	fn test_send_response()
	{
		let req = "GET /test HTTP1.1\r\nHeader1: value1\r\nHeader2: value2\r\n\r\n";
		let mut ss=StringStream::new_reader(req);
		let mut ct = HttpProcessor::new();
		ct.process_request(&mut ss);

		let response = "Hello!";
		let mut response_stream=StringStream::new_reader(response);

		let mut out_stream=StringStream::new_writer();

		ct.response_code=200;
		ct.send_response(&mut response_stream, &mut out_stream);
		assert_eq!(out_stream.to_string(),"HTTP1.1 200\r\n\r\nHello!".to_owned());
	}
	#[test]
	fn test_send_response_one_header()
	{
		let req = "GET /test HTTP1.1\r\nHeader1: value1\r\nHeader2: value2\r\n\r\n";
		let mut ss=StringStream::new_reader(req);
		let mut ct = HttpProcessor::new();
		ct.process_request(&mut ss);

		let response = "Hello!";
		let mut response_stream=StringStream::new_reader(response);

		let mut out_stream=StringStream::new_writer();

		ct.response_code=200;
		ct.response_headers.insert("header".to_owned(),"value".to_owned());
		ct.send_response(&mut response_stream, &mut out_stream);
		assert_eq!(out_stream.to_string(),"HTTP1.1 200\r\nheader: value\r\n\r\nHello!".to_owned());
	}
	
	#[test]
	fn test_process_header()
	{
		let (h,v)=HttpProcessor::process_header(&"header: value".to_owned().into_bytes()).unwrap();		
		assert_eq!(h,"header");
		assert_eq!(v,"value");
	}
	#[test]
	fn test_process_header_multiple_spaces()
	{
		let (h,v)=HttpProcessor::process_header(&"header:      value".to_owned().into_bytes()).unwrap();		
		assert_eq!(h,"header");
		assert_eq!(v,"value");
	}
	#[test]
	fn test_process_header_empty()
	{
		let (h,v)=HttpProcessor::process_header(&"header".to_owned().into_bytes()).unwrap();		
		assert_eq!(h,"header");
		assert_eq!(v,"");
	}
	#[test]
	fn test_process_header_space()
	{
		let (h,v)=HttpProcessor::process_header(&"header : value".to_owned().into_bytes()).unwrap();		
		assert_eq!(h,"header");
		assert_eq!(v,"value");
	}
	#[test]
	fn test_process_empty_header()
	{
		let a=HttpProcessor::process_header(&"".to_owned().into_bytes());		
		assert_eq!(a,None);
	}

}