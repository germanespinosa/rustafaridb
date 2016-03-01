use std::io::prelude::*;
use std::io::Result;
use std::str::from_utf8;

pub struct StringStream
{
	in_buffer : Vec<u8>,
	out_buffer : Vec<u8>,
	in_position: usize,
}

impl StringStream 
{
	pub fn new (in_value:&[u8]) -> Self
	{
		StringStream {
			in_buffer : in_value.to_owned(),
			out_buffer : vec![],
			in_position : 0,
		}
	}
	pub fn new_reader (in_value:&str) -> Self
	{
		Self::new(&in_value.to_owned().into_bytes())
	}
	pub fn new_writer () -> Self
	{
		Self::new(&vec![])
	}
	pub fn to_string(&self) -> String
	{
		if let Ok(s) = from_utf8(&self.out_buffer)
		{
			s.to_owned()
		}
		else
		{
			"".to_owned()
		}
	}
}

impl Read for StringStream 
{
	fn read(&mut self, buf: &mut [u8])-> Result<usize>
	{
		let mut count=0;
		while self.in_position<self.in_buffer.len() && count< buf.len()
		{
			buf[count]=self.in_buffer[self.in_position];
			self.in_position +=1;
			count +=1;
		}
		Ok(count)
	}
}

impl Write for StringStream 
{
	fn write(&mut self, buf: &[u8])-> Result<usize>
	{
		let mut count=0;
		while count< buf.len()
		{
			self.out_buffer.push(buf[count]);
			count +=1;
		}
		Ok(count)
	}
	fn flush(&mut self)->Result<()>
	{
		self.out_buffer=vec![];
		Ok(())
	}
}

#[cfg(test)]
mod test_string_stream
{
	use super::StringStream;
	use std::str::from_utf8;
	use std::io::prelude::*;	
	
	#[test]
	fn test_new()
	{
		let mut ss=StringStream::new_reader("Testing");
		let mut buf=[0;1024];
		let b = ss.read(&mut buf).unwrap();
		assert_eq!(b,7);
		let r = from_utf8(&buf[0..b]).unwrap();
		assert_eq!(r,"Testing");
	}
	#[test]
	fn test_new_reader()
	{
		let mut ss=StringStream::new_reader("Testing");
		let mut buf=[0;1024];
		let b = ss.read(&mut buf).unwrap();
		assert_eq!(b,7);
		let r = from_utf8(&buf[0..b]).unwrap();
		assert_eq!(r,"Testing");		
	}
	#[test]
	fn test_new_writer()
	{
		let mut ss=StringStream::new_writer();
		let b = ss.write(&"Testing".to_owned().into_bytes()).unwrap();
		assert_eq!(b,7);
		assert_eq!(ss.to_string(),"Testing");
	}
	#[test]
	fn test_multiple_reads()
	{
		let mut ss=StringStream::new_reader("Testing");
		let mut buf=[0;5];
		{
			let b = ss.read(&mut buf).unwrap();
			assert_eq!(b,5);
		
			let r = from_utf8(&buf[0..b]).unwrap();
			assert_eq!(r,"Testi");		
		}
		{
			let b = ss.read(&mut buf).unwrap();
			assert_eq!(b,2);
			let r = from_utf8(&buf[0..b]).unwrap();
			assert_eq!(r,"ng");
		}
	}
	#[test]
	fn test_multiple_writes()
	{
		let mut ss=StringStream::new_writer();
		{
			let b = ss.write(&"Testi".to_owned().into_bytes()).unwrap();
			assert_eq!(b,5);
			assert_eq!(ss.to_string(),"Testi");
		}
		{
			let b = ss.write(&"ng".to_owned().into_bytes()).unwrap();
			assert_eq!(b,2);
			assert_eq!(ss.to_string(),"Testing");
		}
	}
}
