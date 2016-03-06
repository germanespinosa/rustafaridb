#[cfg(test)]
mod test_main
{
	use super::super::{get_collection_key};
	use std::path::Path;
	use std::fs;
	
	#[test]	
	fn count_indexes()
	{
		let mut counter = 0;
		for entry in fs::read_dir(Path::new("./data")).unwrap() {
			let entry = entry.unwrap().path();
			
			let extension = entry.extension().unwrap().to_str().unwrap();
			
			if extension == "rix"
			{		
				counter+=1;
			}
		}
		assert_eq!(counter,1);
	}
	
	#[test]
	fn test_get_file_etx()
	{
		let path = Path::new("/tmp/foo/bar.txt");
		let file = path.file_stem().unwrap().to_str().unwrap();
		let extension = path.extension().unwrap().to_str().unwrap();
		assert_eq!("bar".to_owned(),file);
		assert_eq!("txt".to_owned(),extension);
	}
	#[test]
	fn test_get_file_name()
	{
		let file_name="FOO.txt";
		assert_eq!("FOO".to_owned(),file_name[..file_name.len()-4].to_owned());
	}
	#[test]
	fn test_get_collection_key_with_extra()
	{
		let (col, key)=get_collection_key(&"/c/k/1234".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,Some("k".to_owned()));
	}
	#[test]
	fn test_get_collection_key()
	{
		let (col, key)=get_collection_key(&"/c/k".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,Some("k".to_owned()));
	}
	#[test]
	fn test_get_collection_key_only_col()
	{
		let (col, key)=get_collection_key(&"/c".to_owned());		
		assert_eq!(col,Some("c".to_owned()));
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_none()
	{
		let (col, key)=get_collection_key(&"/".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_none_none()
	{
		let (col, key)=get_collection_key(&"//".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}
	#[test]
	fn test_get_collection_key_empty()
	{
		let (col, key)=get_collection_key(&"".to_owned());		
		assert_eq!(col,None);
		assert_eq!(key,None);
	}
	#[test]
	fn test_hash()
	{
		use std::hash::{Hash, SipHasher, Hasher};

		#[derive(Hash)]
		struct Person {
			id: u32,
			name: String,
			phone: u64,
		}

		let person1 = Person { id: 5, name: "Janet".to_string(), phone: 555_666_7777 };
		let person2 = Person { id: 5, name: "Bob".to_string(), phone: 555_666_7777 };

		assert_eq!(hash(&person1) % 9, 7);
		assert_eq!(hash(&person2) % 9, 5);

		fn hash<T: Hash>(t: &T) -> u64 {
			let mut s = SipHasher::new();
			t.hash(&mut s);
			s.finish()
		}	
	}
	
}
