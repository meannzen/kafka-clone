use std::{io::{ Write}, net::TcpListener};

const PORT: u16 =  9092;

fn main() {
      println!("Logs from your program will appear here!");
     let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).unwrap();
     for stream in listener.incoming() {
         match stream {
             Ok(mut stream) => {
                 stream.write_all(&[0,0,0,0]).unwrap();//  message_size any just set to 0
                 stream.write_all(&[0,0,0,7]).unwrap();// correclation_id 7
             }
             Err(e) => {
                 println!("error: {}", e);
             }
         }
     }
}
