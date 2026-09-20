#![allow(dead_code)]

use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};

const PORT: u16 =  9092;

struct  RequestHeader  {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
}

struct Message  {
    messaage_size: i32,
    request_header: RequestHeader
}

fn handle_connection(mut stream:TcpStream) {
    let mut buf = [0u8; 1240];
    stream.read(&mut buf).unwrap();
    let messaage_size  = i32::from_be_bytes(buf[0..4].try_into().unwrap());
    let _request_api_key = i16::from_be_bytes(buf[4..6].try_into().unwrap());
    let request_api_version = i16::from_be_bytes(buf[6..8].try_into().unwrap());
    let correlation_id =  i32::from_be_bytes(buf[8..12].try_into().unwrap());

    let mut error_code: i16 = 0;
    if request_api_version > 4 {
        error_code = 35;
    }
    stream.write_all(&messaage_size.to_be_bytes()).unwrap();
    stream.write_all(&correlation_id.to_be_bytes()).unwrap();
    stream.write_all(&error_code.to_be_bytes()).unwrap();
}


fn main() {
      println!("Logs from your program will appear here!");
     let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).unwrap();
     for stream in listener.incoming() {
         match stream {
             Ok(stream) => {
                 handle_connection(stream);
             }
             Err(e) => {
                 println!("error: {}", e);
             }
         }
     }
}
