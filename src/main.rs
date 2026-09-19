use std::net::TcpListener;

const PORT: u16 =  9092;

fn main() {
    println!("Logs from your program will appear here!");

     let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).unwrap();
     for stream in listener.incoming() {
         match stream {
             Ok(_stream) => {
                 println!("accepted new connection");
             }
             Err(e) => {
                 println!("error: {}", e);
             }
         }
     }
}
