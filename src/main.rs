use std::{
    io::{self, Read, Write},
    net::TcpListener,
};

use crate::db::{
    manifest::Manifest,
    sstable::{MainSSTable, SSTable},
    storage_engine::StorageEngine,
};

mod db;
fn main() -> io::Result<()> {
    let mut walnut_db = StorageEngine::new()?;
    let mut manifest = Manifest::new(0);
    let mut main_table = SSTable::new("MANIFEST/current.sst")?;
    let mut main_store = MainSSTable::new();
    walnut_db.inital_load(&mut main_table, &mut manifest, &mut main_store)?;
    manifest.load_count()?;

    loop {
        let listner = TcpListener::bind("127.0.0.1:9098")?;

        let (mut socket, _) = listner.accept()?;
        let mut buffer = vec![0u8; 4096];

        loop {
            socket.write_all(b"walnutDB: ")?;
            let bytes = socket.read(&mut buffer)?;

            if bytes == 0 {
                println!("Client disconnected");
                break;
            }

            let message = String::from_utf8(buffer[..bytes].to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let operation = message.split(" ").collect::<Vec<&str>>()[0];

            match operation {
                "GET" => {
                    let mut input = message.split_whitespace();
                    let _ = input.next();
                    let key = input.next();

                    match key {
                        Some(key) => {
                            let response = walnut_db.find_one(
                                key.as_bytes().to_vec(),
                                &mut main_table,
                                &mut main_store,
                            )?;

                            socket.write_all(response.to_string().as_bytes())?;
                            socket.write_all(b"\n")?;
                        }
                        _ => {
                            socket.write_all(b"Please enter a valid query\n")?;
                        }
                    }
                }
                "SET" => {
                    let mut input = message.split_whitespace();
                    let _ = input.next();
                    let key = input.next();
                    let value = input.next();

                    match (key, value) {
                        (Some(key), Some(value)) => {
                            walnut_db.insert(
                                key.as_bytes().to_vec(),
                                value.as_bytes().to_vec(),
                                &mut main_table,
                                &mut manifest,
                                &mut main_store,
                            )?;
                            socket.write_all(b"Inserted\n")?;
                        }
                        _ => {
                            socket.write_all(b"Please enter a valid query\n")?;
                        }
                    }
                }
                "DEL" => {}
                _ => {
                    socket.write_all(b"Please enter a valid operation\n")?;
                }
            }
        }
    }
}
