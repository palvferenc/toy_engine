use std::env;
use tokio::io::{self};
use tokio::fs::File;
use tokio::sync::mpsc::channel;
use csv_async::{AsyncWriterBuilder};
use std::collections::HashMap;

mod csv_parser;
mod transaction_manager;

#[macro_use]
extern crate matches;


#[tokio::main]
async fn main() {
    if let Some(input_file) = env::args().collect::<Vec<String>>().get(1) {
        match File::open(input_file).await {
            Ok(file) => {
                let (tx, mut rx) = channel(100);

                tokio::spawn(async move {
                    csv_parser::deserialize_csv(tx, file).await;
                });

                let mut accounts = HashMap::new();


                while let Some(transaction) = rx.recv().await {
                    transaction_manager::process_transaction(&mut accounts, &transaction).await;
                }

                let mut serializer = AsyncWriterBuilder::new()
                    .delimiter(b',')
                    .create_serializer(io::stdout());

                for account in accounts {
                    serializer.serialize(account.1).await;
                }
            },
            Err(err) => {eprintln!("Cannot open input file {:?}", err); }
        }
}   else {
        eprintln!("Input file argument not provided!");
        eprintln!("Usage parse_csv <source_filepath>");
    }
}
