use std::env;
use tokio::io::{self};
use tokio::fs::File;
use tokio::sync::mpsc::channel;
use csv_async::{AsyncWriterBuilder};
use std::collections::HashMap;

mod csv_parser;
mod transaction_manager;

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

                while let Some(message) = rx.recv().await {
                    let result = transaction_manager::process_transaction(&mut accounts, &message.transaction).await;
                    if let Err(err )= message.sender.send(result) {
                        eprintln!("Cannot send the transaction process result to the client! : {:?}", err);
                    }
                }

                let mut serializer = AsyncWriterBuilder::new()
                    .delimiter(b',')
                    .create_serializer(io::stdout());

                for account in accounts {
                    if serializer.serialize(account.1).await.is_err(){
                        eprintln!("Unable to deserialize record.");
                    }
                }
            },
            Err(err) => {eprintln!("Cannot open input file {:?}", err); }
        }
}   else {
        eprintln!("Input file argument not provided!");
        eprintln!("Usage parse_csv <source_filepath>");
    }
}
