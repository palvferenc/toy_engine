use serde::Deserialize;
use tokio::io::AsyncRead;
use csv_async::{AsyncReaderBuilder, Trim};
use futures::stream::StreamExt;
use tokio::fs::File;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType{
    Deposit,
    WithDrawal,
    Dispute,
    Resolve,
    ChargeBack,

}


#[derive(Deserialize, Debug)]
pub struct Transaction {
    #[serde(alias = "type")]
    pub trans_type: TransactionType,
    pub client: u64,
    pub tx: u64,
    pub amount: f32
}

pub async fn deserialize_csv(tx: tokio::sync::mpsc::Sender<Transaction>, reader: impl AsyncRead + Unpin + Send + Sync)
{
    let mut deserializer = AsyncReaderBuilder::new()
        .trim(Trim::All)
        .create_deserializer(reader);
    let mut records = deserializer.deserialize::<Transaction>();

    while let Some(record) = records.next().await{
        match record {
            Ok(record) => {
                if tx.send(record).await.is_err() {
                    panic!("Internal server error, cannot send deserialized record to transaction manager!");
                }
            },
            Err(err) => eprintln!("Unable to parse record: {:?}", err)
        }
    }
}
#[tokio::test]
async fn test_simple_csv_parse() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    let file = File::open("test/parse.csv").await.unwrap();

    tokio::spawn(async move {
     deserialize_csv(tx,file).await;
    });

    let mut transactions = Vec::new();
    while let Some(transaction) = rx.recv().await {
        transactions.push(transaction);
    }

    assert!(matches!(transactions[0].trans_type, TransactionType::Deposit));
    assert_eq!(transactions[0].client, 1);
    assert_eq!(transactions[0].tx, 1);
    assert_eq!(transactions[0].amount, 1.0);

    assert!(matches!(transactions[1].trans_type, TransactionType::WithDrawal));
    assert_eq!(transactions[1].client, 2);
    assert_eq!(transactions[1].tx, 5);
    assert_eq!(transactions[1].amount, 3.0);
}
