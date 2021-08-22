use serde::Deserialize;
use tokio::io::AsyncRead;
use csv_async::{AsyncReaderBuilder, Trim};
use futures::stream::StreamExt;
use tokio::sync::oneshot;
use std::fmt::{Debug, Formatter, Display};
use std::error::Error;

#[derive(Deserialize, Debug,Copy,Clone)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
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
    pub client: u16,
    pub tx: u32,
    pub amount: Option<f32>
}

impl ToOwned for Transaction {
    type Owned = Transaction;

    fn to_owned(&self) -> Transaction {
        Transaction{
            trans_type: self.trans_type,
            client: self.client,
            tx: self.tx,
            amount: self.amount,
        }
    }
}

#[derive(Debug)]
pub enum TransactionError {
    InsufficientFund,
    InvalidReferencedTransaction,
    ReferencedTransactionIsNotDisputed,
    NoAmountForTransaction,
    ExistingTransactionId,
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::InsufficientFund => {write!(f, "No available fund")}
            TransactionError::InvalidReferencedTransaction => {write!(f, "Cannot find the transaction based on tx id")}
            TransactionError::ReferencedTransactionIsNotDisputed => {write!(f, "Referenced transaction not under dispute")}
            TransactionError::NoAmountForTransaction => {write!(f, "Invalid transaction, it doesn't have amount")}
            TransactionError::ExistingTransactionId => {write!(f, "Transaction id is already exists")}
        }
    }
}

impl Error for TransactionError{}

pub struct TransactionMessage {
    pub transaction: Transaction,
    pub sender: oneshot::Sender<Result<(), TransactionError>>
}

pub async fn deserialize_csv(tx: tokio::sync::mpsc::Sender<TransactionMessage>, reader: impl AsyncRead + Unpin + Send + Sync)
{

    let mut deserializer = AsyncReaderBuilder::new()
        .trim(Trim::All)
        .create_deserializer(reader);
    let mut records = deserializer.deserialize::<Transaction>();

    while let Some(record) = records.next().await{
        match record {
            Ok(record) => {
                let (otx, orx) = oneshot::channel::<Result<(), TransactionError>>();

                let message = TransactionMessage {
                    transaction: record,
                    sender: otx,
                };

                if tx.send(message).await.is_err() {
                    panic!("Internal server error, cannot send deserialized record to transaction manager!");
                }
                if let Err(err) = orx.await.unwrap() {
                    eprintln!("Transaction error {:?}",err)
                }
            },
            Err(err) => eprintln!("Unable to parse record: {:?}", err)
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::fs::File;
    use crate::csv_parser::*;

    #[tokio::test]
    async fn test_simple_csv_parse() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        let file = File::open("test/parse.csv").await.unwrap();

        tokio::spawn(async move {
            deserialize_csv(tx,file).await;
        });

        let mut transactions = Vec::new();
        while let Some(message) = rx.recv().await {
            transactions.push(message.transaction);
            message.sender.send(Ok(())).unwrap();
        }

        assert!(matches!(transactions[0].trans_type, TransactionType::Dispute));
        assert_eq!(transactions[0].client, 1);
        assert_eq!(transactions[0].tx, 5);
        assert_eq!(transactions[0].amount, None);

        assert!(matches!(transactions[1].trans_type, TransactionType::Deposit));
        assert_eq!(transactions[1].client, 2);
        assert_eq!(transactions[1].tx, 4);
        assert_eq!(transactions[1].amount.unwrap(), 1.0);

        assert!(matches!(transactions[2].trans_type, TransactionType::WithDrawal));
        assert_eq!(transactions[2].client, 3);
        assert_eq!(transactions[2].tx, 3);
        assert_eq!(transactions[2].amount.unwrap(), 3.0);

        assert!(matches!(transactions[3].trans_type, TransactionType::Resolve));
        assert_eq!(transactions[3].client, 4);
        assert_eq!(transactions[3].tx, 2);
        assert_eq!(transactions[3].amount, None);

        assert!(matches!(transactions[4].trans_type, TransactionType::ChargeBack));
        assert_eq!(transactions[4].client, 5);
        assert_eq!(transactions[4].tx, 1);
        assert_eq!(transactions[4].amount, None);
    }
}


