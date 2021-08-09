use crate::csv_parser::{Transaction, TransactionType};
use std::collections::HashMap;

use serde::Serialize;
use std::error::Error;
use std::fmt::{Debug, Formatter, Display};

#[derive(Serialize, Debug)]
pub struct Account {
    id: u64,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

#[derive(Debug)]
pub enum TransactionError {
    InvalidOperation
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::InvalidOperation => {write!(f, "Test")}
        }
    }
}

impl Error for TransactionError{}


pub async fn process_transaction(accounts: &mut HashMap<u64, Account>, transaction: &Transaction) -> Result<(), TransactionError> {
    if !accounts.contains_key(&transaction.client) {
        accounts.insert(
            transaction.client,
            Account {
                id: transaction.client,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
            },
        );
    }
    if let Some(account) = accounts.get_mut(&transaction.client) {
        manage_transaction(account,transaction).await?;
    } else {
        panic!("Cannot get account from store!");
    }
    Ok(())
}

async fn manage_transaction(account: &mut Account,transaction: &Transaction) -> Result<(), TransactionError> {
    Ok(())
}


#[tokio::test]
async fn test_account_create(){
    let mut accounts = HashMap::new();

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Deposit,
        tx: 1,
        amount: 1.0,

    };

    assert_matches!(process_transaction(&mut accounts, &transaction).await, Ok(_));

    assert!(!accounts.is_empty());
    assert_eq!(accounts.get(&transaction.client).unwrap().id, 1);
}
