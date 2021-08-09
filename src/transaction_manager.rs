use crate::csv_parser::{Transaction, TransactionType, TransactionError};
use std::collections::HashMap;

use serde::Serialize;
use std::fmt::{Debug};

#[derive(Serialize, Debug)]
pub struct Account {
    id: u64,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
    #[serde(skip)]
    transactions: HashMap< u64, (bool, Transaction)>,
}

impl Account {
    fn new(id: u64) -> Account{
        Account {
                id: id,
                available: 0.0,
                held: 0.0,
                total: 0.0,
                locked: false,
                transactions: HashMap::new(),
        }
    }
}

pub async fn process_transaction(accounts: &mut HashMap<u64, Account>, transaction: &Transaction) -> Result<(), TransactionError> {

    let account = accounts.entry(transaction.client).or_insert_with(||Account::new(transaction.client));

    manage_transaction(account,transaction).await
}

async fn manage_transaction(account: &mut Account, transaction: &Transaction) -> Result<(), TransactionError> {

    match transaction.trans_type {
        TransactionType::Deposit => {
            account.transactions.insert(transaction.tx,(false,transaction.to_owned()));
            account.available += transaction.amount;
            account.total += transaction.amount;
        },
        TransactionType::WithDrawal => {
            if (account.available - transaction.amount) < 0.0  {
                return Err(TransactionError::InsufficientFund)
            }

            account.transactions.insert(transaction.tx,(false,transaction.to_owned()));
            account.available -= transaction.amount;
            account.total -= transaction.amount;
        },
        TransactionType::Dispute => {
            if !account.transactions.contains_key(&transaction.tx) {
                return Err(TransactionError::InvalidReferencedTransaction)
            }
            let referenced_trans_with_dispute = account.transactions.get_mut(&transaction.tx).unwrap();
            let referenced_transaction = &referenced_trans_with_dispute.1;

            referenced_trans_with_dispute.0 = true;
            account.available -= referenced_transaction.amount;
            account.held += referenced_transaction.amount;
        },
        TransactionType::Resolve => {
            if !account.transactions.contains_key(&transaction.tx) {
                return Err(TransactionError::InvalidReferencedTransaction)
            }
            let referenced_trans_with_dispute = account.transactions.get_mut(&transaction.tx).unwrap();
            let referenced_transaction = &referenced_trans_with_dispute.1;
            if !referenced_trans_with_dispute.0 {
                return Err(TransactionError::ReferencedTransactionIsNotDisputed)
            }
            referenced_trans_with_dispute.0 = false;
            account.available += referenced_transaction.amount;
            account.held -= referenced_transaction.amount;
        },
        TransactionType::ChargeBack => {
            if !account.transactions.contains_key(&transaction.tx) {
                return Err(TransactionError::InvalidReferencedTransaction)
            }
            let referenced_trans_with_dispute = account.transactions.get_mut(&transaction.tx).unwrap();
            let referenced_transaction = &referenced_trans_with_dispute.1;
            if !referenced_trans_with_dispute.0 {
                return Err(TransactionError::ReferencedTransactionIsNotDisputed)
            }
            referenced_trans_with_dispute.0 = false;
            account.total-= referenced_transaction.amount;
            account.held -= referenced_transaction.amount;
            account.locked = true;
        },
    }
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

#[tokio::test]
async fn test_transaction_deposit(){
    let mut account = Account::new(1);

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Deposit,
        tx: 1,
        amount: 1.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await, Ok(_));

    assert_eq!(account.available, 1.0);
    assert_eq!(account.total, 1.0);
}

#[tokio::test]
async fn test_transaction_withdrawal(){
    let mut account = Account::new(1);
    account.available = 1.0;
    account.total = 1.0;
    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::WithDrawal,
        tx: 1,
        amount: 1.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await, Ok(_));

    assert_eq!(account.available, 0.0);
    assert_eq!(account.total, 0.0);
}

#[tokio::test]
async fn test_transaction_withdrawal_error(){
    let mut account = Account::new(1);
    account.available = 1.0;
    account.total = 1.0;
    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::WithDrawal,
        tx: 1,
        amount: 2.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InsufficientFund));
}

#[tokio::test]
async fn test_transaction_dispute_error(){
    let mut account = Account::new(1);
    account.available = 1.0;
    account.total = 1.0;

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Dispute,
        tx: 1,
        amount: 2.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InvalidReferencedTransaction));
}


#[tokio::test]
async fn test_transaction_dispute(){
    let mut account = Account::new(1);

    let deposit = Transaction {
        client: 1,
        trans_type : TransactionType::Deposit,
        tx: 1,
        amount: 1.0,

    };

    manage_transaction(&mut account, &deposit).await.unwrap();

    assert_eq!(account.available, 1.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 0.0);

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Dispute,
        tx: 1,
        amount: 300.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await,Ok(()));
    assert_eq!(account.available, 0.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 1.0);

    assert_eq!(account.transactions.get(&1).unwrap().0, true);
}

#[tokio::test]
async fn test_transaction_resolve(){
    let mut account = Account::new(1);

    let deposit = Transaction {
        client: 1,
        trans_type : TransactionType::Deposit,
        tx: 1,
        amount: 1.0,

    };

    manage_transaction(&mut account, &deposit).await.unwrap();

    assert_eq!(account.available, 1.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 0.0);

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Dispute,
        tx: 1,
        amount: 300.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await,Ok(()));
    assert_eq!(account.available, 0.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 1.0);

    assert_eq!(account.transactions.get(&1).unwrap().0, true);

    let resolve = Transaction {
        client: 1,
        trans_type : TransactionType::Resolve,
        tx: 1,
        amount: 300.0,

    };

    assert_matches!(manage_transaction(&mut account, &resolve).await,Ok(()));
    assert_eq!(account.available, 1.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 0.0);

    assert_eq!(account.transactions.get(&1).unwrap().0, false);
}

#[tokio::test]
async fn test_transaction_chargeback(){
    let mut account = Account::new(1);

    let deposit = Transaction {
        client: 1,
        trans_type : TransactionType::Deposit,
        tx: 1,
        amount: 1.0,

    };

    manage_transaction(&mut account, &deposit).await.unwrap();

    println!("{:?}",account);

    assert_eq!(account.available, 1.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 0.0);

    let transaction = Transaction {
        client: 1,
        trans_type : TransactionType::Dispute,
        tx: 1,
        amount: 300.0,

    };

    assert_matches!(manage_transaction(&mut account, &transaction).await,Ok(()));
    assert_eq!(account.available, 0.0);
    assert_eq!(account.total, 1.0);
    assert_eq!(account.held, 1.0);

    assert_eq!(account.transactions.get(&1).unwrap().0, true);

    let chargeback = Transaction {
        client: 1,
        trans_type : TransactionType::ChargeBack,
        tx: 1,
        amount: 300.0,

    };

    assert_matches!(manage_transaction(&mut account, &chargeback).await,Ok(()));
    assert_eq!(account.available, 0.0);
    assert_eq!(account.total, 0.0);
    assert_eq!(account.held, 0.0);

    assert_eq!(account.locked, true);
}
