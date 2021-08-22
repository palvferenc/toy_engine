use crate::csv_parser::{Transaction, TransactionType, TransactionError};
use std::collections::HashMap;

use serde::Serialize;
use std::fmt::{Debug};



#[derive(Serialize, Debug)]
pub struct Account {
    #[serde(rename = "client")]
    id: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
    #[serde(skip)]
    transactions: HashMap< u32, (bool, Transaction)>,
}

impl Account {
    fn new(id: u16) -> Account{
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

pub async fn process_transaction(accounts: &mut HashMap<u16, Account>, transaction: &Transaction) -> Result<(), TransactionError> {
    if !valid_transaction_id(accounts, transaction).await {
        return Err(TransactionError::ExistingTransactionId)
    }
    let account = accounts.entry(transaction.client).or_insert_with(||Account::new(transaction.client));
    manage_transaction(account, transaction).await
}

async fn valid_transaction_id(accounts: &HashMap<u16, Account>, transaction: &Transaction) -> bool {
    match transaction.trans_type {
        TransactionType::Deposit | TransactionType::WithDrawal => {
            for account in accounts.values() {
                if account.transactions.contains_key(&transaction.tx) {
                    return false
                }
            }
            true
        }
        _ => { true }
    }
}

async fn manage_transaction(account: &mut Account, transaction: &Transaction) -> Result<(), TransactionError> {

    match transaction.trans_type {
        TransactionType::Deposit => {
            if let Some(amount) = transaction.amount{
                account.transactions.insert(transaction.tx,(false,transaction.to_owned()));
                account.available += amount;
                account.total += amount;
            }
            else {
                return Err(TransactionError::NoAmountForTransaction)
            }
        },
        TransactionType::WithDrawal => {
            if let Some(amount) = transaction.amount{
                if (account.available - amount) < 0.0  {
                    return Err(TransactionError::InsufficientFund)
                }

                account.transactions.insert(transaction.tx,(false,transaction.to_owned()));
                account.available -= amount;
                account.total -= amount;
            }
            else {
                return Err(TransactionError::NoAmountForTransaction)
            }
        },
        TransactionType::Dispute => {
            if !account.transactions.contains_key(&transaction.tx) {
                return Err(TransactionError::InvalidReferencedTransaction)
            }
            let referenced_trans_with_dispute = account.transactions.get_mut(&transaction.tx).unwrap();
            let referenced_transaction = &referenced_trans_with_dispute.1;

            referenced_trans_with_dispute.0 = true;
            account.available -= referenced_transaction.amount.unwrap();
            account.held += referenced_transaction.amount.unwrap();
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
            account.available += referenced_transaction.amount.unwrap();
            account.held -= referenced_transaction.amount.unwrap();
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
            account.total-= referenced_transaction.amount.unwrap();
            account.held -= referenced_transaction.amount.unwrap();
            account.locked = true;
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use matches::assert_matches;
    use crate::transaction_manager::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_account_create(){
        let mut accounts = HashMap::new();

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
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
            amount: Some(1.0),
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Ok(_));

        assert_eq!(account.available, 1.0);
        assert_eq!(account.total, 1.0);
    }



    #[tokio::test]
    async fn test_transaction_deposit_error_no_amount(){
        let mut account = Account::new(1);

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::NoAmountForTransaction));
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
            amount: Some(1.0),
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Ok(_));

        assert_eq!(account.available, 0.0);
        assert_eq!(account.total, 0.0);
    }

    #[tokio::test]
    async fn test_transaction_withdrawal_error_insufficient_fund(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;
        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::WithDrawal,
            tx: 1,
            amount: Some(2.0),

        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InsufficientFund));
    }

    #[tokio::test]
    async fn test_transaction_withdrawal_error_no_amount(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;
        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::WithDrawal,
            tx: 1,
            amount: None,

        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::NoAmountForTransaction));
    }



    #[tokio::test]
    async fn test_transaction_dispute(){
        let mut account = Account::new(1);

        let deposit = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        manage_transaction(&mut account, &deposit).await.unwrap();

        assert_eq!(account.available, 1.0);
        assert_eq!(account.total, 1.0);
        assert_eq!(account.held, 0.0);

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Dispute,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await,Ok(()));
        assert_eq!(account.available, 0.0);
        assert_eq!(account.total, 1.0);
        assert_eq!(account.held, 1.0);

        assert_eq!(account.transactions.get(&1).unwrap().0, true);
    }

    #[tokio::test]
    async fn test_transaction_dispute_error_invalid_referenced_trans(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Dispute,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InvalidReferencedTransaction));
    }



    #[tokio::test]
    async fn test_transaction_resolve(){
        let mut account = Account::new(1);

        let deposit = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        manage_transaction(&mut account, &deposit).await.unwrap();

        assert_eq!(account.available, 1.0);
        assert_eq!(account.total, 1.0);
        assert_eq!(account.held, 0.0);

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Dispute,
            tx: 1,
            amount: None,
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
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &resolve).await,Ok(()));
        assert_eq!(account.available, 1.0);
        assert_eq!(account.total, 1.0);
        assert_eq!(account.held, 0.0);

        assert_eq!(account.transactions.get(&1).unwrap().0, false);
    }

    #[tokio::test]
    async fn test_transaction_resolve_error_invalid_referenced_trans(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Resolve,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InvalidReferencedTransaction));
    }

    #[tokio::test]
    async fn test_transaction_resolve_error_referenced_trans_not_dispute(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        account.transactions.insert(transaction.tx,(false,transaction));

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Resolve,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::ReferencedTransactionIsNotDisputed));
    }

    #[tokio::test]
    async fn test_transaction_chargeback(){
        let mut account = Account::new(1);

        let deposit = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
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
            amount: None,
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
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &chargeback).await,Ok(()));
        assert_eq!(account.available, 0.0);
        assert_eq!(account.total, 0.0);
        assert_eq!(account.held, 0.0);

        assert_eq!(account.locked, true);
    }

    #[tokio::test]
    async fn test_transaction_chargeback_error_invalid_referenced_trans(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::ChargeBack,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::InvalidReferencedTransaction));
    }

    #[tokio::test]
    async fn test_transaction_chargeback_error_referenced_trans_not_dispute(){
        let mut account = Account::new(1);
        account.available = 1.0;
        account.total = 1.0;

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        account.transactions.insert(transaction.tx,(false,transaction));

        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::ChargeBack,
            tx: 1,
            amount: None,
        };

        assert_matches!(manage_transaction(&mut account, &transaction).await, Err(TransactionError::ReferencedTransactionIsNotDisputed));
    }

    #[tokio::test]
    async fn test_process_transaction_error_transaction_id_not_unique(){

        let mut accounts = HashMap::new();


        let transaction = Transaction {
            client: 1,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        assert_matches!(process_transaction(&mut accounts, &transaction).await, Ok(_));

        let transaction_overlap_deposit = Transaction {
            client: 2,
            trans_type : TransactionType::Deposit,
            tx: 1,
            amount: Some(1.0),
        };

        assert_matches!(process_transaction(&mut accounts, &transaction_overlap_deposit).await, Err(TransactionError::ExistingTransactionId));

        let transaction_overlap_withdrawal = Transaction {
            client: 3,
            trans_type : TransactionType::WithDrawal,
            tx: 1,
            amount: Some(1.0),
        };

        assert_matches!(process_transaction(&mut accounts, &transaction_overlap_withdrawal).await, Err(TransactionError::ExistingTransactionId));
    }
}


