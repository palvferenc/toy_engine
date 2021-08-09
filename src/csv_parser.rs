use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType{
    Deposit,
    WithDrawal,
    Dispute,
    Resolve,
    ChargeBack,

}


#[derive(Deserialize)]
pub struct Transaction {
    r#type: TransactionType,
    client: u64,
    tx: u64,
    amount: f32
}
