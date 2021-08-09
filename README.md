# Toy transaction engine

- this project is highly motivated by the [Tokio channels example](https://tokio.rs/tokio/tutorial/channels) 
- using derive to deserialize csv
- using tokio mpsc channel to send deserialized transactions to the transaction manager and tokio oneshot to propagate invalid transactions info back the client
- using asyncreaader and asyncwriter for csv input and output
- errors and invalid transactions printed on the error console 
