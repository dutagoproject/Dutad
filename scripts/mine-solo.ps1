param([string]$Rpc='http://127.0.0.1:19085',[string]$Address='YOUR_DUTA_ADDRESS',[int]$Threads=8)
cargo run -p dutad --bin dutaminer -- --rpc $Rpc --address $Address --threads $Threads
