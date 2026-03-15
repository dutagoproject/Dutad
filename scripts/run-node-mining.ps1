param([string]$DataDir='.\data\mainnet',[string]$MiningBind='127.0.0.1:19085')
cargo run -p dutad -- --datadir $DataDir --mining-bind $MiningBind
