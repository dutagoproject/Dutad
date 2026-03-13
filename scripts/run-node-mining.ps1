param([string]$DataDir='.\data\mainnet',[string]$MiningBind='0.0.0.0:19085')
cargo run -p dutad -- --datadir $DataDir --mining-bind $MiningBind
