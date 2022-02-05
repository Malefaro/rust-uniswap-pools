use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::{env, fs, os};

use csv::Writer;
use web3::contract::tokens::{Detokenize, Tokenize};
use web3::contract::{Contract, Options};
use web3::Web3;

use web3::ethabi::{
    Contract as AbiContract, Event, EventParam, Int, Log, ParamType, RawLog, Token, Topic, Uint,
};
use web3::futures::FutureExt;
use web3::futures::{future, Future, Stream, StreamExt};
use web3::transports::WebSocket;
use web3::types::{Address, FilterBuilder, H160, H256, U256, U64};

fn wei_to_eth(wei_val: U256) -> f64 {
    let res = wei_val.as_u128() as f64;
    res / 1_000_000_000_000_000_000.0
}

#[derive(Debug, serde::Serialize)]
struct PoolInfo {
    pub pool_addr: H160,
    pub token0_name: String,
    pub token0_symbol: String,
    pub token1_name: String,
    pub token1_symbol: String,
    pub fee: usize,
    pub token0_addr: H160,
    pub token1_addr: H160,
    pub block_number: u64,
}

impl PoolInfo {
    pub fn from_pool_created_event(
        event: PoolCreatedEvent,
        token_infos: &HashMap<H160, TokenInfo>,
    ) -> PoolInfo {
        PoolInfo {
            pool_addr: event.pool,
            token0_name: token_infos.get(&event.token0.clone()).unwrap().name.clone(),
            token1_name: token_infos.get(&event.token1.clone()).unwrap().name.clone(),
            token0_symbol: token_infos
                .get(&event.token0.clone())
                .unwrap()
                .symbol
                .clone(),
            token1_symbol: token_infos
                .get(&event.token1.clone())
                .unwrap()
                .symbol
                .clone(),
            fee: event.fee,
            token0_addr: event.token0,
            token1_addr: event.token1,
            block_number: event.block_number.as_u64(),
        }
    }
}

#[derive(Debug, Default)]
struct PoolCreatedEvent {
    token0: H160,
    token1: H160,
    fee: usize,
    tick_spacing: usize,
    pool: H160,
    block_number: U64,
}

impl PoolCreatedEvent {
    pub fn from_log(log: Log) -> PoolCreatedEvent {
        let mut pool_event = PoolCreatedEvent::default();
        for param in log.params.into_iter() {
            match param.name.as_str() {
                "token0" => pool_event.token0 = param.value.into_address().unwrap(),
                "token1" => pool_event.token1 = param.value.into_address().unwrap(),
                "fee" => pool_event.fee = param.value.into_uint().unwrap().as_usize(),
                "tick_spacing" => {
                    pool_event.tick_spacing = param.value.into_int().unwrap().as_usize()
                }
                "pool" => pool_event.pool = param.value.into_address().unwrap(),
                _ => continue,
            }
        }
        pool_event
    }
}

struct TokenInfo {
    name: String,
    symbol: String,
    address: H160,
    // decimals: U256,
}

async fn get_token_info(web3s: Web3<WebSocket>, addr: H160) -> TokenInfo {
    let token_contract =
        Contract::from_json(web3s.eth(), addr, include_bytes!("token_abi.json")).unwrap();
    async fn caller<T: Detokenize + Default>(contract: &Contract<WebSocket>, name: &str) -> T {
        let token_data: T = contract
            .query(name, (), None, Options::default(), None)
            .await
            .unwrap_or_default();
        token_data
    }
    let token_name: String = caller(&token_contract, "name").await;
    let token_symbol: String = caller(&token_contract, "symbol").await;
    // let decimals: U256 = caller(&token_contract, "decimals").await;
    TokenInfo {
        name: token_name,
        address: addr,
        // decimals,
        symbol: token_symbol,
    }
}

async fn parse_logs_data(web3s: Web3<WebSocket>, address: H160) {
    let filter = FilterBuilder::default()
        .address(vec![address])
        .from_block(12369621.into()) // genesis block of uniswap factory
        .build();
    let params = vec![
        EventParam {
            name: "token0".to_string(),
            kind: ParamType::Address,
            indexed: true,
        },
        EventParam {
            name: "token1".to_string(),
            kind: ParamType::Address,
            indexed: true,
        },
        EventParam {
            name: "fee".to_string(),
            kind: ParamType::Uint(24),
            indexed: true,
        },
        EventParam {
            name: "tickSpacing".to_string(),
            kind: ParamType::Int(24),
            indexed: false,
        },
        EventParam {
            name: "pool".to_string(),
            kind: ParamType::Address,
            indexed: false,
        },
    ];
    let event = Event {
        name: "PoolCreated".to_string(),
        inputs: params,
        anonymous: false,
    };
    let logs = web3s.eth_filter().create_logs_filter(filter).await.unwrap();
    let l = logs.logs().await.unwrap();
    let mut token_infos: HashMap<H160, TokenInfo> = HashMap::new();
    let mut writer = Writer::from_writer(vec![]);
    let total_len = l.len();
    println!("total_len: {total_len}");
    for (i, log) in l.into_iter().enumerate() {
        let processed = i as f64 / total_len as f64;
        if i % 10 == 0 {
            println!("processed {processed}");
        }
        let lr = event.parse_log(RawLog {
            topics: log.topics.clone(),
            data: log.data.clone().0,
        });
        let l = match lr {
            Ok(l) => l,
            Err(err) => {
                println!("{err}");
                continue;
            }
        };
        let mut pce = PoolCreatedEvent::from_log(l);
        pce.block_number = log.block_number.unwrap_or(U64::from(0));
        if !token_infos.contains_key(&pce.token0) {
            token_infos.insert(pce.token0, get_token_info(web3s.clone(), pce.token0).await);
        }
        if !token_infos.contains_key(&pce.token1) {
            token_infos.insert(pce.token1, get_token_info(web3s.clone(), pce.token1).await);
        }
        let pool_info = PoolInfo::from_pool_created_event(pce, &token_infos);
        writer.serialize(pool_info).unwrap();
    }
    let csv_data = String::from_utf8(writer.into_inner().unwrap()).unwrap();
    fs::write("pools.csv", csv_data).unwrap();
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    dotenv::dotenv().ok();

    let websocket = web3::transports::WebSocket::new(&env::var("INFURA_URL").unwrap()).await?;
    let web3s = web3::Web3::new(websocket);
    parse_logs_data(
        web3s.clone(),
        Address::from_str("0x1F98431c8aD98523631AE4a59f267346ea31F984").unwrap(),
    )
    .await;
    Ok(())
}
