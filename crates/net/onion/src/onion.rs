use reqwest::{Client, Proxy, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tracing::warn;

use alloy_primitives::{b256, hex, Bytes, B256};

// MOO: reuse?
#[derive(Serialize, Deserialize, Debug)]
pub struct SendRawTxRequest {
    jsonrpc: String,
    method: String,
    params: Vec<String>,
    id: u32,
}

// MOO: reuse?
#[derive(Serialize, Deserialize, Debug)]
pub struct SendRawTxResponse {
    result: String,
    error: Option<Value>,
    id: u32,
}

pub struct TorJsonRpcClient {
    client: Client,
    endpoint: Url,
}

impl TorJsonRpcClient {
    fn new(
        endpoint: String,
        tor_proxy: String,
        dial_timeout: Duration,
        keep_alive: Duration,
        request_timeout: Duration,
        idle_conn_timeout: Duration,
        max_idle_conns: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = Url::parse(format!("http://{}", endpoint).as_str())?;
        let proxy = Proxy::http(format!("socks5h://{}", tor_proxy))?;

        let client = Client::builder()
            .proxy(proxy)
            .timeout(request_timeout)
            .connect_timeout(dial_timeout)
            .pool_idle_timeout(idle_conn_timeout)
            .pool_max_idle_per_host(max_idle_conns)
            .tcp_keepalive(Some(keep_alive))
            .build()?;

        Ok(Self { client, endpoint })
    }

    // MOO: error handling!
    async fn send_tx(
        &self,
        tx: &Bytes,
        id: u32,
    ) -> Result<B256, Box<dyn std::error::Error + Send + Sync>> {
        let request = new_send_raw_tx_request(tx, id);
        let response = self
            .client
            .post(self.endpoint.as_str())
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;
        
        let rpc_response: SendRawTxResponse = response.json().await?;

        if rpc_response.id != request.id {
            return Err(
                format!("ID mismatch: expected {}, got {}", request.id, rpc_response.id).into()
            );
        }

        if let Some(error) = rpc_response.error {
            return Err(format!("RPC Error: {}", error).into());
        }

        return Ok(rpc_response.result.parse::<B256>()?);
    }
}

pub struct Onion {
    peers: Vec<TorJsonRpcClient>,
    request_id: AtomicU32,
    next_peer: AtomicU32,
}

fn new_send_raw_tx_request(tx: &Bytes, id: u32) -> SendRawTxRequest {
    let rlp_hex = hex::encode_prefixed(tx);
    SendRawTxRequest {
        jsonrpc: "2.0".to_string(),
        method: "eth_sendRawTransaction".to_string(),
        params: vec![rlp_hex],
        id,
    }
}

#[derive(Debug)]
pub struct OnionConfig {
    pub tor_proxy: String,
    pub addresses: Vec<String>,
    pub dial_timeout: Duration,
    pub keep_alive: Duration,
    pub request_timeout: Duration,
    pub idle_conn_timeout: Duration,
    pub max_idle_conns: usize,
}

impl OnionConfig {
    pub fn default() -> Self {
        Self {
            tor_proxy: "127.0.0.1:9050".to_string(),
            addresses: vec![],
            dial_timeout: Duration::from_secs(60),
            keep_alive: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
            idle_conn_timeout: Duration::from_secs(60),
            max_idle_conns: 0usize,
        }
    }

    pub fn with_peers(self, addresses: Vec<String>) -> Self {
        Self {
            tor_proxy: self.tor_proxy,
            addresses,
            dial_timeout: self.dial_timeout,
            keep_alive: self.keep_alive,
            request_timeout: self.request_timeout,
            idle_conn_timeout: self.idle_conn_timeout,
            max_idle_conns: self.max_idle_conns,
        }
    }
}

impl Onion {
    pub fn try_new(config: OnionConfig) -> Result<Self, String> {
        let OnionConfig {
            addresses,
            tor_proxy,
            dial_timeout,
            keep_alive,
            request_timeout,
            idle_conn_timeout,
            max_idle_conns,
        } = config;
        let peers: Vec<TorJsonRpcClient> = addresses
            .into_iter()
            .filter_map(|addr| {
                TorJsonRpcClient::new(
                    addr,
                    tor_proxy.clone(),
                    dial_timeout,
                    keep_alive,
                    request_timeout,
                    idle_conn_timeout,
                    max_idle_conns,
                )
                .ok()
            })
            .collect();

        if peers.is_empty() {
            return Err("no valid peers!".to_string());
        }

        Ok(Self { peers, request_id: AtomicU32::new(0), next_peer: AtomicU32::new(0) })
    }

    // MOO: err?
    pub async fn submit_tx(&self, tx: &Bytes, retries: usize) -> Result<B256, String> {
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        for attempt in 0..retries {
            let peer_id = self.next_peer.fetch_add(1, Ordering::SeqCst) as usize % self.peers.len();
            let peer = &self.peers[peer_id];
            let result = peer.send_tx(tx, id).await;

            match result {
                Ok(hash) => {
                    return Ok(hash);
                }
                Err(err) => {
                    warn!("Failed to send tx to {} ({})", peer.endpoint, err);
                }
            }

            if attempt + 1 < retries {
                tokio::time::sleep(Duration::from_secs(attempt as u64 + 1)).await;
            }
        }

        return Err(format!("failed to send tx through TOR after {} retries", retries));
    }
}
