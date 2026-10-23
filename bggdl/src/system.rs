use aws_config::BehaviorVersion;
use aws_sdk_sqs::operation::list_queues::ListQueuesOutput;
use lambda_runtime::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use aws_sdk_secretsmanager as secretsmanager;
use aws_sdk_sqs as sqs;
use aws_sdk_ssm as ssm;
use aws_sdk_s3 as s3;
use aws_sdk_lambda as lambda;
use std::fmt;

pub const NEXT_TIME: &str = "/extstats/downloader/bggnext";

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct URLRequest {
    pub bgg_api_key: String,
    pub url: String,
    pub not_before: Option<i64>
}

// #[derive(Debug, Deserialize, Serialize)]
// #[serde(rename_all = "camelCase")]
// pub struct URLResponse {
//     pub url: String,
//     pub result: DownloadResult
// }
//
// impl URLResponse {
//     pub fn new(url: String, result: DownloadResult) -> Self {
//         URLResponse { url, result }
//     }
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DownloadRequest {
    #[serde(rename = "type")]
    pub kind: String,
    pub requests: Vec<URLRequest>,
    pub request_id: String,
    // for continuations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_data: Option<Value>,
    // for callbacks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DownloadResult {
    pub url: String,
    #[serde(rename = "type")]
    pub kind: String,
    // kind = s3
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    // kind = content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    // kind = failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContinuationDownloadResponse {
    pub request_id: String,
    pub results: Vec<DownloadResult>,
    pub continuation_data: Option<Value>
}

impl fmt::Display for ContinuationDownloadResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = serde_json::to_string_pretty(self).map_err(|_| fmt::Error)?;
        f.write_str(&json)
    }
}

impl ContinuationDownloadResponse {
    pub fn new(request_id: String, results: Vec<DownloadResult>, continuation_data: Option<Value>) -> Self {
        ContinuationDownloadResponse { request_id, results, continuation_data }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallbackDownloadResponse {
    pub request_id: String,
    pub responses: Vec<DownloadResult>
}

impl CallbackDownloadResponse {
    pub fn new(request_id: String, responses: Vec<DownloadResult>) -> Self {
        CallbackDownloadResponse { request_id, responses }
    }
}

impl DownloadResult {
    pub fn s3(url: String, bucket: String, object_key: String) -> Self {
        DownloadResult { url, kind: "s3".to_string(), bucket: Some(bucket), object_key: Some(object_key), content: None, error: None }
    }

    pub fn content(url: String, content: String) -> Self {
        DownloadResult { url, kind: "content".to_string(), bucket: None, object_key: None, content: Some(content), error: None }
    }

    pub fn failure(url: String, error: String) -> Self {
        DownloadResult { url, kind: "failure".to_string(), bucket: None, object_key: None, content: None, error: Some(error) }
    }
}

#[derive(Deserialize)]
#[expect(dead_code)]
struct Secrets {
    forum_checker: String,
    geeklist_token: String,
    users_token: String,
    games_token: String,
    extras_token: String,
    plays_token: String,
    collection_token: String,
    cache_bucket: String,
}

pub struct System {
    pub users_token: String,
    pub games_token: String,
    pub extras_token: String,
    pub plays_token: String,
    pub collection_token: String,
    pub bgg_queue1_url: String,
    pub bgg_queue2_url: String,
    pub ssm_client: ssm::Client,
    pub sqs_client: sqs::Client,
    pub s3_client: s3::Client,
    pub reqwest_client: reqwest::Client,
    pub lambda_client: lambda::Client,
    pub cache_bucket: String,
}

impl System {
    fn new(users_token: String, games_token: String, extras_token: String, plays_token: String,
           collection_token: String, bgg_queue1_url: String, bgg_queue2_url: String, cache_bucket: String,
           ssm_client: ssm::Client, sqs_client: sqs::Client, reqwest_client: reqwest::Client,
           s3_client: s3::Client, lambda_client: lambda::Client) -> Self {
        System { users_token, games_token, extras_token, plays_token, collection_token,
            bgg_queue1_url, bgg_queue2_url, ssm_client, sqs_client, reqwest_client, s3_client, lambda_client, cache_bucket }
    }
}

pub async fn load_system() -> Result<System, Error> {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let sqs_client = sqs::Client::new(&sdk_config);
    let ssm_client = ssm::Client::new(&sdk_config);
    let reqwest_client = reqwest::Client::new();
    let s3_client = s3::Client::new(&sdk_config);
    let lambda_client = lambda::Client::new(&sdk_config);

    // Loads region and credentials from the default provider chain
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = secretsmanager::Client::new(&config);

    // load BGG secrets
    let resp = client
        .get_secret_value()
        .secret_id("extstats/bgg") // name or ARN
        .send()
        .await?;
    let secret_string = resp.secret_string().ok_or("secret has no string value")?;
    // If the secret is stored as JSON (the common case)
    let secrets: Secrets = serde_json::from_str(secret_string)?;

    // load queues
    let resp: ListQueuesOutput = sqs_client.list_queues().queue_name_prefix("downloader").max_results(10).send().await?;
    let url1 = resp.queue_urls().iter().find(|qu| qu.ends_with("BGGQueue1")).ok_or_else(|| "BGG Queue1 not found")?;
    let url2 = resp.queue_urls().iter().find(|qu| qu.ends_with("BGGQueue2")).ok_or_else(|| "BGG Queue2 not found")?;

    let system = System::new(secrets.users_token, secrets.games_token, secrets.extras_token,
                             secrets.plays_token, secrets.collection_token, url1.clone(), url2.clone(),
                             secrets.cache_bucket, ssm_client, sqs_client, reqwest_client, s3_client,
                             lambda_client);
    Ok(system)
}