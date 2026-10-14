use aws_config::BehaviorVersion;
use aws_sdk_sqs::operation::list_queues::ListQueuesOutput;
use lambda_runtime::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use aws_sdk_secretsmanager as secretsmanager;
use aws_sdk_sqs as sqs;

pub const NEXT_TIME: &str = "/extstats/downloader/bggnext";

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct URLRequest {
    pub bgg_api_key: String,
    pub url: String,
    pub not_before: Option<i64>
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadResult {
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

#[derive(Deserialize)]
#[expect(dead_code)]
struct Secrets {
    forum_checker: String,
    geeklist_token: String,
    users_token: String,
    games_token: String,
    extras_token: String,
    plays_token: String,
    collection_token: String
}

pub struct System {
    pub users_token: String,
    pub games_token: String,
    pub extras_token: String,
    pub plays_token: String,
    pub collection_token: String,
    pub bgg_queue1_url: String,
    pub bgg_queue2_url: String,
}

impl System {
    fn new(users_token: String, games_token: String, extras_token: String, plays_token: String,
           collection_token: String, bgg_queue1_url: String, bgg_queue2_url: String) -> Self {
        System { users_token, games_token, extras_token, plays_token, collection_token, bgg_queue1_url, bgg_queue2_url }
    }
}

pub async fn load_system(sqs_client: &sqs::Client) -> Result<System, Error> {
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

    let system = System::new(secrets.users_token, secrets.games_token, secrets.extras_token, secrets.plays_token, secrets.collection_token, url1.clone(), url2.clone());
    Ok(system)
}