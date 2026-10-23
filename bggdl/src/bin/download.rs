use lambda_runtime::{service_fn, Error};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use aws_lambda_events::event::sqs::SqsEvent;
use bggdl::{load_system, CallbackDownloadResponse, ContinuationDownloadResponse, DownloadRequest, DownloadResult, System};
use durable_execution_sdk::{durable_execution, DurableError, Duration};
use std::collections::HashMap;
use aws_sdk_lambda::types::InvocationType;
use thiserror::Error;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_lambda::primitives::Blob;

static DEPS: OnceLock<System> = OnceLock::new();

#[derive(Debug, Error)]
pub enum BggError {
    #[error("rate limit exceeded")]
    RateLimited(),

    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    DEPS.set(load_system().await?).map_err(|_| "dependencies already initialized")?;
    lambda_runtime::run(service_fn(handler)).await
}

#[durable_execution]
async fn handler(event: SqsEvent, context: DurableContext) -> Result<(), DurableError> {
    let system = DEPS.get().unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_millis() as i64;
    let mut count = 0;
    let mut results: HashMap<String, DownloadResult> = HashMap::new();
    for record in event.records {
        let Some(body) = record.body else { continue };
        let request: DownloadRequest = serde_json::from_str(&body)?;
        for req in &request.requests {
            let found = results.get(req.url.as_str());
            if found.is_none() {
                // println!("looking for {}", req.url.as_str());
                if req.not_before.is_none() {
                    continue;
                }
                if now < req.not_before.unwrap() {
                    let wait_ms: u64 = (req.not_before.unwrap() - now) as u64;
                    println!("{}: Got to wait {} milliseconds", request.request_id, wait_ms);
                    let wait_secs = wait_ms.div_ceil(1000);
                    context.wait(Duration::from_seconds(wait_secs), Some("wait1")).await?;
                }
                println!("{}: Now I am allowed to download {}", request.request_id, req.url);
                let api_key = req.bgg_api_key.clone();
                let url = req.url.clone();

                let dlr: DownloadResult = context.step(|_| async move {
                    match fetch_xml_from_bgg(system, &api_key, &url).await {
                        Ok(xml) => Ok(DownloadResult::content(url, xml)),
                        Err(e) => Ok(DownloadResult::failure(url, e.to_string())),
                    }
                }, None).await?;
                count+=1;
                results.insert(req.url.clone(), dlr);
            }
        }
        let failures = results.values().filter(|dlr| { dlr.kind == "failure" }).count();
        if failures == 0 {
            write_to_s3_if_needed(system, &mut results, &request).await;
            dispatch(system, &request, &results).await?;
        } else {
            // TODO - retry
        }
        if record.receipt_handle.is_some() {
            remove_from_queue(system, record.receipt_handle.unwrap().as_str()).await?;
        }
    }
    // println!("Finished downloading {} URLs", count);

    // deliver the result
    // for (url, result) in &results {
    //     println!("{url}: kind={}, error={:?}", result.kind, result.error);
    // }
    Ok(())
}

async fn dispatch(system: &System, request: &DownloadRequest, results: &HashMap<String, DownloadResult>) -> Result<(), Error> {
    let ordered_results: Vec<DownloadResult> = request.requests.iter()
        .map(|r| results.get(&r.url).unwrap().clone())
        .collect::<Vec<_>>();
    match request.kind.as_str() {
        "continuation" => {
            let payload= ContinuationDownloadResponse::new(request.request_id.clone(), ordered_results, request.continuation_data.clone());
            let bytes = serde_json::to_vec(&payload)?;
            // println!("Continuation function: {}", request.function_name.as_ref().unwrap());
            // println!("Continuation payload:\n{}", &payload);
            let resp = system.lambda_client.invoke()
                .function_name(request.function_name.clone().unwrap().clone())
                .set_invocation_type(Some(InvocationType::Event))
                .set_payload(Some(Blob::new(bytes)))
                .send().await?;
            // println!("Response:\n{:?}", &resp);
            Ok(())
        },
        "callback" => {
            let payload= CallbackDownloadResponse::new(request.request_id.clone(), ordered_results);
            let bytes = serde_json::to_vec(&payload)?;
            let callback_id = request.callback_id.clone().ok_or("callback_id missing")?;
            system.lambda_client
                .send_durable_execution_callback_success()
                .callback_id(callback_id)
                .result(Blob::new(bytes))
                .send()
                .await?;
            Ok(())
        },
        _ => Err("Request kind unknown".into())
    }
}

async fn write_to_s3_if_needed(system: &System, results: &mut HashMap<String, DownloadResult>, request: &DownloadRequest) {
    let contents = results.values().filter(|dlr| { dlr.kind == "content" }).count();
    if contents > 1 {
        // write contents to S3
        let mut index = 0;
        for req in &request.requests {
            let dlr = results.get(req.url.as_str()).unwrap();
            if dlr.kind == "content" {
                results.insert(req.url.clone(), write_to_s3(system, req.url.clone(), dlr.content.as_ref().unwrap(), &request.request_id, index).await);
            }
            index += 1;
        }
    } else if contents == 1 && results.len() == 1 && results.values().find(|_| { true }).unwrap().content.as_ref().unwrap().len() > 65536 {
        // write contents to S3
        let req = &request.requests[0];
        let dlr = results.get(req.url.as_str()).unwrap();
        results.insert(req.url.clone(), write_to_s3(system, req.url.clone(), dlr.content.as_ref().unwrap(), &request.request_id, 0).await);
    }
}

async fn write_to_s3(system: &System, url: String, content: &str, request_id: &str, index: i32) -> DownloadResult {
    let object_key = format!("{request_id}-{index}");
    match system.s3_client
        .put_object()
        .bucket(&system.cache_bucket)
        .key(&object_key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
    {
        Ok(_) => DownloadResult::s3(url, system.cache_bucket.to_string(), object_key),
        Err(e) => DownloadResult::failure(url, e.to_string()),
    }
}

async fn fetch_xml_from_bgg(system: &System, token: &str, url: &str) -> Result<String, BggError> {
    let xml = system.reqwest_client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/xml")
        .bearer_auth(token)
        .send()
        .await?
        .text()
        .await?;
    if xml.contains("Rate limit exceeded") {
        println!("Rate limit exceeded downloading {url}");
        return Err(BggError::RateLimited());
    }
    Ok(xml)
}

async fn remove_from_queue(system: &System, receipt_handle: &str) -> Result<(), Error> {
    println!("Removing from queue {}", receipt_handle);
    system.sqs_client.delete_message().queue_url(&system.bgg_queue2_url).receipt_handle(receipt_handle).send().await?;
    Ok(())
}

