use lambda_runtime::{service_fn, Error};
use std::sync::OnceLock;
use aws_lambda_events::event::sqs::SqsEvent;
use bggdl::{load_next_time, load_system, now_millis, write_next_time, CallbackDownloadResponse, ContinuationDownloadResponse, DownloadRequest, DownloadResult, System};
use durable_execution_sdk::{durable_execution, DurableError, Duration};
use std::collections::HashMap;
use aws_sdk_lambda::types::InvocationType;
use thiserror::Error;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_lambda::primitives::Blob;

static DEPS: OnceLock<System> = OnceLock::new();
const BIG: usize = 65536;

#[derive(Debug, Error)]
pub enum BggError {
    #[error("rate limit exceeded")]
    RateLimited(),
    #[error("some other error")]
    SomethingElse(),

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

    for record in event.records {
        let Some(body) = record.body else { continue };
        let request: DownloadRequest = serde_json::from_str(&body)
            .inspect_err(|e| println!("Failed to parse request body: {e}"))?;
        let mut results: HashMap<String, DownloadResult> = HashMap::new();

        for (index, req) in request.requests.iter().enumerate() {
            let found = results.get(req.url.as_str());
            if found.is_none() {
                let (wait_ms, now) = context.step_named("calc wait time",|_| async {
                    let not_before = load_next_time(&system).await?;
                    let now = now_millis();
                    let wms = if now < not_before { not_before - now } else { 0 };
                    Ok::<(u128, u128), _>((wms, now))
                }, None).await?;
                let wait_secs = wait_ms / 1000;
                let already_secs = (now - request.initiated) / 1000;
                println!("{}: Got to wait {} seconds, already running for {} seconds. {}", request.request_id, wait_secs, already_secs, req.url );
                if wait_secs > 0 {
                    context.wait(Duration::from_seconds(wait_secs as u64), Some("rate limit wait")).await.inspect_err(|e| println!("Wait of {wait_secs} seconds failed: {e}"))?;
                }
                context.step_named("write next time",|_| async {
                    write_next_time(system, now + 5000).await?;
                    Ok(())
                }, None).await?;

                println!("{}: Now I am allowed to download {}", request.request_id, req.url);
                let api_key = req.bgg_api_key.clone();
                let url = req.url.clone();
                let req_id_for_filename = request.request_id.clone();
                let dlr = context.step_named("BGG download",|_| async move {
                    match fetch_xml_from_bgg(system, &api_key, &url).await {
                        Ok(xml) => {
                            if xml.len() > BIG {
                                Ok(write_to_s3(&system, req.url.clone(), &xml, &req_id_for_filename, index as i32).await)
                            } else {
                                Ok(DownloadResult::content(url, xml))
                            }
                        },
                        Err(e) => Ok(DownloadResult::failure(url, e.to_string())),
                    }
                }, None).await;
                match dlr {
                    Ok(result) => {
                        results.insert(req.url.clone(), result);
                    }
                    _ => {
                        println!("Download Error: {:?}", dlr);
                    }
                }
            }
        }
        let failures = results.values().filter(|dlr| { dlr.kind == "failure" }).count();
        if failures > 0 {
            let fails: Vec<&DownloadResult> = results.values().filter(|dlr| { dlr.kind == "failure" }).collect();
            println!("Download Failures: {:?}", fails);
            continue;
        }
        let request_for_dispatch = request.clone();
        let results_for_dispatch = results.clone();
        dispatch(system, &request_for_dispatch, &results_for_dispatch, now_millis())
            .await
            .inspect_err(|e| println!("Dispatch Error: {}", e))?;
        if record.receipt_handle.is_some() {
            let receipt = record.receipt_handle.clone();
            remove_from_queue(system, &receipt.unwrap())
                .await
                .inspect_err(|e| println!("Remove Error: {}", e))?;
        }
    }
    Ok(())
}

async fn dispatch(system: &System, request: &DownloadRequest, results: &HashMap<String, DownloadResult>, downloaded: u128) -> Result<(), Error> {
    let ordered_results: Vec<DownloadResult> = request.requests.iter()
        .map(|r| results.get(&r.url).cloned().ok_or_else(|| format!("no result recorded for {}", r.url)))
        .collect::<Result<Vec<_>, String>>()
        .inspect_err(|e| println!("Error sorting requests: {:?}", e))?;
    match request.kind.as_str() {
        "continuation" => {
            let payload= ContinuationDownloadResponse::new(
                request.request_id.clone(), ordered_results.clone(), request.continuation_data.clone(), request.attempts + 1,
                request.initiated, request.scheduled, downloaded, request.received, request.completed
            );
            let bytes = serde_json::to_vec(&payload)
                .inspect_err(|e| {
                    println!("Error converting to JSON: {:?}", e);
                    for dlr in ordered_results {
                        println!("{} {}", dlr.url, dlr.content.as_deref().unwrap_or("(none)"));
                    }
                })?;
            // println!("Continuation function: {}", request.function_name.as_ref().unwrap());
            // println!("Continuation payload:\n{}", &payload);
            let resp = system.lambda_client.invoke()
                .function_name(request.function_name.clone().unwrap().clone())
                .set_invocation_type(Some(InvocationType::Event))
                .set_payload(Some(Blob::new(bytes)))
                .send().await
                .inspect_err(|e| println!("Error sending to Lambda {}: {:?}", request.function_name.as_deref().unwrap_or("(none)"), e))
                ?;
            if resp.status_code != 202 {
                println!("Continuation Error: {:?}", resp);
            }
            Ok(())
        },
        "callback" => {
            let payload= CallbackDownloadResponse::new(
                request.request_id.clone(), ordered_results, request.attempts + 1,
                request.initiated, request.scheduled, downloaded, request.received, request.completed);
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

async fn write_to_s3(system: &System, url: String, content: &str, request_id: &str, index: i32) -> DownloadResult {
    let object_key = format!("{request_id}-{index}.xml");
    match system.s3_client
        .put_object()
        .bucket(&system.cache_bucket)
        .key(&object_key)
        .content_type("text/xml")
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
        .await;
    match xml {
        Ok(xml) => {
            if xml.contains("Rate limit exceeded") {
                println!("Rate limit exceeded downloading {url}");
                Err(BggError::RateLimited())
            } else {
                Ok(xml)
            }
        },
        _ => { println!("{:?}", xml); Err(BggError::SomethingElse()) }
    }
}

async fn remove_from_queue(system: &System, receipt_handle: &str) -> Result<(), Error> {
    println!("Removing from queue {}", receipt_handle);
    let resp = system.sqs_client.delete_message().queue_url(&system.bgg_queue2_url).receipt_handle(receipt_handle).send().await;
    println!("SqsResponse: {:?}", resp);
    Ok(())
}


