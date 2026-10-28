use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use aws_sdk_ssm as ssm;
use std::sync::Arc;
use aws_lambda_events::event::sqs::SqsEvent;
use std::time::{SystemTime, UNIX_EPOCH};
use bggdl::{load_system, DownloadRequest, NEXT_TIME, System};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let system = Arc::new(load_system().await?);

    lambda_runtime::run(service_fn(move |event| {
        let system = Arc::clone(&system);
        async move { handler(&system, event).await }
    })).await
}

// https://github.com/aws/aws-lambda-rust-runtime
pub(crate) async fn handler(system: &System, event: LambdaEvent<SqsEvent>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    let mut next_time = load_next_time(&system).await?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_millis();
    if next_time < now { 
        println!("Increasing next time by {} seconds", (now - next_time) / 1000);
        next_time = now; 
    }

    for record in event.records {
        let Some(body) = record.body else { continue };
        let mut request: DownloadRequest = serde_json::from_str(&body)?;
        for req in request.requests.iter_mut() {
            req.not_before = Some(next_time);
            request.scheduled = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is before 1970")
                .as_millis();
            let delta = (next_time as i128 - request.scheduled as i128) / 1000;
            println!("timestamping {} {} {} delta {}", request.request_id , req.url, next_time / 1000, delta);
            next_time += 5000;
        }
        let body = serde_json::to_string(&request)?;
        println!("{}", body);
        send_to_queue(system, body).await?;
        if record.receipt_handle.is_some() {
            remove_from_queue(system, record.receipt_handle.unwrap().as_str()).await?;
        } else {
            println!("no receipt handle");
        }
        // would be safer to write it in the loop, but we probably don't need to be that paranoid
        write_next_time(&system, next_time).await?;
    }
    Ok(json!({}))
}

async fn load_next_time(system: &System) -> Result<u128, Error> {
    let now_ms: u128 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_millis();
    let resp = system.ssm_client.get_parameter().name(NEXT_TIME).send().await?;
    println!("Next time: {:?}", resp);
    let ns = resp.parameter().and_then(|p| p.value()).ok_or(now_ms.to_string())?;
    Ok(ns.trim().parse::<u128>()?)
}

async fn write_next_time(system: &System, when: u128) -> Result<(), Error> {
    system.ssm_client
        .put_parameter().name(NEXT_TIME)
        .value(when.to_string())
        .r#type(ssm::types::ParameterType::String)
        .overwrite(true)
        .send()
        .await?;
    Ok(())
}

async fn send_to_queue(system: &System, body: String) -> Result<(), Error> {
    let result = system.sqs_client
        .send_message()
        .queue_url(&system.bgg_queue2_url)
        .message_body(body)
        .message_group_id("add_timestamp")
        .send().await;
    println!("send result {:?}", result);
    Ok(())
}

async fn remove_from_queue(system: &System, receipt_handle: &str) -> Result<(), Error> {
    system.sqs_client.delete_message().queue_url(&system.bgg_queue1_url).receipt_handle(receipt_handle).send().await?;
    Ok(())
}

