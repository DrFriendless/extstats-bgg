use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};
use aws_config::{BehaviorVersion};
use aws_sdk_ssm as ssm;
use aws_sdk_sqs as sqs;
use std::sync::Arc;
use aws_lambda_events::event::sqs::SqsEvent;
use bggdl::{load_system, DownloadRequest, System};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let sdk_config = Arc::new(aws_config::load_defaults(BehaviorVersion::latest()).await);
    let sqs_client = Arc::new(sqs::Client::new(&sdk_config));
    let ssm_client = Arc::new(ssm::Client::new(&sdk_config));
    let sqs_c = Arc::clone(&sqs_client);
    let system = Arc::new(load_system(&sqs_c).await?);

    lambda_runtime::run(service_fn(move |event| {
        let system = Arc::clone(&system);
        let ssm_client = Arc::clone(&ssm_client);
        let sqs_client = Arc::clone(&sqs_client);
        async move { handler(&system, &ssm_client, &sqs_client, event).await }
    })).await
}

// https://github.com/aws/aws-lambda-rust-runtime
pub(crate) async fn handler(system: &System, ssm_client: &ssm::Client, sqs_client: &sqs::Client,
                            event: LambdaEvent<SqsEvent>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    /*    Context {
            request_id: "e14293ab-fc88-5743-ac9a-f0e338b8c3cc",
            deadline: 1791935001851,
            invoked_function_arn: "arn:aws:lambda:ap-southeast-2:067508173724:function:RustDownloaderStack-rustfnaddtimestamp1599C35F-5EJrfsnZ4IBi:$LATEST",
            xray_trace_id: Some(
                "Root=1-6acec1fb-fb7916fd0fa0cf26792172c9;Parent=68ab5e41ec2792c7;Sampled=0;Lineage=2:746c5daa:0",
            ),
            client_context: None,
            identity: None,
            tenant_id: None,
            env_config: Config {
                function_name: "RustDownloaderStack-rustfnaddtimestamp1599C35F-5EJrfsnZ4IBi",
                memory: 128,
                version: "$LATEST",
                log_stream: "2026/10/13/[$LATEST]6f6010f028d240f39c5f7975beb0bc05",
                log_group: "/aws/lambda/RustDownloaderStack-rustfnaddtimestamp1599C35F-5EJrfsnZ4IBi",
            },
        }
        */

    for record in event.records {
        let Some(body) = record.body else { continue };
        let request: DownloadRequest = serde_json::from_str(&body)?;
        for req in request.requests {
            // TODO
        }
        if record.receipt_handle.is_some() {
            remove_from_queue(system, sqs_client, record.receipt_handle.unwrap().as_str()).await?;
        }
    }
    Ok(json!({}))
}

async fn remove_from_queue(system: &System, sqs_client: &sqs::Client, receipt_handle: &str) -> Result<(), Error> {
    println!("Removing from queue {}", receipt_handle);
    sqs_client.delete_message().queue_url(&system.bgg_queue2_url).receipt_handle(receipt_handle).send().await?;
    Ok(())
}

