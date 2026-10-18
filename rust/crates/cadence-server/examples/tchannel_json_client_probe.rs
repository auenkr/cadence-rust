use cadence_server::tchannel_json_client::{call, JsonCallError};
use serde_json::json;
use std::{collections::HashMap, env, time::Duration};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        return Err("usage: probe ADDR METHOD [null|empty|large|timeout|cancel] [service]".into());
    }
    let mode = args.get(3).map(String::as_str).unwrap_or("null");
    let service = args.get(4).map(String::as_str).unwrap_or("test-json");
    let body = if mode == "large" {
        json!({})
    } else if mode == "large-request" {
        json!({"value": "x".repeat(200000)})
    } else {
        json!({"value":"request"})
    };
    let headers = match mode {
        "empty" => Some(HashMap::new()),
        "null" => None,
        _ => Some(HashMap::from([("x-probe".into(), "present".into())])),
    };
    if mode == "cancel" {
        let addr = args[1].clone();
        let method = args[2].clone();
        let task = tokio::spawn(async move {
            call(
                &addr,
                "test-json",
                &method,
                None,
                &json!({}),
                Duration::from_secs(5),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        task.abort();
        let _ = task.await;
        return Ok(());
    }
    let deadline = if mode == "timeout" {
        Duration::from_millis(100)
    } else {
        Duration::from_secs(5)
    };
    match call(&args[1], service, &args[2], headers, &body, deadline).await {
        Ok(response) => {
            println!("{}", serde_json::to_string(&response.body)?);
            Ok(())
        }
        Err(JsonCallError::Application(value)) => {
            println!("APPLICATION {value}");
            Ok(())
        }
        Err(JsonCallError::Transport(error)) if mode == "timeout" => {
            println!("TIMEOUT {error}");
            Ok(())
        }
        Err(JsonCallError::Transport(error)) => Err(error.into()),
    }
}
