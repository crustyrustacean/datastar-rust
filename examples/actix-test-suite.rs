mod sdk_test;

use {
    actix_web::{App, HttpServer, Responder, web},
    asynk_strim::{Yielder, stream_fn},
    core::error::Error,
    datastar::{
        DatastarEvent,
        actix::{ReadSignals, Sse},
    },
    sdk_test::TestCase,
    tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    HttpServer::new(|| {
        App::new()
            // The conformance test runner uses a combination of GET, POST, and
            // QUERY requests. `web::route` dispatches on the method, and
            // `ReadSignals` reads the query parameter for GET/DELETE and the
            // JSON body otherwise.
            .route("/test", web::route().to(test))
    })
    .bind(("127.0.0.1", 9200))?
    .run()
    .await?;

    Ok(())
}

async fn test(ReadSignals(test_case): ReadSignals<TestCase>) -> impl Responder {
    Sse::new(stream_fn(
        |mut yielder: Yielder<DatastarEvent>| async move {
            for event in test_case.events {
                yielder.yield_item(event.into_datastar_event()).await;
            }
        },
    ))
}
