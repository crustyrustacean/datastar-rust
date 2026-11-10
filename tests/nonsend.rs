//! Verifies that `Sse` accepts a non-`Send` stream.
//!
//! Actix-Web runs handlers *on* the worker thread rather than moving them
//! between threads, so `Handler::Future` and `HttpResponse::streaming` carry
//! no `Send` bound. `Sse` must not be stricter than the framework it wraps.

use {
    actix_web::{App, body::to_bytes, test, web},
    datastar::{DatastarEvent, actix::Sse, consts},
    futures_util::{StreamExt, stream},
    std::{cell::Cell, rc::Rc, time::Duration},
};

fn event() -> DatastarEvent {
    DatastarEvent {
        event: consts::EventType::PatchElements,
        id: None,
        retry: Duration::from_millis(consts::DEFAULT_SSE_RETRY_DURATION),
        data: vec!["<div id=\"target\">hi</div>".to_owned()],
    }
}

#[actix_web::test]
async fn sse_accepts_non_send_stream() {
    // Rc<Cell<usize>> is !Send, and the stream holds it across each yield.
    // Shared with the handler so the test can observe it being advanced.
    let counter = Rc::new(Cell::new(0usize));

    async fn handler(counter: Rc<Cell<usize>>) -> Sse {
        let body = stream::iter([event(), event()]).map(move |event| {
            counter.set(counter.get() + 1);
            event
        });

        Sse::new(body)
    }

    let app = test::init_service(App::new().route(
        "/",
        web::get().to({
            let counter = Rc::clone(&counter);
            move || handler(Rc::clone(&counter))
        }),
    ))
    .await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = to_bytes(resp.into_body()).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(
        text.matches("event: datastar-patch-elements").count(),
        2,
        "{text}"
    );

    // The !Send state was advanced twice while the stream was polled, proving
    // the stream really was consumed rather than merely accepted.
    assert_eq!(counter.get(), 2);
}
