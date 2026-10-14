//! A body coming into the store is noted for the index's sweep to take up
//! first, from an upload, even of bytes already stored, and from promotion.
use super::*;
use crate::chain::Hex;

/// Bytes the store already holds still count when uploaded again: the index
/// waiting on them hears they are here.
#[test]
fn a_body_already_stored_is_noted_when_uploaded_again() {
    let body = record("text/plain", b"stored before its upload");
    with_server(
        "arrival-again",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let store = state.store.as_ref().unwrap();
            store.bodies.put(body.clone()).await.unwrap();
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            assert!(index.arrivals_for_test().is_empty());
            let upload = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(upload.status(), 200);
            assert_eq!(upload.json::<Value>().await.unwrap()["stored"], false);
            assert_eq!(
                index.arrivals_for_test(),
                [Hex(keccak::hash(&[body.as_slice()]).0)]
            );
        },
    );
}

/// A promoted body is noted as soon as it is stored, while its attachment's
/// write still waits.
#[test]
fn a_promoted_body_is_noted_before_its_attachments_are_written() {
    let attachment = record("image/png", b"an attachment written after its body");
    let body = record(
        "text/plain",
        format!("a body with record:{}", hx(&attachment)).as_bytes(),
    );
    with_server(
        "arrival-promoted",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            for bytes in [body.clone(), attachment.clone()] {
                let held = publish(&client, &base, "chain_id=31337&message_id=1", bytes).await;
                assert_eq!(held.status(), 202);
            }
            node.unwrap().node.set_head(16);
            crate::index::follow::once(&state).await;
            let store = state.store.as_ref().unwrap();
            let (attachments, _) = store.attachments.as_ref().unwrap();
            let writes = attachments.hold_writes_for_test().await;
            let promoting = {
                let state = state.clone();
                tokio::spawn(async move { promote_once(&state).await })
            };
            wait_until(
                Duration::from_secs(5),
                "the attachment's write waits",
                || attachments.writer_waiting_for_test(),
            )
            .await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            assert_eq!(
                index.arrivals_for_test(),
                [Hex(keccak::hash(&[body.as_slice()]).0)]
            );
            drop(writes);
            assert_eq!(promoting.await.unwrap(), 2);
        },
    );
}
