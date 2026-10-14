//! Malformed path segments answer in the JSON envelope like every other
//! refusal, not in the framework's plain text.

use super::*;

#[test]
fn a_malformed_path_segment_answers_json() {
    with_server(
        "json-paths",
        None,
        |_| Setup::default(),
        |Server { base, client, .. }| async move {
            for path in [
                "/api/chains/abc/message/1",
                "/api/chains/31337/message/notanumber",
                "/api/chains/31337/transaction/0xnothex",
                "/api/names/not-an-address",
            ] {
                let response = client.get(format!("{base}{path}")).send().await.unwrap();
                assert_eq!(response.status(), 400, "{path}");
                let body: Value = response.json().await.expect(path);
                assert!(body["error"].is_string(), "{path}: {body}");
            }
        },
    );
}
