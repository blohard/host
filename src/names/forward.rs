//! Recipient resolution uses short-lived answers, separate from display names.
use super::*;

const TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize)]
pub(super) struct Resolved {
    name: String,
    address: Option<Address>,
}

pub(super) fn checked_name(name: &str) -> Result<String> {
    // Bound work before normalization as well as the normalized DNS packet.
    ensure!(name.len() <= MAX_NAME && name.contains('.'), "invalid name");
    let name = NORMALIZER
        .normalize(name)
        .map_err(|_| anyhow!("invalid name"))?;
    ensure!(name.ends_with(".eth"), "invalid name");
    dns(&name)?;
    Ok(name)
}

impl Names {
    pub(super) async fn lookup_name(
        self: &Arc<Self>,
        name: String,
        permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Result<Resolved, &'static str> {
        let service = self.clone();
        self.forward_flights
            .run(name.clone(), || self.forward_cache.get(&name).map(Ok), {
                let name = name.clone();
                async move {
                    service
                        .run_lookup(permit, async {
                            let address = service.forward(&name).await?;
                            let answer = Resolved {
                                name: name.clone(),
                                address,
                            };
                            service.forward_cache.put(name, answer.clone(), TTL);
                            Ok(answer)
                        })
                        .await
                }
            })
            .await
            .map_err(flight_error)?
    }

    async fn forward(&self, name: &str) -> Result<Option<Address>> {
        let data = addrCall {
            node: node(name).into(),
        }
        .abi_encode();
        let bytes = if name.ends_with(".base.eth") {
            let url = self
                .base
                .as_ref()
                .ok_or_else(|| anyhow!("basenames unavailable"))?;
            self.ask("basenames", || async {
                let Some(target) = self.base_resolver(url, name).await? else {
                    return Ok(None);
                };
                absent_if_record_failed(self.call(url, &target, data, CallPolicy::Resolver).await)
            })
            .await?
        } else {
            let url = self
                .ethereum
                .as_ref()
                .ok_or_else(|| anyhow!("ens unavailable"))?;
            self.ask("ens", || async {
                let Some(bytes) = absent_if_record_failed(
                    self.call(
                        url,
                        UNIVERSAL,
                        resolveWithGatewaysCall {
                            name: dns(name)?.into(),
                            data: data.into(),
                            gateways: vec![BATCH_GATEWAY.into()],
                        }
                        .abi_encode(),
                        CallPolicy::Universal,
                    )
                    .await,
                )?
                else {
                    return Ok(None);
                };
                Ok(resolveWithGatewaysCall::abi_decode_returns_validate(&bytes)
                    .ok()
                    .map(|v| v.result.to_vec()))
            })
            .await?
        };
        Ok(bytes
            .and_then(|b| addrCall::abi_decode_returns_validate(&b).ok())
            .filter(|a| !a.is_zero())
            .map(|a| crate::chain::Hex(a.into_array())))
    }
}

pub async fn resolve(
    State(state): State<Arc<AppState>>,
    JsonPath(name): JsonPath<String>,
    permit: Option<Extension<Arc<tokio::sync::OwnedSemaphorePermit>>>,
) -> Response {
    let Ok(name) = checked_name(&name) else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_name");
    };
    let Some(names) = state.gateway.as_ref().and_then(|g| g.names.as_ref()) else {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "name_unavailable");
    };
    match names.lookup_name(name, permit.map(|p| p.0)).await {
        Ok(answer) => json_ok(json!(answer)),
        Err(code) => json_error(StatusCode::SERVICE_UNAVAILABLE, code),
    }
}
