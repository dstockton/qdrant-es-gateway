//! Explicit deployment checks and operator commands for coordinated caching.
use super::*;
use crate::{cache_runtime::CacheRuntime, read_cache::ReadCache};
use std::io::Read;

struct Settings {
    url: String,
    catalog: String,
    identity: String,
    serve: bool,
    ttl: u64,
    max_bytes: usize,
}

fn catalog_settings() -> anyhow::Result<(String, String)> {
    let url = env::var("CATALOG_REDIS_URL")
        .map_err(|_| anyhow::anyhow!("read caching requires CATALOG_REDIS_URL"))?;
    let key = env::var("CATALOG_REDIS_KEY").unwrap_or_else(|_| "qeg:{catalog}:v1".into());
    Ok((url, key))
}

fn settings() -> anyhow::Result<Option<Settings>> {
    let identity = match env::var("READ_CACHE_DEPLOYMENT") {
        Ok(identity) if !identity.is_empty() => identity,
        Ok(_) => anyhow::bail!("READ_CACHE_DEPLOYMENT must not be empty"),
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(_) => anyhow::bail!("READ_CACHE_DEPLOYMENT must be valid UTF-8"),
    };
    let (url, catalog) = catalog_settings()?;
    let serve = match env::var("READ_CACHE_SERVE").as_deref() {
        Ok("true" | "1") | Err(env::VarError::NotPresent) => true,
        Ok("false" | "0") => false,
        _ => anyhow::bail!("READ_CACHE_SERVE must be true or false"),
    };
    fn number(name: &str, default: u64) -> anyhow::Result<u64> {
        match env::var(name) {
            Ok(value) => value.parse().map_err(|_| anyhow::anyhow!("invalid {name}")),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(_) => anyhow::bail!("invalid {name}"),
        }
    }
    Ok(Some(Settings {
        url,
        catalog,
        identity,
        serve,
        ttl: number("READ_CACHE_TTL_SECONDS", 60)?,
        max_bytes: usize::try_from(number("READ_CACHE_MAX_BYTES", 1024 * 1024)?)?,
    }))
}

/// Must run before SharedCatalog::from_env can initialize any catalog key.
pub(crate) async fn preflight() -> anyhow::Result<()> {
    if settings()?.is_some() {
        anyhow::ensure!(
            env::var("CATALOG_INITIALIZE").as_deref() != Ok("true"),
            "CATALOG_INITIALIZE is forbidden with cache coordination"
        );
    } else if let Ok(url) = env::var("CATALOG_REDIS_URL") {
        let key = env::var("CATALOG_REDIS_KEY").unwrap_or_else(|_| "qeg:{catalog}:v1".into());
        ReadCache::reject_unconfigured(&url, &key).await?;
    }
    Ok(())
}

fn validate_config(cfg: &Config) -> anyhow::Result<()> {
    anyhow::ensure!(
        !cfg.async_payload_writes && !cfg.async_search_projection,
        "cache coordination requires synchronous payload and search projection writes"
    );
    anyhow::ensure!(
        cfg.qdrant_replication_factor == Some(1),
        "cache coordination requires QDRANT_REPLICATION_FACTOR=1 for future collections"
    );
    anyhow::ensure!(
        env::var("CATALOG_INITIALIZE").as_deref() != Ok("true"),
        "CATALOG_INITIALIZE is forbidden with cache coordination"
    );
    Ok(())
}

fn validate_cluster(value: &Value) -> anyhow::Result<()> {
    anyhow::ensure!(value.pointer("/result/status").and_then(Value::as_str) == Some("disabled"),
        "cache coordination currently requires a standalone Qdrant node with distributed mode disabled");
    Ok(())
}

fn validate_collection(value: &Value) -> anyhow::Result<()> {
    anyhow::ensure!(
        value
            .pointer("/result/config/params/replication_factor")
            .and_then(Value::as_u64)
            == Some(1),
        "cache coordination requires every existing Qdrant collection to have replication_factor=1"
    );
    Ok(())
}

async fn validate_backend(qdrant: &Qdrant) -> anyhow::Result<()> {
    validate_cluster(&qdrant.request(Method::GET, "/cluster", None).await?)?;
    let collections = qdrant.request(Method::GET, "/collections", None).await?;
    let collections = collections
        .pointer("/result/collections")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("cannot verify Qdrant collection topology"))?;
    for collection in collections {
        let name = collection
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("invalid Qdrant collection listing"))?;
        let encoded =
            percent_encoding::utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC);
        validate_collection(
            &qdrant
                .request(Method::GET, &format!("/collections/{encoded}"), None)
                .await?,
        )?;
    }
    Ok(())
}

// Binary identity prevents old responses surviving a semantic code change.
// Configuration bytes (including the credential) are hashed, never logged or
// stored as plaintext in Valkey. A credential change requires maintenance too.
fn semantic_identity(cfg: &Config) -> anyhow::Result<Vec<u8>> {
    let mut file = std::fs::File::open(std::env::current_exe()?)?;
    let mut digest = Sha256::new();
    let mut chunk = [0; 64 * 1024];
    loop {
        let count = file.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        digest.update(&chunk[..count]);
    }
    digest.update(serde_json::to_vec(&json!({
        "response_format":1,"qdrant":cfg.qdrant_url,"credential":cfg.qdrant_api_key,
        "compat":cfg.compat_version,"body":cfg.max_body_bytes,"bulk":cfg.max_bulk_bytes,
        "page":cfg.max_page_size,"projection":cfg.document_projection,
        "async_payload":cfg.async_payload_writes,"async_search":cfg.async_search_projection,
        "replication":cfg.qdrant_replication_factor,
    }))?);
    Ok(digest.finalize().to_vec())
}

async fn connect(cfg: &Config, settings: &Settings) -> anyhow::Result<ReadCache> {
    Ok(ReadCache::connect(
        &settings.url,
        &settings.catalog,
        &settings.identity,
        &semantic_identity(cfg)?,
        settings.ttl,
        settings.max_bytes,
    )
    .await?)
}

pub(crate) async fn activate(
    cfg: &Config,
    qdrant: &Qdrant,
    catalog: Option<&shared_catalog::SharedCatalog>,
) -> anyhow::Result<Option<CacheRuntime>> {
    let Some(settings) = settings()? else {
        return Ok(None);
    };
    validate_config(cfg)?;
    anyhow::ensure!(
        catalog.is_some(),
        "cache coordination requires the shared catalog"
    );
    validate_backend(qdrant).await?;
    let cache = connect(cfg, &settings).await?;
    cache.validate_durability().await?;
    cache.status().await?;
    Ok(Some(CacheRuntime {
        cache,
        serve_reads: settings.serve,
        max_entry_bytes: settings.max_bytes,
    }))
}

/// Operator assertions are explicit: software cannot prove external writers
/// stopped or unresolved Qdrant operations were reconciled. Never infer either
/// from a timeout. Recovery always changes deployment identity.
pub(crate) async fn command(cfg: &Config, qdrant: &Qdrant) -> anyhow::Result<bool> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        return Ok(false);
    }
    anyhow::ensure!(
        args.first().map(String::as_str) == Some("cache"),
        "unknown command"
    );
    let settings = settings()?
        .ok_or_else(|| anyhow::anyhow!("cache commands require READ_CACHE_DEPLOYMENT"))?;
    let cache = connect(cfg, &settings).await?;
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["cache", "status"] => {
            let status = cache.status().await?;
            println!("{}", json!({"deployment":status.deployment,"epoch":status.epoch,"state":status.state,"writer_tokens":status.writer_tokens}));
        }
        ["cache", "bootstrap", "--quiesced"] => {
            validate_config(cfg)?;
            validate_backend(qdrant).await?;
            cache.bootstrap(true).await?;
            println!("{}", json!({"deployment":settings.identity,"state":"bootstrapped"}));
        }
        ["cache", "maintenance"] => {
            cache.validate_durability().await?;
            let _token = cache.maintenance().await?;
            println!("{}", json!({"deployment":settings.identity,"state":"fenced","recovery_required":true}));
        }
        ["cache", "recover", "--new-identity", identity, "--reconciled"] => {
            validate_config(cfg)?;
            validate_backend(qdrant).await?;
            cache.recover(identity, true).await?;
            println!("{}", json!({"deployment":identity,"state":"recovered","restart_all_replicas":true}));
        }
        _ => anyhow::bail!("usage: cache status | bootstrap --quiesced | maintenance | recover --new-identity ID --reconciled"),
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn future_collections_and_both_async_modes_are_checked() {
        let mut cfg = Config::from_env();
        cfg.qdrant_replication_factor = Some(1);
        cfg.async_payload_writes = false;
        cfg.async_search_projection = false;
        assert!(validate_config(&cfg).is_ok());
        cfg.qdrant_replication_factor = None;
        assert!(validate_config(&cfg).is_err());
        cfg.qdrant_replication_factor = Some(1);
        cfg.async_payload_writes = true;
        assert!(validate_config(&cfg).is_err());
        cfg.async_payload_writes = false;
        cfg.async_search_projection = true;
        assert!(validate_config(&cfg).is_err());
    }

    #[test]
    fn topology_must_be_explicit_and_standalone() {
        assert!(validate_cluster(&json!({"result":{"status":"disabled"}})).is_ok());
        for value in [json!({}), json!({"result":{"status":"enabled"}})] {
            assert!(validate_cluster(&value).is_err());
        }
        assert!(validate_collection(
            &json!({"result":{"config":{"params":{"replication_factor":1}}}})
        )
        .is_ok());
        for factor in [json!(null), json!(0), json!(2), json!("1")] {
            assert!(validate_collection(
                &json!({"result":{"config":{"params":{"replication_factor":factor}}}})
            )
            .is_err());
        }
    }
}
