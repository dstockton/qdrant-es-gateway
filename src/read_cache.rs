//! Fail-closed, shared read-cache coordination. Runtime never creates control state.
//!
//! The caller must validate deployment durability and fence every writer before
//! enabling this module. A writer token deliberately has no Drop cleanup or TTL:
//! cancellation and uncertain backend completion require explicit recovery.
use redis::aio::MultiplexedConnection;
use sha2::{Digest, Sha256};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TTL_SECONDS: u64 = 3600;
const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
const SCHEMA: &str = "1";

#[derive(Debug, thiserror::Error)]
pub(crate) enum CacheError {
    #[error("invalid read cache configuration")]
    Configuration,
    #[error("read cache coordination unavailable or requires recovery")]
    Unavailable,
}

#[derive(Clone)]
pub(crate) struct ReadCache {
    connection: MultiplexedConnection,
    catalog: String,
    control: String,
    admin: String,
    entries: String,
    deployment: String,
    semantic: String,
    ttl_seconds: u64,
    max_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ReadTicket {
    epoch: String,
    deployment: String,
    semantic: String,
}

#[derive(Debug)]
pub(crate) struct WriterToken {
    id: String,
    deployment: String,
    epoch: String,
}

impl WriterToken {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug)]
pub(crate) struct Lookup {
    pub(crate) ticket: Option<ReadTicket>,
    /// Opaque bytes of the caller's versioned response envelope. The caller must
    /// validate its status, headers and body before serving it.
    pub(crate) response: Option<Vec<u8>>,
    /// Authoritative catalog captured atomically with a quiet miss ticket.
    pub(crate) catalog_snapshot: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct ControlStatus {
    pub(crate) deployment: String,
    pub(crate) epoch: String,
    pub(crate) state: String,
    pub(crate) writer_tokens: Vec<String>,
}

// All relevant keys derive from one catalog namespace and share its explicit
// Redis hash tag. Epochs never pass through Lua floating-point conversion.
const VALIDATE: &str = r#"
local function fail() return redis.error_reply('cache state requires recovery') end
local function typ(k) return redis.call('TYPE', k).ok end
local function decimal(s)
    return s and (s == '0' or string.match(s, '^[1-9][0-9]*$'))
        and (#s < 19 or (#s == 19 and s <= '9223372036854775807'))
end
local function le(a,b) return #a < #b or (#a == #b and a <= b) end
if typ(KEYS[1]) ~= 'hash' or redis.call('PTTL',KEYS[1]) ~= -1 then return fail() end
if typ(KEYS[2]) ~= 'string' or redis.call('PTTL',KEYS[2]) ~= -1 then return fail() end
local fields = redis.call('HMGET',KEYS[1],'schema','deployment','epoch','state')
if fields[1] ~= ARGV[1] or fields[2] ~= ARGV[2]
    or not decimal(fields[3]) or (fields[4] ~= 'active' and not (allow_maintenance and fields[4] == 'maintenance')) then return fail() end
local epoch = fields[3]
local count = redis.call('HLEN',KEYS[1])
if count < 4 then return fail() end
local function token_fields()
    local result = {}
    if count == 4 then return result end
    local all = redis.call('HGETALL',KEYS[1])
    for i=1,#all,2 do
        local k,v = all[i],all[i+1]
        if k ~= 'schema' and k ~= 'deployment' and k ~= 'epoch' and k ~= 'state' then
            local id = string.match(k,'^writer:([0-9a-f]+)$')
            if not id or #id ~= 32 or not decimal(v) or not le(v,epoch) then return nil end
            table.insert(result,id)
        end
    end
    return result
end
"#;

const LOOKUP: &str = r#"
if count ~= 4 or redis.call('EXISTS',KEYS[3]) ~= 0 then return {'busy'} end
local function miss()
    if redis.call('STRLEN',KEYS[2]) > tonumber(ARGV[5]) then return fail() end
    return {'miss',epoch,redis.call('GET',KEYS[2])}
end
local header = ARGV[2] .. '\n' .. ARGV[3] .. '\n' .. epoch .. '\n'
local t = typ(KEYS[4])
if t == 'none' then return miss() end
-- Wrong-type/oversized/immortal/corrupt entries are misses, never control errors.
if t ~= 'string' or redis.call('PTTL',KEYS[4]) < 0
    or redis.call('STRLEN',KEYS[4]) > tonumber(ARGV[4]) + #header then return miss() end
local value = redis.call('GET',KEYS[4])
if string.sub(value,1,#header) ~= header then return miss() end
local response = string.sub(value,#header+1)
if #response == 0 then return miss() end
return {'hit',epoch,response}
"#;

const PUBLISH: &str = r#"
if count ~= 4 or redis.call('EXISTS',KEYS[3]) ~= 0 or epoch ~= ARGV[4] then return {'skip'} end
if #ARGV[5] == 0 or #ARGV[5] > tonumber(ARGV[6]) then return {'skip'} end
local value = ARGV[2] .. '\n' .. ARGV[3] .. '\n' .. epoch .. '\n' .. ARGV[5]
-- Older Redis uses verbatim scripts unless effects replication is enabled.
-- On current Valkey this compatibility call is a successful no-op.
if not redis.replicate_commands() then return fail() end
-- Only the disposable response write skips incremental AOF/replica propagation.
-- Rewrites may still snapshot it; the durable generation always gates reuse.
redis.set_repl(redis.REPL_NONE)
redis.call('SET',KEYS[4],value,'EX',ARGV[7])
redis.set_repl(redis.REPL_ALL)
return {'ok'}
"#;

const ADMIT: &str = r#"
if redis.call('EXISTS',KEYS[3]) ~= 0 or not token_fields() then return fail() end
if epoch == '9223372036854775807' then return fail() end
local token = ARGV[3]
if #token ~= 32 or not string.match(token,'^[0-9a-f]+$') then return fail() end
if redis.call('HEXISTS',KEYS[1],'writer:' .. token) ~= 0 then return fail() end
local chars = {}
local carry = 1
for i=#epoch,1,-1 do
    local digit = string.byte(epoch,i)-48+carry
    if digit == 10 then digit=0; carry=1 else carry=0 end
    table.insert(chars,1,string.char(48+digit))
end
if carry == 1 then table.insert(chars,1,'1') end
local next_epoch = table.concat(chars)
-- One command changes epoch and records ownership together. No second command
-- can fail after an epoch advance but before the persistent token is recorded.
local next_state = ARGV[4] or 'active'
if next_state ~= 'active' and next_state ~= 'maintenance' then return fail() end
redis.call('HSET',KEYS[1],'epoch',next_epoch,'writer:' .. token,next_epoch,'state',next_state)
return {'ok',next_epoch}
"#;

const RELEASE: &str = r#"
-- Another admitted administrative writer may own the catalog claim. Its token
-- still fences reads; releasing this completed writer must not strand it.
if not token_fields() then return fail() end
if redis.call('HGET',KEYS[1],'writer:' .. ARGV[3]) ~= ARGV[4] then return fail() end
redis.call('HDEL',KEYS[1],'writer:' .. ARGV[3])
return {'ok'}
"#;

const STATUS: &str = r#"
local tokens = token_fields()
if not tokens then return fail() end
local result = {'ok',epoch,fields[4]}
for _,id in ipairs(tokens) do table.insert(result,id) end
return result
"#;

fn hash(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn valid_digest(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn valid_identity(identity: &str) -> bool {
    !identity.is_empty()
        && identity.len() <= 128
        && identity
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn catalog_has_tag(key: &str) -> bool {
    key.find('{')
        .and_then(|start| key[start + 1..].find('}').map(|end| end > 0))
        == Some(true)
}

impl ReadCache {
    /// Connect only. Startup must separately validate the deployment and call
    /// status; this does not initialize or repair missing state.
    pub(crate) async fn connect(
        url: &str,
        catalog_key: &str,
        deployment: &str,
        semantic_identity: &[u8],
        ttl_seconds: u64,
        max_bytes: usize,
    ) -> Result<Self, CacheError> {
        if !catalog_has_tag(catalog_key)
            || !valid_identity(deployment)
            || semantic_identity.is_empty()
            || !(1..=MAX_TTL_SECONDS).contains(&ttl_seconds)
            || !(1..=MAX_ENTRY_BYTES).contains(&max_bytes)
        {
            return Err(CacheError::Configuration);
        }
        let client = redis::Client::open(url).map_err(|_| CacheError::Configuration)?;
        let connection = tokio::time::timeout(TIMEOUT, client.get_multiplexed_async_connection())
            .await
            .map_err(|_| CacheError::Unavailable)?
            .map_err(|_| CacheError::Unavailable)?;
        let semantic = hash(&[semantic_identity]);
        Ok(Self {
            connection,
            catalog: catalog_key.to_owned(),
            control: format!("{catalog_key}:read-cache:control"),
            admin: format!("{catalog_key}:admin"),
            entries: format!("{catalog_key}:read-cache:entry:{deployment}:{semantic}:"),
            deployment: deployment.to_owned(),
            semantic,
            ttl_seconds,
            max_bytes,
        })
    }

    pub(crate) fn request_key(&self, method: &str, uri: &str, body: &[u8]) -> String {
        hash(&[
            self.semantic.as_bytes(),
            method.as_bytes(),
            uri.as_bytes(),
            body,
        ])
    }

    async fn run(
        &self,
        operation: &str,
        key: Option<&str>,
        args: &[&[u8]],
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        if key.is_some_and(|k| !valid_digest(k)) {
            return Err(CacheError::Configuration);
        }
        let allow_maintenance = operation == STATUS || operation == RELEASE;
        let script = redis::Script::new(&format!(
            "local allow_maintenance = {allow_maintenance}\n{VALIDATE}\n{operation}"
        ));
        let mut invocation = script.prepare_invoke();
        invocation
            .key(&self.control)
            .key(&self.catalog)
            .key(&self.admin);
        if let Some(key) = key {
            invocation.key(format!("{}{key}", self.entries));
        }
        invocation.arg(SCHEMA).arg(&self.deployment);
        for arg in args {
            invocation.arg(*arg);
        }
        let mut connection = self.connection.clone();
        tokio::time::timeout(TIMEOUT, invocation.invoke_async(&mut connection))
            .await
            .map_err(|_| CacheError::Unavailable)?
            .map_err(|_| CacheError::Unavailable)
    }

    pub(crate) async fn lookup(&self, key: &str) -> Result<Lookup, CacheError> {
        let max = self.max_bytes.to_string();
        let catalog_max = crate::shared_catalog::MAX_BYTES.to_string();
        let result = self
            .run(
                LOOKUP,
                Some(key),
                &[
                    self.semantic.as_bytes(),
                    max.as_bytes(),
                    catalog_max.as_bytes(),
                ],
            )
            .await?;
        self.parse_lookup(result)
    }

    fn parse_lookup(&self, result: Vec<Vec<u8>>) -> Result<Lookup, CacheError> {
        match result.as_slice() {
            [tag] if tag == b"busy" => Ok(Lookup {
                ticket: None,
                response: None,
                catalog_snapshot: None,
            }),
            [tag, epoch, value] if tag == b"miss" || tag == b"hit" => {
                let epoch =
                    String::from_utf8(epoch.clone()).map_err(|_| CacheError::Unavailable)?;
                // Protocol tags are distinct: catalog bytes must never be decoded
                // as a cached response, nor a hit envelope used as a catalog.
                Ok(Lookup {
                    ticket: Some(ReadTicket {
                        epoch,
                        deployment: self.deployment.clone(),
                        semantic: self.semantic.clone(),
                    }),
                    response: (tag == b"hit").then(|| value.clone()),
                    catalog_snapshot: (tag == b"miss").then(|| value.clone()),
                })
            }
            _ => Err(CacheError::Unavailable),
        }
    }

    /// Publication failure must not replace the ordinary handler's response.
    /// The wrapper may ignore Err here, but must never ignore admission errors.
    pub(crate) async fn publish(
        &self,
        ticket: &ReadTicket,
        key: &str,
        response: &[u8],
    ) -> Result<bool, CacheError> {
        if ticket.deployment != self.deployment || ticket.semantic != self.semantic {
            return Err(CacheError::Configuration);
        }
        if response.is_empty() || response.len() > self.max_bytes {
            return Ok(false);
        }
        let max = self.max_bytes.to_string();
        let ttl = self.ttl_seconds.to_string();
        let result = self
            .run(
                PUBLISH,
                Some(key),
                &[
                    self.semantic.as_bytes(),
                    ticket.epoch.as_bytes(),
                    response,
                    max.as_bytes(),
                    ttl.as_bytes(),
                ],
            )
            .await?;
        match result.as_slice() {
            [value] if value == b"ok" => Ok(true),
            [value] if value == b"skip" => Ok(false),
            _ => Err(CacheError::Unavailable),
        }
    }

    pub(crate) async fn admit_writer(&self) -> Result<WriterToken, CacheError> {
        self.admit_state("active").await
    }

    async fn admit_state(&self, state: &str) -> Result<WriterToken, CacheError> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let result = self
            .run(ADMIT, None, &[id.as_bytes(), state.as_bytes()])
            .await?;
        match result.as_slice() {
            [ok, epoch] if ok == b"ok" => Ok(WriterToken {
                id,
                deployment: self.deployment.clone(),
                epoch: String::from_utf8(epoch.clone()).map_err(|_| CacheError::Unavailable)?,
            }),
            _ => Err(CacheError::Unavailable),
        }
    }

    /// Only call after every backend attempt and catalog publication is known
    /// complete. No timeout, Drop, restart, or blind retry may release a token.
    pub(crate) async fn release_writer(&self, token: &WriterToken) -> Result<(), CacheError> {
        if token.deployment != self.deployment {
            return Err(CacheError::Configuration);
        }
        let result = self
            .run(
                RELEASE,
                None,
                &[token.id.as_bytes(), token.epoch.as_bytes()],
            )
            .await?;
        match result.as_slice() {
            [value] if value == b"ok" => Ok(()),
            _ => Err(CacheError::Unavailable),
        }
    }

    /// Fail closed before catalog initialization, including malformed control keys.
    pub(crate) async fn reject_unconfigured(
        url: &str,
        catalog_key: &str,
    ) -> Result<(), CacheError> {
        let client = redis::Client::open(url).map_err(|_| CacheError::Configuration)?;
        tokio::time::timeout(TIMEOUT, async {
            let mut connection = client
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| CacheError::Unavailable)?;
            // Cache-off legacy catalogs need no hash tag. Activation still does.
            for suffix in ["control", "initialized"] {
                let exists: bool = redis::cmd("EXISTS")
                    .arg(format!("{catalog_key}:read-cache:{suffix}"))
                    .query_async(&mut connection)
                    .await
                    .map_err(|_| CacheError::Unavailable)?;
                if exists {
                    return Err(CacheError::Unavailable);
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| CacheError::Unavailable)?
    }

    /// Inspect the actual server, not requested application settings. Filesystem
    /// durability, exclusive ownership and no restore/failover remain deployment contracts.
    pub(crate) async fn validate_durability(&self) -> Result<(), CacheError> {
        let mut connection = self.connection.clone();
        let (config, info) = tokio::time::timeout(TIMEOUT, async {
            let config: std::collections::HashMap<String, String> = redis::cmd("CONFIG")
                .arg("GET")
                .arg(&[
                    "appendonly",
                    "appendfsync",
                    "no-appendfsync-on-rewrite",
                    "maxmemory",
                    "maxmemory-policy",
                    "cluster-enabled",
                ])
                .query_async(&mut connection)
                .await
                .map_err(|_| CacheError::Unavailable)?;
            let info: String = redis::cmd("INFO")
                .arg("ALL")
                .query_async(&mut connection)
                .await
                .map_err(|_| CacheError::Unavailable)?;
            Ok::<_, CacheError>((config, info))
        })
        .await
        .map_err(|_| CacheError::Unavailable)??;
        validate_server(&config, &info)
    }

    async fn validated_catalog(&self) -> Result<Vec<u8>, CacheError> {
        let mut connection = self.connection.clone();
        let bytes: Vec<u8> = tokio::time::timeout(
            TIMEOUT,
            redis::Script::new("if redis.call('TYPE',KEYS[1]).ok ~= 'string' or redis.call('PTTL',KEYS[1]) ~= -1 or redis.call('STRLEN',KEYS[1]) > 8388608 then return redis.error_reply('invalid catalog') end; return redis.call('GET',KEYS[1])")
                .key(&self.catalog)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| CacheError::Unavailable)?
        .map_err(|_| CacheError::Unavailable)?;
        validate_snapshot(&bytes)?;
        Ok(bytes)
    }

    /// Explicit administrative operation only, after stopping all old/external writers.
    pub(crate) async fn bootstrap(&self, operator_quiesced: bool) -> Result<(), CacheError> {
        if !operator_quiesced {
            return Err(CacheError::Configuration);
        }
        self.validate_durability().await?;
        let catalog = self.validated_catalog().await?;
        self.administration(BOOTSTRAP, &self.deployment, &catalog)
            .await
    }

    /// Persistent maintenance fence. Stopping/restarting this process never clears it.
    pub(crate) async fn maintenance(&self) -> Result<WriterToken, CacheError> {
        self.validate_durability().await?;
        self.admit_state("maintenance").await
    }

    /// This flag is an explicit operator assertion, not automated reconciliation.
    /// Every writer must be stopped and ambiguous backend operations accounted for.
    pub(crate) async fn recover(
        &self,
        new_identity: &str,
        operator_reconciled: bool,
    ) -> Result<(), CacheError> {
        if !operator_reconciled || !valid_identity(new_identity) || new_identity == self.deployment
        {
            return Err(CacheError::Configuration);
        }
        self.validate_durability().await?;
        let catalog = self.validated_catalog().await?;
        self.administration(RECOVER, new_identity, &catalog).await
    }

    async fn administration(
        &self,
        operation: &str,
        identity: &str,
        catalog: &[u8],
    ) -> Result<(), CacheError> {
        let mut connection = self.connection.clone();
        let script = redis::Script::new(&format!("{ADMIN_VALIDATE}\n{operation}"));
        let result: i32 = tokio::time::timeout(
            TIMEOUT,
            script
                .key(&self.control)
                .key(&self.catalog)
                .key(&self.admin)
                .key(format!("{}:read-cache:identity:{identity}", self.catalog))
                .key(format!(
                    "{}:read-cache:identity:{}",
                    self.catalog, self.deployment
                ))
                .key(format!("{}:read-cache:initialized", self.catalog))
                .arg(SCHEMA)
                .arg(&self.deployment)
                .arg(identity)
                .arg(catalog)
                .invoke_async(&mut connection),
        )
        .await
        .map_err(|_| CacheError::Unavailable)?
        .map_err(|_| CacheError::Unavailable)?;
        if result == 1 {
            Ok(())
        } else {
            Err(CacheError::Unavailable)
        }
    }

    pub(crate) async fn status(&self) -> Result<ControlStatus, CacheError> {
        let result = self.run(STATUS, None, &[]).await?;
        if result.len() < 3 || result[0] != b"ok" {
            return Err(CacheError::Unavailable);
        }
        let text = |v: &[u8]| String::from_utf8(v.to_vec()).map_err(|_| CacheError::Unavailable);
        Ok(ControlStatus {
            deployment: self.deployment.clone(),
            epoch: text(&result[1])?,
            state: text(&result[2])?,
            writer_tokens: result[3..]
                .iter()
                .map(|v| text(v))
                .collect::<Result<_, _>>()?,
        })
    }
}

const ADMIN_VALIDATE: &str = r#"
local function fail() return redis.error_reply('cache administration requires reconciliation') end
local function typ(k) return redis.call('TYPE',k).ok end
if typ(KEYS[2]) ~= 'string' or redis.call('PTTL',KEYS[2]) ~= -1
    or redis.call('GET',KEYS[2]) ~= ARGV[4] or redis.call('EXISTS',KEYS[3]) ~= 0 then return fail() end
-- An identity is never reusable, even after a successful recovery. These small
-- administrative tombstones have no TTL and share the durable catalog namespace.
if redis.call('EXISTS',KEYS[4]) ~= 0 then return fail() end
"#;

const BOOTSTRAP: &str = r#"
if redis.call('EXISTS',KEYS[1]) ~= 0 or redis.call('EXISTS',KEYS[6]) ~= 0 then return fail() end
-- One atomic command records the identity and lineage. If control allocation
-- fails afterward, only explicit reconciliation can finish this deployment.
redis.call('MSET',KEYS[4],'1',KEYS[6],ARGV[3])
redis.call('HSET',KEYS[1],'schema',ARGV[1],'deployment',ARGV[3],'epoch','0','state','active')
return 1
"#;

const RECOVER: &str = r#"
if typ(KEYS[6]) ~= 'string' or redis.call('GET',KEYS[6]) ~= ARGV[2]
    or redis.call('PTTL',KEYS[6]) ~= -1
    or typ(KEYS[5]) ~= 'string' or redis.call('GET',KEYS[5]) ~= '1'
    or redis.call('PTTL',KEYS[5]) ~= -1 then return fail() end
-- Reconciliation is explicit and all writers must be stopped. Missing/corrupt
-- control can be replaced using the independently persisted lineage identity.
-- Fence BEFORE changing lineage: Lua errors do not roll previous writes back.
if typ(KEYS[1]) ~= 'none' and typ(KEYS[1]) ~= 'hash' then redis.call('DEL',KEYS[1]) end
redis.call('HSET',KEYS[1],'schema',ARGV[1],'deployment',ARGV[2],'epoch','0','state','maintenance')
redis.call('PERSIST',KEYS[1])
redis.call('MSET',KEYS[4],'1',KEYS[6],ARGV[3])
redis.call('HSET',KEYS[1],'deployment',ARGV[3],'state','active')
local fields = redis.call('HKEYS',KEYS[1])
for _,field in ipairs(fields) do
    if field ~= 'schema' and field ~= 'deployment' and field ~= 'epoch' and field ~= 'state' then
        redis.call('HDEL',KEYS[1],field)
    end
end
return 1
"#;

fn validate_server(
    config: &std::collections::HashMap<String, String>,
    info: &str,
) -> Result<(), CacheError> {
    let expected = [
        ("appendonly", "yes"),
        ("appendfsync", "always"),
        ("no-appendfsync-on-rewrite", "no"),
        ("cluster-enabled", "no"),
    ];
    if expected
        .iter()
        .any(|(k, v)| config.get(*k).map(String::as_str) != Some(*v))
        || config
            .get("maxmemory")
            .and_then(|v| v.parse::<u64>().ok())
            .is_none_or(|v| v == 0)
        || !config.get("maxmemory-policy").is_some_and(|v| {
            matches!(
                v.as_str(),
                "noeviction" | "volatile-lru" | "volatile-lfu" | "volatile-random" | "volatile-ttl"
            )
        })
    {
        return Err(CacheError::Configuration);
    }
    let fields: std::collections::HashMap<&str, &str> = info
        .lines()
        .filter_map(|l| l.trim().split_once(':'))
        .collect();
    let modes = [
        fields.get("redis_mode").copied(),
        fields.get("server_mode").copied(),
    ];
    if modes.iter().all(Option::is_none) || modes.iter().flatten().any(|mode| *mode != "standalone")
    {
        return Err(CacheError::Configuration);
    }
    for (key, value) in [
        ("role", "master"),
        ("connected_slaves", "0"),
        ("aof_enabled", "1"),
        ("aof_last_write_status", "ok"),
        ("aof_last_bgrewrite_status", "ok"),
        ("aof_rewrite_in_progress", "0"),
        ("aof_rewrite_scheduled", "0"),
        ("loading", "0"),
    ] {
        if fields.get(key).copied() != Some(value) {
            return Err(CacheError::Configuration);
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheCatalogSnapshot {
    indices: Vec<(String, String, String)>,
    aliases: Vec<(String, String)>,
}

fn validate_snapshot(bytes: &[u8]) -> Result<(), CacheError> {
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(CacheError::Unavailable);
    }
    let snapshot: CacheCatalogSnapshot =
        serde_json::from_slice(bytes).map_err(|_| CacheError::Unavailable)?;
    let mut names = std::collections::HashSet::new();
    for (name, mapping, vectors) in &snapshot.indices {
        if !names.insert(name) {
            return Err(CacheError::Unavailable);
        }
        serde_json::from_str::<serde_json::Value>(mapping).map_err(|_| CacheError::Unavailable)?;
        serde_json::from_str::<Vec<String>>(vectors).map_err(|_| CacheError::Unavailable)?;
    }
    let mut aliases = std::collections::HashSet::new();
    for (alias, _) in &snapshot.aliases {
        if !aliases.insert(alias) {
            return Err(CacheError::Unavailable);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> ReadCache {
        let url = std::env::var("CATALOG_TEST_REDIS_URL").expect("isolated Valkey required");
        let key = format!("qeg:{{cache-test-{}}}:v1", uuid::Uuid::new_v4());
        let cache = ReadCache::connect(&url, &key, "deployment-a", b"build-and-config", 60, 1024)
            .await
            .unwrap();
        assert!(cache.status().await.is_err(), "runtime cannot bootstrap");
        let mut c = cache.connection.clone();
        redis::cmd("SET")
            .arg(&cache.catalog)
            .arg(r#"{"indices":[],"aliases":[]}"#)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        reset_control(&cache, "0").await;
        let status = cache.status().await.unwrap();
        assert_eq!(status.deployment, "deployment-a");
        assert_eq!(status.epoch, "0");
        assert_eq!(status.state, "active");
        assert!(status.writer_tokens.is_empty());
        cache
    }

    async fn reset_control(cache: &ReadCache, epoch: &str) {
        let mut c = cache.connection.clone();
        redis::cmd("DEL")
            .arg(&cache.control)
            .arg(&cache.admin)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        redis::cmd("HSET")
            .arg(&cache.control)
            .arg("schema")
            .arg(SCHEMA)
            .arg("deployment")
            .arg(&cache.deployment)
            .arg("epoch")
            .arg(epoch)
            .arg("state")
            .arg("active")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
    }

    async fn cleanup(cache: &ReadCache) {
        let mut c = cache.connection.clone();
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{}*", cache.catalog))
            .query_async(&mut c)
            .await
            .unwrap();
        if !keys.is_empty() {
            redis::cmd("DEL")
                .arg(keys)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
        }
    }

    #[test]
    fn request_identity_is_length_delimited_and_configuration_scoped() {
        assert_ne!(hash(&[b"ab", b"c"]), hash(&[b"a", b"bc"]));
        assert_ne!(
            hash(&[b"GET", b"/x?y=1", b""]),
            hash(&[b"GET", b"/x?y=2", b""])
        );
        assert_ne!(
            hash(&[b"build-a", b"GET", b"/x"]),
            hash(&[b"build-b", b"GET", b"/x"])
        );
        assert!(valid_digest(&hash(&[b"x"])));
        assert!(!valid_digest(&"A".repeat(64)));
        assert!(!catalog_has_tag("catalog"));
        assert!(!catalog_has_tag("qeg:{}:{later}"));
        assert!(catalog_has_tag("qeg:{catalog}:v1"));
        assert!(!valid_identity("bad\nidentity"));
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cache_publication_failure_leaves_coordination_and_later_writes_intact() {
        let cache = fixture().await;
        let key = cache.request_key("GET", "/items/_doc/1", b"");
        let entry = format!("{}{key}", cache.entries);
        let mut connection = cache.connection.clone();
        let catalog_before: Vec<u8> = redis::cmd("GET")
            .arg(&cache.catalog)
            .query_async(&mut connection)
            .await
            .unwrap();
        for (next_epoch, invalid_ttl) in [("1", true), ("2", false)] {
            let ticket = cache.lookup(&key).await.unwrap().ticket.unwrap();
            let control_before: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
                .arg(&cache.control)
                .query_async(&mut connection)
                .await
                .unwrap();
            let mut publisher = cache.clone();
            if invalid_ttl {
                // Bypass configuration validation only in this test to make
                // SET fail after REPL_NONE, before the explicit restore.
                publisher.ttl_seconds = 0;
                assert!(publisher.publish(&ticket, &key, b"rejected").await.is_err());
                let exists: bool = redis::cmd("EXISTS")
                    .arg(&entry)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(!exists);
            } else {
                assert!(publisher.publish(&ticket, &key, b"valid").await.unwrap());
                assert_eq!(
                    cache.lookup(&key).await.unwrap().response.as_deref(),
                    Some(b"valid".as_slice())
                );
                let ttl: i64 = redis::cmd("PTTL")
                    .arg(&entry)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!((1..=60_000).contains(&ttl));
            }
            let catalog_after: Vec<u8> = redis::cmd("GET")
                .arg(&cache.catalog)
                .query_async(&mut connection)
                .await
                .unwrap();
            let control_after: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
                .arg(&cache.control)
                .query_async(&mut connection)
                .await
                .unwrap();
            assert_eq!(catalog_after, catalog_before);
            assert_eq!(control_after, control_before);

            // The next mutation after either outcome must still advance and
            // fence the shared epoch. Persistence is checked by the AOF test.
            let writer = cache.admit_writer().await.unwrap();
            assert_eq!(writer.epoch, next_epoch);
            assert!(cache.lookup(&key).await.unwrap().ticket.is_none());
            cache.release_writer(&writer).await.unwrap();
            let miss = cache.lookup(&key).await.unwrap();
            assert!(miss.response.is_none());
            assert_eq!(miss.ticket.unwrap().epoch, next_epoch);
        }
        let control_ttl: i64 = redis::cmd("PTTL")
            .arg(&cache.control)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(control_ttl, -1);
        cleanup(&cache).await;
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cache_epoch_fences_racing_reads_and_owned_persistent_writers() {
        let cache = fixture().await;
        for malformed in [
            vec![b"miss".to_vec(), b"0".to_vec()],
            vec![b"hit".to_vec(), b"0".to_vec()],
            vec![b"busy".to_vec(), b"0".to_vec(), b"{}".to_vec()],
            vec![b"other".to_vec(), b"0".to_vec(), b"{}".to_vec()],
            vec![
                b"miss".to_vec(),
                b"0".to_vec(),
                b"{}".to_vec(),
                b"extra".to_vec(),
            ],
        ] {
            assert!(cache.parse_lookup(malformed).is_err());
        }
        let peer = cache.clone();
        let key = cache.request_key("POST", "/items/_search", b"{}");
        let ticket = cache.lookup(&key).await.unwrap().ticket.unwrap();
        assert!(cache.publish(&ticket, &key, b"first").await.unwrap());
        assert_eq!(
            peer.lookup(&key).await.unwrap().response.as_deref(),
            Some(b"first".as_slice())
        );
        let first = cache.admit_writer().await.unwrap();
        let second = peer.admit_writer().await.unwrap();
        assert_eq!(cache.status().await.unwrap().writer_tokens.len(), 2);
        assert!(peer.lookup(&key).await.unwrap().ticket.is_none());
        assert!(!cache.publish(&ticket, &key, b"stale").await.unwrap());
        let forged = WriterToken {
            id: "0".repeat(32),
            deployment: first.deployment.clone(),
            epoch: first.epoch.clone(),
        };
        assert!(cache.release_writer(&forged).await.is_err());
        // An unrelated admin writer's claim must not strand a completed token.
        let mut admin_connection = cache.connection.clone();
        redis::cmd("SET")
            .arg(&cache.admin)
            .arg("second-writer-admin-claim")
            .query_async::<()>(&mut admin_connection)
            .await
            .unwrap();
        cache.release_writer(&first).await.unwrap();
        assert!(peer.lookup(&key).await.unwrap().ticket.is_none());
        redis::cmd("DEL")
            .arg(&cache.admin)
            .query_async::<()>(&mut admin_connection)
            .await
            .unwrap();
        assert!(
            cache.release_writer(&first).await.is_err(),
            "release is owned, not idempotent cleanup"
        );
        assert!(peer.lookup(&key).await.unwrap().response.is_none());
        // Dropping an ownership handle never clears uncertainty.
        let second_id = second.id.clone();
        drop(second);
        assert_eq!(
            cache.status().await.unwrap().writer_tokens,
            vec![second_id.clone()]
        );
        let restarted = ReadCache::connect(
            &std::env::var("CATALOG_TEST_REDIS_URL").unwrap(),
            &cache.catalog,
            "deployment-a",
            b"build-and-config",
            60,
            1024,
        )
        .await
        .unwrap();
        assert_eq!(
            restarted.status().await.unwrap().writer_tokens,
            vec![second_id]
        );
        assert!(restarted.lookup(&key).await.unwrap().ticket.is_none());
        let mut c = cache.connection.clone();
        let ttl: i64 = redis::cmd("PTTL")
            .arg(&cache.control)
            .query_async(&mut c)
            .await
            .unwrap();
        assert_eq!(ttl, -1);
        // Explicit test-only recovery simulates a deliberate identity replacement.
        reset_control(&cache, "3").await;
        assert!(
            !cache.publish(&ticket, &key, b"stale").await.unwrap(),
            "read begun before mutation cannot publish afterward"
        );
        let fresh = cache.lookup(&key).await.unwrap().ticket.unwrap();
        assert!(cache.publish(&fresh, &key, b"fresh").await.unwrap());
        let entry = format!("{}{key}", cache.entries);
        let ttl: i64 = redis::cmd("PTTL")
            .arg(&entry)
            .query_async(&mut c)
            .await
            .unwrap();
        assert!((1..=60_000).contains(&ttl));
        let after = cache.lookup(&key).await.unwrap();
        assert_eq!(after.response.as_deref(), Some(b"fresh".as_slice()));
        let ttl_after: i64 = redis::cmd("PTTL")
            .arg(&entry)
            .query_async(&mut c)
            .await
            .unwrap();
        assert!(ttl_after <= ttl, "hit must not refresh TTL");
        assert!(!cache.publish(&fresh, &key, &[b'x'; 1025]).await.unwrap());
        cleanup(&cache).await;
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cache_rejects_corrupt_control_and_exact_epoch_never_rounds() {
        let cache = fixture().await;
        let key = cache.request_key("GET", "/items/_doc/1", b"");
        let mut c = cache.connection.clone();
        for field in ["schema", "deployment", "epoch", "state"] {
            reset_control(&cache, "0").await;
            redis::cmd("HDEL")
                .arg(&cache.control)
                .arg(field)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
            assert!(cache.lookup(&key).await.is_err());
            assert!(cache.admit_writer().await.is_err());
        }
        for (field, value) in [
            ("schema", "2"),
            ("deployment", "different"),
            ("state", "recovering"),
        ] {
            reset_control(&cache, "0").await;
            redis::cmd("HSET")
                .arg(&cache.control)
                .arg(field)
                .arg(value)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
            assert!(cache.lookup(&key).await.is_err());
            assert!(cache.admit_writer().await.is_err());
        }
        for epoch in [
            "",
            "01",
            "-1",
            "1.0",
            "1e3",
            "9223372036854775808",
            "9007199254740993x",
        ] {
            reset_control(&cache, epoch).await;
            assert!(cache.admit_writer().await.is_err());
            assert!(cache.lookup(&key).await.is_err());
        }
        reset_control(&cache, "9007199254740992").await;
        let token = cache.admit_writer().await.unwrap();
        assert_eq!(token.epoch, "9007199254740993");
        cache.release_writer(&token).await.unwrap();
        assert_eq!(cache.status().await.unwrap().epoch, "9007199254740993");
        reset_control(&cache, "9223372036854775806").await;
        let last = cache.admit_writer().await.unwrap();
        assert_eq!(last.epoch, "9223372036854775807");
        assert!(cache.admit_writer().await.is_err());
        cache.release_writer(&last).await.unwrap();
        reset_control(&cache, "0").await;
        redis::cmd("HSET")
            .arg(&cache.control)
            .arg("writer:bad")
            .arg("0")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.status().await.is_err());
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.lookup(&key).await.unwrap().ticket.is_none());
        reset_control(&cache, "0").await;
        redis::cmd("PEXPIRE")
            .arg(&cache.control)
            .arg(60000)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.lookup(&key).await.is_err());
        reset_control(&cache, "0").await;
        redis::cmd("SET")
            .arg(&cache.control)
            .arg("wrong-type")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.lookup(&key).await.is_err());
        cleanup(&cache).await;
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cache_admin_catalog_and_entry_fail_closed() {
        let cache = fixture().await;
        let key = cache.request_key("POST", "/items/_search", b"{}");
        let mut c = cache.connection.clone();
        let entry = format!("{}{key}", cache.entries);
        let ticket = cache.lookup(&key).await.unwrap().ticket.unwrap();
        redis::cmd("SET")
            .arg(&cache.admin)
            .arg("admin-owner")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.lookup(&key).await.unwrap().ticket.is_none());
        assert!(!cache.publish(&ticket, &key, b"ignored").await.unwrap());
        redis::cmd("DEL")
            .arg(&cache.admin)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        for value in ["wrong-header", "deployment-a\nwrong-semantic\n0\nbody"] {
            redis::cmd("SET")
                .arg(&entry)
                .arg(value)
                .arg("EX")
                .arg(60)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
            assert!(cache.lookup(&key).await.unwrap().response.is_none());
        }
        redis::cmd("DEL")
            .arg(&entry)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        redis::cmd("HSET")
            .arg(&entry)
            .arg("x")
            .arg("y")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.lookup(&key).await.unwrap().response.is_none());
        assert!(
            cache.publish(&ticket, &key, b"valid").await.unwrap(),
            "entry corruption may be replaced"
        );
        redis::cmd("PERSIST")
            .arg(&entry)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(
            cache.lookup(&key).await.unwrap().response.is_none(),
            "immortal entry is invalid"
        );
        redis::cmd("DEL")
            .arg(&cache.catalog)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache.lookup(&key).await.is_err());
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.publish(&ticket, &key, b"bad").await.is_err());
        cleanup(&cache).await;
    }
    #[test]
    fn cache_requires_actual_durable_standalone_configuration() {
        let config = [
            ("appendonly", "yes"),
            ("appendfsync", "always"),
            ("no-appendfsync-on-rewrite", "no"),
            ("cluster-enabled", "no"),
            ("maxmemory", "268435456"),
            ("maxmemory-policy", "noeviction"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<std::collections::HashMap<_, _>>();
        let info = "redis_mode:standalone\nrole:master\nconnected_slaves:0\naof_enabled:1\naof_last_write_status:ok\naof_last_bgrewrite_status:ok\naof_rewrite_in_progress:0\naof_rewrite_scheduled:0\nloading:0\n";
        assert!(validate_server(&config, info).is_ok());
        assert!(validate_server(&config, &info.replace("redis_mode:", "server_mode:")).is_ok());
        assert!(validate_server(&config, &format!("{info}server_mode:standalone\n")).is_ok());
        assert!(validate_server(&config, &format!("{info}server_mode:cluster\n")).is_err());
        assert!(validate_server(&config, &info.replace("redis_mode:standalone\n", "")).is_err());
        for key in config.keys() {
            let mut broken = config.clone();
            broken.remove(key);
            assert!(validate_server(&broken, info).is_err(), "missing {key}");
        }
        for (key, value) in [
            ("appendonly", "no"),
            ("appendfsync", "everysec"),
            ("no-appendfsync-on-rewrite", "yes"),
            ("cluster-enabled", "yes"),
            ("maxmemory", "0"),
            ("maxmemory-policy", "allkeys-lru"),
        ] {
            let mut broken = config.clone();
            broken.insert(key.into(), value.into());
            assert!(validate_server(&broken, info).is_err(), "unsafe {key}");
        }
        for policy in [
            "volatile-lru",
            "volatile-lfu",
            "volatile-random",
            "volatile-ttl",
        ] {
            let mut acceptable = config.clone();
            acceptable.insert("maxmemory-policy".into(), policy.into());
            assert!(validate_server(&acceptable, info).is_ok());
        }
        for (old, new) in [
            ("standalone", "cluster"),
            ("master", "slave"),
            ("connected_slaves:0", "connected_slaves:1"),
            ("aof_enabled:1", "aof_enabled:0"),
            ("aof_last_write_status:ok", "aof_last_write_status:err"),
            (
                "aof_last_bgrewrite_status:ok",
                "aof_last_bgrewrite_status:err",
            ),
            ("aof_rewrite_in_progress:0", "aof_rewrite_in_progress:1"),
            ("aof_rewrite_scheduled:0", "aof_rewrite_scheduled:1"),
            ("aof_rewrite_in_progress:0\n", ""),
            ("aof_rewrite_scheduled:0\n", ""),
            ("loading:0", "loading:1"),
        ] {
            assert!(validate_server(&config, &info.replace(old, new)).is_err());
        }
        assert!(validate_snapshot(br#"{"indices":[],"aliases":[]}"#).is_ok());
        for bytes in [
            br#"{}"#.as_slice(),
            br#"{"indices":{},"aliases":[]}"#,
            br#"{"indices":[],"aliases":[["a","i"],["a","j"]]}"#,
            br#"{"indices":[["i","invalid","[]"]],"aliases":[]}"#,
        ] {
            assert!(validate_snapshot(bytes).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cache_explicit_administration_never_resurrects_identity_or_implicitly_recovers() {
        let cache = fixture().await;
        let url = std::env::var("CATALOG_TEST_REDIS_URL").unwrap();
        let legacy_key = format!("legacy-untagged-{}", uuid::Uuid::new_v4());
        assert!(ReadCache::reject_unconfigured(&url, &legacy_key)
            .await
            .is_ok());
        let mut legacy_connection = cache.connection.clone();
        let legacy_control = format!("{legacy_key}:read-cache:control");
        redis::cmd("SET")
            .arg(&legacy_control)
            .arg("malformed")
            .query_async::<()>(&mut legacy_connection)
            .await
            .unwrap();
        assert!(ReadCache::reject_unconfigured(&url, &legacy_key)
            .await
            .is_err());
        redis::cmd("DEL")
            .arg(&legacy_control)
            .query_async::<()>(&mut legacy_connection)
            .await
            .unwrap();
        assert!(ReadCache::reject_unconfigured(&url, &cache.catalog)
            .await
            .is_err());
        assert!(cache.bootstrap(false).await.is_err());
        assert!(cache.recover("deployment-b", false).await.is_err());
        assert!(cache.recover("deployment-a", true).await.is_err());
        let snapshot = cache.validated_catalog().await.unwrap();
        assert!(cache
            .administration(BOOTSTRAP, "deployment-a", &snapshot)
            .await
            .is_err());
        let mut c = cache.connection.clone();
        redis::cmd("DEL")
            .arg(&cache.control)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(ReadCache::reject_unconfigured(&url, &cache.catalog)
            .await
            .is_ok());
        cache
            .administration(BOOTSTRAP, "deployment-a", &snapshot)
            .await
            .unwrap();
        assert!(cache
            .administration(BOOTSTRAP, "deployment-a", &snapshot)
            .await
            .is_err());
        let key = cache.request_key("GET", "/i/_doc/1", b"");
        let old = cache.lookup(&key).await.unwrap().ticket.unwrap();
        cache.publish(&old, &key, b"old response").await.unwrap();
        let writer = cache.admit_writer().await.unwrap();
        let maintenance = cache.admit_state("maintenance").await.unwrap();
        assert!(!maintenance.id().is_empty());
        assert_eq!(cache.status().await.unwrap().state, "maintenance");
        assert_eq!(cache.status().await.unwrap().writer_tokens.len(), 2);
        assert!(cache.admit_writer().await.is_err());
        assert!(cache.lookup(&key).await.is_err());
        assert!(cache.publish(&old, &key, b"unsafe").await.is_err());
        cache.release_writer(&writer).await.unwrap();
        assert_eq!(cache.status().await.unwrap().state, "maintenance");
        redis::cmd("SET")
            .arg(&cache.admin)
            .arg("uncertain-admin")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache
            .administration(RECOVER, "deployment-b", &snapshot)
            .await
            .is_err());
        redis::cmd("DEL")
            .arg(&cache.admin)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        cache
            .administration(RECOVER, "deployment-b", &snapshot)
            .await
            .unwrap();
        assert!(cache.lookup(&key).await.is_err());
        assert!(cache.release_writer(&maintenance).await.is_err());
        assert!(
            cache
                .administration(RECOVER, "deployment-c", &snapshot)
                .await
                .is_err(),
            "expected identity CAS"
        );
        let fresh = ReadCache::connect(
            &url,
            &cache.catalog,
            "deployment-b",
            b"build-and-config",
            60,
            1024,
        )
        .await
        .unwrap();
        assert!(fresh.lookup(&key).await.unwrap().response.is_none());
        assert!(fresh.status().await.unwrap().writer_tokens.is_empty());
        assert!(
            fresh
                .administration(RECOVER, "deployment-a", &snapshot)
                .await
                .is_err(),
            "old identity cannot resurrect entries"
        );
        // Deleting control does not make a previously used identity reusable.
        redis::cmd("DEL")
            .arg(&cache.control)
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(cache
            .administration(BOOTSTRAP, "deployment-a", &snapshot)
            .await
            .is_err());
        assert!(
            fresh
                .administration(BOOTSTRAP, "deployment-c", &snapshot)
                .await
                .is_err(),
            "loss is not first initialization"
        );
        assert!(ReadCache::reject_unconfigured(&url, &cache.catalog)
            .await
            .is_err());
        fresh
            .administration(RECOVER, "deployment-c", &snapshot)
            .await
            .unwrap();
        let recovered = ReadCache::connect(
            &url,
            &cache.catalog,
            "deployment-c",
            b"build-and-config",
            60,
            1024,
        )
        .await
        .unwrap();
        assert_eq!(recovered.status().await.unwrap().state, "active");
        assert!(recovered.lookup(&key).await.unwrap().response.is_none());
        redis::cmd("SET")
            .arg(&cache.control)
            .arg("corrupted-type")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        recovered
            .administration(RECOVER, "deployment-d", &snapshot)
            .await
            .unwrap();
        let repaired = ReadCache::connect(
            &url,
            &cache.catalog,
            "deployment-d",
            b"build-and-config",
            60,
            1024,
        )
        .await
        .unwrap();
        assert_eq!(repaired.status().await.unwrap().state, "active");
        cleanup(&cache).await;
    }
}
