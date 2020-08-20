pub const PRODUCT_NAME: &str = "fluvio";

// SPU/SC Server Path
pub const SERVER_CONFIG_BASE_PATH: &str = "/etc";
pub const SERVER_CONFIG_DIR: &str = "fluvio";
pub const CONFIG_FILE_EXTENTION: &str = "toml";

// SC defaults
pub const SC_DEFAULT_ID: i32 = 1;
pub const SC_CONFIG_FILE: &str = "sc_server";
pub const SC_PUBLIC_PORT: u16 = 9003;
pub const SC_PRIVATE_PORT: u16 = 9004;
pub const SC_HOSTNAME: &str = "localhost";
pub const SC_RECONCILIATION_INTERVAL_SEC: u64 = 300; // 5 min

// SPU defaults
pub const SPU_DEFAULT_ID: i32 = 0;
pub const SPU_DEFAULT_NAME: &str = "spu";
pub const SPU_CONFIG_FILE: &str = "spu_server";
pub const SPU_PUBLIC_PORT: u16 = 9005;
pub const SPU_PRIVATE_PORT: u16 = 9006;
pub const SPU_PUBLIC_HOSTNAME: &str = "0.0.0.0";
pub const SPU_PRIVATE_HOSTNAME: &str = "0.0.0.0";
pub const SPU_CREDENTIALS_FILE: &str = "/etc/fluvio/.credentials/token_secret";
pub const SPU_RETRY_SC_TIMEOUT_MS: u16 = 3000;
pub const SPU_MIN_IN_SYNC_REPLICAS: u16 = 1;
pub const SPU_LOG_BASE_DIR: &str = "/tmp/fluvio";
pub const SPU_LOG_SIZE: &str = "1Gi";
pub const SPU_LOG_INDEX_MAX_BYTES: u32 = 10485760;
pub const SPU_LOG_INDEX_MAX_INTERVAL_BYTES: u32 = 4096;
pub const SPU_LOG_SEGMENT_MAX_BYTES: u32 = 1073741824;

// CLI config
pub const CLI_PROFILES_DIR: &str = "profiles";
pub const CLI_DEFAULT_PROFILE: &str = "default";
pub const CLI_CONFIG_PATH: &str = ".fluvio";

// Env
pub const FLV_FLUVIO_HOME: &str = "FLUVIO_HOME";
pub const FLV_SPU_ID: &str = "FLV_SPU_ID";
pub const FLV_SPU_TYPE: &str = "FLV_SPU_TYPE";
pub const FLV_TOKEN_SECRET_FILE: &str = "FLV_TOKEN_SECRET_FILE";
pub const FLV_RACK: &str = "FLV_RACK";
pub const FLV_SPU_PUBLIC_HOST: &str = "FLV_SPU_PUBLIC_HOST";
pub const FLV_SPU_PUBLIC_PORT: &str = "FLV_SPU_PUBLIC_PORT";
pub const FLV_SPU_PRIVATE_HOST: &str = "FLV_SPU_PRIVATE_HOST";
pub const FLV_SPU_PRIVATE_PORT: &str = "FLV_SPU_PRIVATE_PORT";
pub const FLV_SC_PRIVATE_HOST: &str = "FLV_SC_PRIVATE_HOST";
pub const FLV_SC_PRIVATE_PORT: &str = "FLV_SC_PRIVATE_PORT";
pub const FLV_SC_RETRY_TIMEOUT_MS: &str = "FLV_SC_RETRY_TIMEOUT_MS";
pub const FLV_REPLICA_IN_SYNC_REPLICA_MIN: &str = "FLV_REPLICA_IN_SYNC_REPLICA_MIN";
pub const FLV_LOG_BASE_DIR: &str = "FLV_LOG_BASE_DIR";
pub const FLV_LOG_SIZE: &str = "FLV_LOG_SIZE";
pub const FLV_LOG_INDEX_MAX_BYTES: &str = "FLV_LOG_INDEX_MAX_BYTES";
pub const FLV_LOG_INDEX_MAX_INTERVAL_BYTES: &str = "FLV_LOG_INDEX_MAX_INTERVAL_BYTES";
pub const FLV_LOG_SEGMENT_MAX_BYTES: &str = "FLV_LOG_SEGMENT_MAX_BYTES";

// Health Checks
pub const HC_SPU_TRIGGER_INTERVAL_SEC: u64 = 60 * 5;
pub const HC_SPU_PING_INTERVAL_SEC: u64 = 5;

/// K8 Secret
pub const K8_TOKEN_SECRET_KEY: &str = "token_secret";

// Kafka
pub const KF_REQUEST_TIMEOUT_MS: i32 = 1500;
