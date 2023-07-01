use crate::cmd::RedisClient;
use serde::Deserialize;
use std::fs::read_to_string;
use redis::aio::ConnectionManager;

#[derive(Debug, Default, Clone, Deserialize)]
pub struct RedisDB {
    pub nodes: Option<Vec<String>>,
    pub password: Option<String>,
    pub node: Option<String>,
}

pub fn load_redis_config(filename: &str) -> RedisDB {
    let content = read_to_string(&filename).expect("read redis.toml error");
    let redis_db: RedisDB = toml::from_str(&content).expect("redis config init err");
    redis_db
}

impl RedisDB {
    pub async fn load_redis_pool(&self) -> redis::RedisResult<RedisClient> {
        #[cfg(feature = "cluster")]
        let client = redis::cluster::ClusterClientBuilder::new(self.nodes.unwrap().to_owned())
            .password(redis_config.password.to_owned().unwrap_or_default())
            .build()?;

        #[cfg(feature = "single")]
        let client = self.manager_client().await?;

        Ok(RedisClient { client })
    }

    #[cfg(feature = "single")]
    async fn manager_client(&self) -> redis::RedisResult<ConnectionManager> {
        let client = redis::Client::open(self.node.as_ref().unwrap().as_str())?;
        Ok(ConnectionManager::new(client).await?)
    }
}
