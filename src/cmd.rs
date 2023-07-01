#[cfg(feature = "cluster")]
use redis::cluster::ClusterClient;
use redis::streams::StreamReadOptions;
use redis::{from_redis_value, ToRedisArgs, Value, AsyncCommands};
use std::fmt::{Debug, Formatter};
use redis::aio::ConnectionManager;

#[derive(Clone)]
pub struct RedisClient {
    #[cfg(feature = "cluster")]
    pub client: ClusterClient,
    #[cfg(feature = "single")]
    pub client: ConnectionManager,
}

impl Debug for RedisClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(""))
    }
}

impl RedisClient {
    #[tracing::instrument]
    pub async fn get<K: ToRedisArgs + Debug + Send + Sync>(&mut self, key: K) -> redis::RedisResult<String> {
        #[cfg(feature = "cluster")]
        let data = self.client.get_connection()?.get(key)?;

        #[cfg(feature = "single")]
        let data = self.client.get(key).await?;
        Ok(data)
    }

    #[tracing::instrument]
    pub async fn exists<K: ToRedisArgs + Debug + Send + Sync>(&mut self, key: K) -> redis::RedisResult<u8> {
        #[cfg(feature = "cluster")]
        let data = self.client.get_connection()?.exists(key)?;

        #[cfg(feature = "single")]
        let data = self.client.exists(key).await?;
        Ok(data)
    }

    #[tracing::instrument]
    pub async fn set<K: ToRedisArgs + Debug + Send + Sync, V: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        value: V,
    ) -> redis::RedisResult<bool> {
        #[cfg(feature = "cluster")]
        let ok = self.client.get_connection()?.set(key, value)?;

        #[cfg(feature = "single")]
        let ok =self.client.set(key, value).await?;
        Ok(ok)
    }

    #[tracing::instrument]
    pub async fn expire<K: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        expire: usize,
    ) -> redis::RedisResult<i8> {
        #[cfg(feature = "cluster")]
        let ok = self.client.get_connection()?.expire(key, expire)?;

        #[cfg(feature = "single")]
        let ok =self.client.expire(key, expire).await?;
        Ok(ok)
    }

    #[tracing::instrument]
    pub async fn del<K: ToRedisArgs + Debug + Send + Sync>(&mut self, key: K) -> redis::RedisResult<u8> {
        #[cfg(feature = "cluster")]
        let ok = self.client.get_connection()?.del(key)?;

        #[cfg(feature = "single")]
        let ok =self.client.del(key).await?;
        Ok(ok)
    }

    #[tracing::instrument]
    pub async fn xadd<K: ToRedisArgs + Debug + Send + Sync, F: ToRedisArgs + Debug + Send + Sync, V: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        items: &[(F, V)],
    ) -> redis::RedisResult<String> {
        #[cfg(feature = "cluster")]
        let ok = self.client.get_connection()?.xadd(key, "*", items)?;

        #[cfg(feature = "single")]
        let ok =self.client.xadd(key, "*", items).await?;
        Ok(ok)
    }

    /// return OK
    #[tracing::instrument]
    pub async fn xgroup_create<K: ToRedisArgs + Debug + Send + Sync, G: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        group: G,
    ) -> redis::RedisResult<()> {
        let ok = self.is_exist_group_name(&key, &group).await?;

        if !ok {
            #[cfg(feature = "cluster")]
            self
                .client
                .get_connection()?
                .xgroup_create_mkstream(key, group, 0)?;

            #[cfg(feature = "single")]
            self.client.xgroup_create_mkstream(key, group, 0).await?;


        }
        Ok(())
    }

    /// return OK
    #[tracing::instrument]
    pub async fn xread_group(
        &mut self,
        key: &str,
        group: &str,
        consumer: &str,
    ) -> redis::RedisResult<Value> {
        let keys = vec![key];
        let ids = vec![">"];
        let mut opts = StreamReadOptions::default();
        opts = opts.count(1);
        opts = opts.group(group, consumer);
        opts = opts.block(3000);
        // opts = opts.noack();
        #[cfg(feature = "cluster")]
        let data = self
            .client
            .get_connection()?
            .xread_options(&keys, &ids, &opts)?;

        #[cfg(feature = "single")]
        let data = self.client.xread_options(&keys, &ids, &opts).await?;
        Ok(data)
    }

    #[tracing::instrument]
    pub async fn xinfo_groups<K: ToRedisArgs + Debug + Send + Sync>(&mut self, key: K) -> redis::RedisResult<Value> {
        #[cfg(feature = "cluster")]
        let v = self.client.get_connection()?.xinfo_groups(key)?;

        #[cfg(feature = "single")]
        let v = self.client.xinfo_groups(key).await?;
        Ok(v)
    }

    #[tracing::instrument]
    pub async fn is_exist_group_name<K: ToRedisArgs + Debug + Send + Sync, G: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        group: G,
    ) -> redis::RedisResult<bool> {
        let groups = self.xinfo_groups(&key).await?;
        let groups: Vec<Vec<Value>> = from_redis_value(&groups)?;
        let groupname = group.to_redis_args().get(0).unwrap().to_owned();
        let groupname = String::from_utf8(groupname).unwrap();
        for group in groups {
            let name = group.get(1).unwrap();
            let name: String = from_redis_value(name).unwrap();
            if name == groupname {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[tracing::instrument]
    pub async fn xinfo_consumers<K: ToRedisArgs + Debug + Send + Sync, G: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        group: G,
    ) -> redis::RedisResult<Value> {
        #[cfg(feature = "cluster")]
        let v = self.client.get_connection()?.xinfo_consumers(key, group)?;

        #[cfg(feature = "single")]
        let v = self.client.xinfo_consumers(key, group).await?;
        Ok(v)
    }

    #[tracing::instrument]
    pub async fn xclaim_auto<
        K: ToRedisArgs + Debug + Send + Sync,
        G: ToRedisArgs + Debug + Send + Sync,
        C: ToRedisArgs + Debug + Send + Sync,
        MIT: ToRedisArgs + Debug + Send + Sync,
    >(
        &mut self,
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
    ) -> redis::RedisResult<Value> {
        let data = self.xpending_one(&key, &group).await?;
        let id = self.get_id(&data).await?;
        let ids = vec![id];
        let mut opts = redis::streams::StreamClaimOptions::default();
        opts = opts.time(60000);
        opts = opts.idle(60000);

        #[cfg(feature = "cluster")]
        let v = self.client.get_connection()?.xclaim_options(
            key,
            group,
            consumer,
            min_idle_time,
            &ids,
            opts,
        )?;

        #[cfg(feature = "single")]
        let v = self.client.xclaim_options(
            key,
            group,
            consumer,
            min_idle_time,
            &ids,
            opts,
        ).await?;
        Ok(v)
    }

    #[tracing::instrument]
    pub async fn xpending_one<K: ToRedisArgs + Debug + Send + Sync, G: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        group: G,
    ) -> redis::RedisResult<Value> {
        #[cfg(feature = "cluster")]
        let d: Value = self
            .client
            .get_connection()?
            .xpending_count(key, group, "-", "+", 1)?;

        #[cfg(feature = "single")]
        let d: Value = self
            .client
            .xpending_count(key, group, "-", "+", 1).await?;
        Ok(d)
    }

    #[tracing::instrument]
    pub async fn get_id(&self, data: &Value) -> redis::RedisResult<String> {
        let data: Vec<Vec<Value>> = from_redis_value(&data)?;
        if data.len() > 0 {
            let data = data.get(0).unwrap();
            if data.len() > 0 {
                let data = data.get(0).unwrap();
                let data = from_redis_value(data)?;
                return Ok(data);
            }
        }
        Ok(String::new())
    }

    #[tracing::instrument]
    pub async fn xack<K: ToRedisArgs + Debug + Send + Sync, G: ToRedisArgs + Debug + Send + Sync, I: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        group: G,
        ids: &Vec<I>,
    ) -> redis::RedisResult<u64> {
        #[cfg(feature = "cluster")]
        let ack_count = self.client.get_connection()?.xack(key, group, ids)?;

        #[cfg(feature = "single")]
        let ack_count = self.client.xack(key, group, ids).await?;
        Ok(ack_count)
    }

    #[tracing::instrument]
    pub async fn xack2del<
        K: ToRedisArgs + Debug + Send + Sync,
        G: ToRedisArgs + Debug + Send + Sync,
        I: ToRedisArgs + Debug + Send + Sync,
    >(
        &mut self,
        key: K,
        group: G,
        ids: &Vec<I>,
    ) -> redis::RedisResult<(u64, u64)> {
        #[cfg(feature = "cluster")]
        let ack_count = self.client.get_connection()?.xack(&key, group, ids)?;

        #[cfg(feature = "single")]
        let ack_count = self.client.xack(&key, group, ids).await?;
        let del_count = self.xdel(key, ids).await?;
        Ok((ack_count, del_count))
    }

    ///
    #[tracing::instrument]
    pub async fn xdel<K: ToRedisArgs + Debug + Send + Sync, I: ToRedisArgs + Debug + Send + Sync>(
        &mut self,
        key: K,
        ids: &Vec<I>,
    ) -> redis::RedisResult<u64> {
        #[cfg(feature = "cluster")]
        let del_count = self.client.get_connection()?.xdel(key, ids)?;

        #[cfg(feature = "single")]
        let del_count = self.client.xdel(key, ids).await?;
        Ok(del_count)
    }

    #[cfg(feature = "cluster")]
    pub async fn ping(&mut self) -> redis::RedisResult<bool> {
        let ok = self.client.get_connection()?.check_connection();
        Ok(ok)
    }
}