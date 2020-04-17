//! TDF Config
//! Turbo Development Framework
//!
#[macro_use]
extern crate lazy_static;

pub const MAX_POOL_SIZE: u32 = 64;
pub const MIN_POOL_SIZE: u32 = 8;

pub const REDIS_POOL_SIZE: u32 = 32;

use r2d2::PooledConnection;
use r2d2_redis::RedisConnectionManager;
use sqlx::{Connect, MySqlConnection, MySqlPool, PgConnection, PgPool};

/// 数据源
pub trait DataSource {
    type C;
    fn get_url(&self) -> String;
    fn get_pool(&mut self) -> sqlx::Pool<Self::C>
    where
        Self::C: Connect;
}

#[derive(Debug, Clone)]
pub struct MySqlDataSource {
    url: String,
    pool: sqlx::Pool<MySqlConnection>,
}

impl DataSource for MySqlDataSource {
    type C = MySqlConnection;
    fn get_url(&self) -> String {
        self.url.to_string()
    }
    fn get_pool(&mut self) -> sqlx::Pool<Self::C> {
        self.pool.clone()
    }
}

pub async fn mysql_data_source() -> MySqlDataSource {
    dotenv::dotenv().ok();
    let url = std::env::var("MYSQL_URL").expect("MYSQL_URL must be set");
    let max_pool_size: u32 = std::env::var("MYSQL_MAX_POOL_SIZE")
        .unwrap_or_else(|_| MAX_POOL_SIZE.to_string())
        .parse::<u32>()
        .unwrap_or(MAX_POOL_SIZE);
    let min_pool_size: u32 = std::env::var("MYSQL_MIN_POOL_SIZE")
        .unwrap_or_else(|_| MIN_POOL_SIZE.to_string())
        .parse::<u32>()
        .unwrap_or(MIN_POOL_SIZE);

    let pool: sqlx::MySqlPool = sqlx::Pool::builder()
        .max_size(max_pool_size)
        .min_size(min_pool_size)
        .build(&url)
        .await
        .unwrap();
    MySqlDataSource { url, pool }
}

#[derive(Debug, Clone)]
pub struct PgDataSource {
    pub url: String,
    pub pool: sqlx::Pool<PgConnection>,
}

impl DataSource for PgDataSource {
    type C = PgConnection;

    fn get_url(&self) -> String {
        self.url.to_string()
    }

    fn get_pool(&mut self) -> sqlx::Pool<Self::C> {
        self.pool.clone()
    }
}

pub async fn pg_data_source() -> PgDataSource {
    dotenv::dotenv().ok();
    let url = std::env::var("PG_URL").expect("PG_URL must be set");
    let max_pool_size: u32 = std::env::var("PG_MAX_POOL_SIZE")
        .unwrap_or_else(|_| MAX_POOL_SIZE.to_string())
        .parse::<u32>()
        .unwrap_or(MAX_POOL_SIZE);
    let min_pool_size: u32 = std::env::var("PG_MIN_POOL_SIZE")
        .unwrap_or_else(|_| MIN_POOL_SIZE.to_string())
        .parse::<u32>()
        .unwrap_or(MIN_POOL_SIZE);

    let pool: sqlx::PgPool = sqlx::Pool::builder()
        .max_size(max_pool_size)
        .min_size(min_pool_size)
        .build(&url)
        .await
        .unwrap();
    PgDataSource { url, pool }
}

#[derive(Debug, Clone)]
pub struct RedisDataSource {
    pub url: String,
    pub pool: r2d2::Pool<RedisConnectionManager>,
}

impl RedisDataSource {
    fn get_url(&self) -> String {
        self.url.to_string()
    }
    fn get_pool(self) -> r2d2::Pool<RedisConnectionManager> {
        self.pool.clone()
    }
}

pub fn redis_data_source() -> RedisDataSource {
    dotenv::dotenv().ok();
    let url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");
    let manager = RedisConnectionManager::new(url.clone()).unwrap();
    let redis_pool_size = std::env::var("REDIS_POOL_SIZE")
        .unwrap_or_else(|_| REDIS_POOL_SIZE.to_string())
        .parse::<u32>()
        .unwrap_or(REDIS_POOL_SIZE);
    let pool = r2d2::Pool::builder()
        .max_size(redis_pool_size)
        .build(manager)
        .unwrap();
    RedisDataSource { url, pool }
}

#[cfg(feature = "with-mysql")]
pub type TdfDataSource = MySqlDataSource;
#[cfg(feature = "with-postgres")]
pub type TdfDataSource = PgDataSource;
#[cfg(feature = "with-mysql")]

#[cfg(feature = "with-mysql")]
pub async fn data_source() -> TdfDataSource {
    mysql_data_source().await
}
#[cfg(feature = "with-postgres")]
pub async fn data_source() -> TdfDataSource {
    pg_data_source().await
}


#[cfg(feature = "with-mysql")]
pub type TdfPool = MySqlPool;
#[cfg(feature = "with-postgres")]
pub type TdfPool = PgPool;

#[cfg(test)]
mod tests {
    use crate::{mysql_data_source, pg_data_source, redis_data_source, DataSource};
    use r2d2::PooledConnection;
    use r2d2_redis::RedisConnectionManager;
    use sqlx::prelude::*;
    use std::ops::Deref;

    #[tokio::test]
    async fn test_data_source() {
        let redis_data_source = redis_data_source();
        println!("{:?}", redis_data_source);
        let pool = redis_data_source.get_pool();
        let mut conn: PooledConnection<RedisConnectionManager> = pool.get().unwrap();
        let reply = redis::cmd("PING").query::<String>(&mut *conn).unwrap();

        assert_eq!("PONG", reply);

        let mut my_data_source = mysql_data_source().await;
        println!("{:?}", my_data_source);
        let pool = my_data_source.get_pool();
        println!("{:?}", pool);

        let mut cursor = sqlx::query(r#"SELECT version() v"#).fetch(&pool);
        let row = cursor.next().await.unwrap().unwrap();
        let version = row.get::<&str, &str>("v").to_string();
        println!("{:?}", version);

        let mut pg_data_source = pg_data_source().await;
        println!("{:?}", pg_data_source);
        let pool = pg_data_source.get_pool();
        println!("{:?}", pool);

        let mut cursor = sqlx::query(r#"SELECT version() v"#).fetch(&pool);
        let row = cursor.next().await.unwrap().unwrap();
        let version = row.get::<&str, &str>("v").to_string();
        println!("{:?}", version);
        assert!(version.len() > 0);
        // let version = my_data_source.get_version().await;
        // assert_eq!(version.is_ok(), true);
    }
}

// impl MySqlDataSource {
//     async fn get_version(&mut self) -> std::result::Result<String, Box<dyn std::error::Error>> {
//         use sqlx::mysql::MySqlRow;
//         use sqlx::prelude::*;
//         use sqlx_core::mysql::MySqlCursor;
//
//         let mut cursor: MySqlCursor = sqlx::query(r#"SELECT version() v"#).fetch(&self.pool);
//         //        let row: MySqlRow = cursor.next().await.unwrap().unwrap();
//         let row = cursor.next().await;
//         match row {
//             Ok(r) => {
//                 let version = r.unwrap().get::<&str, &str>("v").to_string();
//                 Ok(version)
//             }
//             Err(e) => Err(Box::<dyn std::error::Error>::from(e)),
//         }
//     }
// }
