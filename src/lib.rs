//! TDF Config
//! Turbo Development Framework
//!
#[macro_use]
extern crate lazy_static;

pub const DB_POOL_SIZE: u32 = 8;
pub const REDIS_POOL_SIZE: u32 = 32;

use postgres::NoTls;
use r2d2;
use r2d2::{Pool, PooledConnection};
use r2d2_mysql;
use r2d2_mysql::mysql::{Opts, OptsBuilder};
use r2d2_mysql::MysqlConnectionManager;
use r2d2_postgres::PostgresConnectionManager;
use r2d2_redis::RedisConnectionManager;

/// 数据源
pub trait DataSource {
    type MC;
    // ManageConnection
    fn get_url(&self) -> String;
    fn get_pool(&mut self) -> PooledConnection<Self::MC>
        where
            Self::MC: r2d2::ManageConnection;
}

#[derive(Debug, Clone)]
pub struct MysqlDataSource {
    url: String,
    pool: Pool<MysqlConnectionManager>,
}

impl MysqlDataSource {
    pub fn new(url: String) -> Self {
        let opts = Opts::from_url(&url).unwrap();
        let builder = OptsBuilder::from_opts(opts);
        let manager = MysqlConnectionManager::new(builder);
        MysqlDataSource {
            url,
            pool: r2d2::Pool::builder()
                .max_size(DB_POOL_SIZE)
                .build(manager)
                .unwrap(),
        }
    }
}

impl DataSource for MysqlDataSource {
    type MC = MysqlConnectionManager;

    fn get_url(&self) -> String {
        self.url.to_string()
    }

    fn get_pool(&mut self) -> PooledConnection<Self::MC> {
        self.pool.get().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct PostgresDataSource {
    url: String,
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresDataSource {
    pub fn new(url: String) -> Self {
        let manager = PostgresConnectionManager::new(url.parse().unwrap(), NoTls);
        PostgresDataSource {
            url,
            pool: r2d2::Pool::builder()
                .max_size(DB_POOL_SIZE)
                .build(manager)
                .unwrap(),
        }
    }
}

impl DataSource for PostgresDataSource {
    type MC = PostgresConnectionManager<NoTls>;

    fn get_url(&self) -> String {
        self.url.to_string()
    }

    fn get_pool(&mut self) -> PooledConnection<Self::MC> {
        self.pool.get().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct RedisDataSource {
    url: String,
    pool: Pool<RedisConnectionManager>,
}

impl RedisDataSource {
    pub fn new(url: String) -> Self {
        let manager = RedisConnectionManager::new(url.clone()).unwrap();
        let pool = r2d2::Pool::builder()
            .max_size(REDIS_POOL_SIZE)
            .build(manager)
            .unwrap();
        RedisDataSource { url, pool }
    }
}

impl DataSource for RedisDataSource {
    type MC = RedisConnectionManager;

    fn get_url(&self) -> String {
        self.url.to_string()
    }

    fn get_pool(&mut self) -> PooledConnection<Self::MC> {
        self.pool.get().unwrap()
    }
}

pub fn redis_connection() -> RedisDataSource {
    dotenv::dotenv().ok();
    let redis_url = dotenv::var("REDIS_URL").expect("REDIS_URL must be set");
    RedisDataSource::new(redis_url)
}

pub fn mysql_connection() -> MysqlDataSource {
    dotenv::dotenv().ok();
    let db_url = dotenv::var("MYSQL_URL").expect("MYSQL_URL must be set");
    MysqlDataSource::new(db_url)
}

#[cfg(feature = "with-mysql")]
pub fn establish_connection() -> MysqlDataSource {
    mysql_connection()
}

#[cfg(feature = "with-mysql")]
pub fn get_connection() -> PooledConnection<MysqlConnectionManager> {
    DATASOURCE.pool.get().unwrap()
}

#[cfg(feature = "with-mysql")]
lazy_static! {
    pub static ref DATASOURCE : MysqlDataSource = {
        dotenv::dotenv().ok();
        let db_url = dotenv::var("MYSQL_URL").expect("MYSQL_URL must be set");
        MysqlDataSource::new(db_url)
    };
}

pub fn postgres_connection() -> PostgresDataSource {
    dotenv::dotenv().ok();
    let db_url = dotenv::var("POSTGRESQL_URL").expect("POSTGRESQL_URL must be set");
    PostgresDataSource::new(db_url)
}

#[cfg(feature = "with-postgres")]
pub fn establish_connection() -> PostgresDataSource {
    postgres_connection()
}

#[cfg(feature = "with-postgres")]
pub fn get_connection() -> PooledConnection<PostgresConnectionManager<NoTls>> {
    DATASOURCE.pool.get().unwrap()
}

#[cfg(feature = "with-postgres")]
lazy_static! {
    pub static ref DATASOURCE : PostgresDataSource = {
        dotenv::dotenv().ok();
        let db_url = dotenv::var("POSTGRESQL_URL").expect("POSTGRESQL_URL must be set");
        PostgresDataSource::new(db_url)
    };
}


#[cfg(test)]
mod tests {
    use r2d2::{PooledConnection, ManageConnection};
    use crate::{postgres_connection, mysql_connection, RedisDataSource};

    #[test]
    fn test_data_source() {
        let redis_data_source = RedisDataSource::new("redis://localhost/0".to_string());
        println!("{:?}", redis_data_source);
        let my_data_source = mysql_connection();
        println!("{:?}", my_data_source);
        let pg_data_source = postgres_connection();
        println!("{:?}", pg_data_source);
    }
}

//        let mut obj:Box<dyn DataSource<PC=MysqlConnectionManager>> = Box::new(my_data_source);
// let mut obj = Box::new(my_data_source) as Box<dyn DataSource<PC=MysqlConnectionManager>>;
//        let p = obj.get_pool();
//         let p = my_data_source.get_version();
//         println!("{:?}", p);
// let mut config = Config {
//     data_source: Box::new(
//         (MysqlDataSource::new(
//             "mysql://app:app@localhost:3306/app?tcp_connect_timeout_ms=5000".to_string(),
//         )),
//     ),
// };
//
// let mut dao= MysqlDao::new(config);
// let v = dao.get_version();
// println!("{:?}", v);

//pub trait Dao<T> {}

// pub struct MysqlDao {
// //    config: Config<T>,
//     pool: PooledConnection<MysqlConnectionManager>,
// }

// impl MysqlDao<PooledConnection<MysqlConnectionManager>> {
//     fn new(mut config: Config<MysqlConnectionManager>) -> Self {
//         MysqlDao { pool: config.data_source.get_pool()}
//     }
// }
//
// impl Repository for MysqlDataSource {
//     fn get_version(&mut self) -> std::result::Result<String, Box<dyn std::error::Error>> {
//         let mut conn = self.pool.get().unwrap();
//         match conn.query(r#"select version() v"#).and_then(|mut qr| {
//             let row = qr.next().unwrap();
//             row.map(|x| from_row::<String>(x))
//         }) {
//             Ok(s) => Ok(s),
//             Err(e) => Err(Box::<dyn std::error::Error>::from("table name error"))
//         }
//     }
// }

// impl Repository for PostgresDataSource {
//     fn get_version(&mut self) -> std::result::Result<String, Box<dyn std::error::Error>> {
//         let mut conn = self.pool.get().unwrap();
//         let v = conn.query(r#"select version() v"#, &[]).unwrap()[0].get::<usize, String>(0);
//         Ok(v)
//         // match conn.query(r#"select version() v"#, &[]).and_then(|mut qr| {
//         //     let row = qr.next().unwrap();
//         //     row.map(|x| from_row::<String>(x))
//         // }) {
//         //     Ok(s) => Ok(s),
//         //     Err(e) => Err(Box::<dyn std::error::Error>::from("table name error"))
//         // }
//     }
// }

// #[derive(Debug)]
// pub enum DS {
//     My(MysqlDataSource),
//     Pg(PostgresDataSource),
// }
//
// impl Repository for DS {
//     fn get_version(&mut self) -> Result<String, Box<dyn std::error::Error>> {
//        match self {
//            DS::My(ds) => ds.get_version(),
//            DS::Pg(ds) => ds.get_version(),
//
//        }
//     }
// }
