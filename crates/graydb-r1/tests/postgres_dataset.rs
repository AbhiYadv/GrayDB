use anyhow::Result;
use graydb_r1::{DatasetLoader, PostgresCopySink, PostgresPublishedSizeProbe};

const SCHEMA_SQL: &str = include_str!("../../../bench/r1/schema.sql");
const MINIMUM_BYTES: u64 = 64 << 20;

async fn connect() -> Result<tokio_postgres::Client> {
    let url = std::env::var("R1_POSTGRES_URL")
        .expect("Task 11 must set R1_POSTGRES_URL for the ignored service test");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn load_once() -> Result<(String, u64, [i64; 4])> {
    let admin = connect().await?;
    admin.batch_execute("DROP PUBLICATION IF EXISTS graydb_r1_pub; DROP PUBLICATION IF EXISTS graydb_r1_control_pub; DROP SCHEMA IF EXISTS r1 CASCADE; DROP SCHEMA IF EXISTS r1_control CASCADE;").await?;
    admin.batch_execute(SCHEMA_SQL).await?;
    let manifest = DatasetLoader::with_probe(
        PostgresPublishedSizeProbe::new(connect().await?),
        PostgresCopySink::new(connect().await?),
        20_260_901,
    )
    .load_until(MINIMUM_BYTES)
    .await?;
    let mut counts = [0; 4];
    for (index, table) in ["tenants", "customers", "orders", "order_events"]
        .iter()
        .enumerate()
    {
        counts[index] = admin
            .query_one(&format!("SELECT count(*) FROM r1.{table}"), &[])
            .await?
            .get(0);
    }
    Ok((
        manifest.content_hash()?,
        manifest.published_table_bytes,
        counts,
    ))
}

#[tokio::test]
#[ignore = "requires the r1 PostgreSQL service"]
async fn postgres_dataset_load_is_reproducible() -> Result<()> {
    let (first_hash, first_bytes, first_counts) = load_once().await?;
    let (second_hash, second_bytes, second_counts) = load_once().await?;
    assert!(first_bytes >= MINIMUM_BYTES && second_bytes >= MINIMUM_BYTES);
    assert_eq!(first_hash, second_hash);
    assert_eq!(first_counts, second_counts);
    assert!(first_counts.iter().all(|rows| *rows > 0));
    Ok(())
}
