use pgx::prelude::*;
use pgx::spi::SpiTupleTable;
use pgx::warning;

pgx::pg_module_magic!();

pub mod partition;
use pgmq_crate::query::{delete, enqueue_str, init_queue, pop, read};

#[pg_extern]
fn pgmq_create(queue_name: &str) -> Result<(), spi::Error> {
    let setup = init_queue(queue_name);
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });
    match ran {
        Ok(_) => Ok(()),
        Err(ran) => Err(ran),
    }
}

#[pg_extern]
fn pgmq_create_partitioned(
    queue_name: &str,
    partition_size: default!(i64, 10000),
) -> Result<(), spi::Error> {
    let setup = partition::init_partitioned_queue(queue_name, partition_size);
    let ran: Result<_, spi::Error> = Spi::connect(|mut c| {
        for q in setup {
            let _ = c.update(&q, None, None)?;
        }
        Ok(())
    });

    match ran {
        Ok(_) => Ok(()),
        Err(ran) => Err(ran),
    }
}

#[pg_extern]
fn pgmq_send(queue_name: &str, message: pgx::Json) -> Result<Option<i64>, spi::Error> {
    let m = serde_json::to_string(&message.0).unwrap();
    Spi::get_one(&enqueue_str(queue_name, &m))
}

#[pg_extern]
fn pgmq_read(
    queue_name: &str,
    vt: i32,
    limit: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(msg_id, i64),
            name!(read_ct, i32),
            name!(vt, TimestampWithTimeZone),
            name!(enqueued_at, TimestampWithTimeZone),
            name!(message, pgx::Json),
        ),
    >,
    spi::Error,
> {
    let results = readit(queue_name, vt, limit)?;
    Ok(TableIterator::new(results.into_iter()))
}

fn readit(
    queue_name: &str,
    vt: i32,
    limit: i32,
) -> Result<
    Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgx::Json,
    )>,
    spi::Error,
> {
    let mut results: Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgx::Json,
    )> = Vec::new();
    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        let mut tup_table: SpiTupleTable =
            client.update(&read(queue_name, &vt, &limit), None, None)?;
        while let Some(row) = tup_table.next() {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            let read_ct = row["read_ct"].value::<i32>()?.expect("no read_ct");
            let vt = row["vt"].value::<TimestampWithTimeZone>()?.expect("no vt");
            let enqueued_at = row["enqueued_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no enqueue time");
            let message = row["message"].value::<pgx::Json>()?.expect("no message");
            results.push((msg_id, read_ct, vt, enqueued_at, message));
        }
        Ok(())
    });
    Ok(results)
}

#[pg_extern]
fn pgmq_delete(queue_name: &str, msg_id: i64) -> Result<Option<bool>, spi::Error> {
    let mut num_deleted = 0;

    Spi::connect(|mut client| {
        let tup_table = client.update(&delete(queue_name, &msg_id), None, None);
        match tup_table {
            Ok(tup_table) => num_deleted = tup_table.len(),
            Err(e) => {
                error!("error deleting message: {}", e);
            }
        }
    });
    match num_deleted {
        1 => Ok(Some(true)),
        0 => {
            warning!("no message found with msg_id: {}", msg_id);
            Ok(Some(false))
        }
        _ => {
            error!("multiple messages found with msg_id: {}", msg_id);
        }
    }
}

// reads and deletes at same time
#[pg_extern]
fn pgmq_pop(
    queue_name: &str,
) -> Result<
    TableIterator<
        'static,
        (
            name!(msg_id, i64),
            name!(read_ct, i32),
            name!(vt, TimestampWithTimeZone),
            name!(enqueued_at, TimestampWithTimeZone),
            name!(message, pgx::Json),
        ),
    >,
    spi::Error,
> {
    let results = popit(queue_name)?;
    Ok(TableIterator::new(results.into_iter()))
}

fn popit(
    queue_name: &str,
) -> Result<
    Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgx::Json,
    )>,
    spi::Error,
> {
    let mut results: Vec<(
        i64,
        i32,
        TimestampWithTimeZone,
        TimestampWithTimeZone,
        pgx::Json,
    )> = Vec::new();
    let _: Result<(), spi::Error> = Spi::connect(|mut client| {
        let tup_table: SpiTupleTable = client.update(&pop(queue_name), None, None)?;
        for row in tup_table {
            let msg_id = row["msg_id"].value::<i64>()?.expect("no msg_id");
            let read_ct = row["read_ct"].value::<i32>()?.expect("no read_ct");
            let vt = row["vt"].value::<TimestampWithTimeZone>()?.expect("no vt");
            let enqueued_at = row["enqueued_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no enqueue time");
            let message = row["message"].value::<pgx::Json>()?.expect("no message");
            results.push((msg_id, read_ct, vt, enqueued_at, message));
        }
        Ok(())
    });
    Ok(results)
}

#[pg_extern]
fn pgmq_list_queues() -> Result<
    TableIterator<
        'static,
        (
            name!(queue_name, String),
            name!(created_at, TimestampWithTimeZone),
        ),
    >,
    spi::Error,
> {
    let results = listit()?;
    Ok(TableIterator::new(results.into_iter()))
}

fn listit() -> Result<Vec<(String, TimestampWithTimeZone)>, spi::Error> {
    let mut results: Vec<(String, TimestampWithTimeZone)> = Vec::new();
    let query = "SELECT * FROM pgmq_meta";
    let _: Result<(), spi::Error> = Spi::connect(|client| {
        let tup_table: SpiTupleTable = client.select(query, None, None)?;
        for row in tup_table {
            let queue_name = row["queue_name"].value::<String>()?.expect("no queue_name");
            let created_at = row["created_at"]
                .value::<TimestampWithTimeZone>()?
                .expect("no created_at");
            results.push((queue_name, created_at));
        }
        Ok(())
    });
    Ok(results)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::*;
    use pgmq_crate::query::TABLE_PREFIX;

    #[pg_test]
    fn test_create() {
        let qname = r#"test_queue"#;
        let _ = pgmq_create(&qname).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 0);
        let _ = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":"y"}))).unwrap();
        let retval = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
            .expect("SQL select failed");
        assert_eq!(retval.unwrap(), 1);
    }

    // assert an invisible message is not readable
    #[pg_test]
    fn test_default() {
        let qname = r#"test_default"#;
        let _ = pgmq_create(&qname);
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        // should not be any messages initially
        assert_eq!(init_count.unwrap(), 0);

        // put a message on the queue
        let _ = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":"y"})));

        // read the message with the pg_extern, sets message invisible
        let _ = pgmq_read(&qname, 10_i32, 1_i32);
        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        // pop the message, must not panic
        let popped = pgmq_pop(&qname);
        assert!(popped.is_ok());
    }

    // validate all internal functions
    // e.g. readit, popit, listit
    #[pg_test]
    fn test_internal() {
        let qname = r#"test_internal"#;
        let _ = pgmq_create(&qname).unwrap();

        let queues = listit().unwrap();
        assert_eq!(queues.len(), 1);

        // put two message on the queue
        let msg_id1 = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":1})))
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":2})))
            .unwrap()
            .unwrap();
        assert_eq!(msg_id1, 1);
        assert_eq!(msg_id2, 2);

        // read first message
        let msg1 = readit(&qname, 1_i32, 1_i32).unwrap();
        // pop the second message
        let msg2 = popit(&qname).unwrap();
        assert_eq!(msg1.len(), 1);
        assert_eq!(msg2.len(), 1);
        assert_eq!(msg1[0].0, msg_id1);
        assert_eq!(msg2[0].0, msg_id2);

        // read again, should be no messages
        let nothing = readit(&qname, 2_i32, 1_i32).unwrap();
        assert_eq!(nothing.len(), 0);

        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        //  delete the messages
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(delete1);

        //  delete when message is gone returns False
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(!delete1);

        // no records after delete
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 0);
    }

    /// lifecycle test for partitioned queues
    #[pg_test]
    fn test_partitioned() {
        let qname = r#"test_internal"#;
        let _ = pgmq_create_partitioned(&qname, 2).unwrap();

        let queues = listit().unwrap();
        assert_eq!(queues.len(), 1);

        // put two message on the queue
        let msg_id1 = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":1})))
            .unwrap()
            .unwrap();
        let msg_id2 = pgmq_send(&qname, pgx::Json(serde_json::json!({"x":2})))
            .unwrap()
            .unwrap();
        assert_eq!(msg_id1, 1);
        assert_eq!(msg_id2, 2);

        // read first message
        let msg1 = readit(&qname, 1_i32, 1_i32).unwrap();
        // pop the second message
        let msg2 = popit(&qname).unwrap();
        assert_eq!(msg1.len(), 1);
        assert_eq!(msg2.len(), 1);
        assert_eq!(msg1[0].0, msg_id1);
        assert_eq!(msg2[0].0, msg_id2);

        // read again, should be no messages
        let nothing = readit(&qname, 2_i32, 1_i32).unwrap();
        assert_eq!(nothing.len(), 0);

        // but still one record on the table
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 1);

        //  delete the messages
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(delete1);

        //  delete when message is gone returns False
        let delete1 = pgmq_delete(&qname, msg_id1).unwrap().unwrap();
        assert!(!delete1);

        // no records after delete
        let init_count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM {TABLE_PREFIX}_{qname}"))
                .expect("SQL select failed");
        assert_eq!(init_count.unwrap(), 0);
    }
}

#[cfg(test)]
pub mod pg_test {
    // pg_test module with both the setup and postgresql_conf_options functions are required

    use std::vec;

    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        // uncomment this when there are tests for the partman background worker
        // vec!["shared_preload_libraries = 'pg_partman_bgw'"]
        vec![]
    }
}