//! graydb-columnar: parquet segments (zstd) + roaring delete bitmaps + LSN-range footers.
//! Update = bitmap-mark + reinsert. Visibility at L: insert_lsn <= L && !deleted_by(<=L).
//! Compaction folds bitmaps; min segment size prevents small-file explosion.
//! (Compaction is post-spike; SP4 delivers write path + LSN-visibility scans.)

pub mod copytext;
pub mod reader;
pub mod store;

pub use store::{ColumnSpec, SegmentMeta, SegmentSnapshot, StoreManifest, TableStore};

#[cfg(test)]
mod tests {
    use super::store::{ColumnSpec, TableStore};
    use graydb_registry::pgoutput::TupleValue;
    use graydb_registry::{Op, TypedChange};

    fn cols() -> Vec<ColumnSpec> {
        vec![
            ColumnSpec { name: "id".into(), type_oid: 20, is_key: true },
            ColumnSpec { name: "v".into(), type_oid: 25, is_key: false },
        ]
    }

    fn change(op: Op, lsn: u64, id: i64, v: &str, with_old: bool) -> TypedChange {
        let img = vec![
            ("id".to_string(), TupleValue::Text(id.to_string())),
            ("v".to_string(), TupleValue::Text(v.to_string())),
        ];
        TypedChange {
            commit_lsn: lsn,
            xid: 1,
            table: "app.t".into(),
            op,
            new: if op == Op::Delete { None } else { Some(img.clone()) },
            old: if with_old || op == Op::Delete { Some(img) } else { None },
        }
    }

    #[test]
    fn update_is_mark_plus_reinsert_and_time_travel_works() {
        let dir = std::env::temp_dir().join(format!("gdb-col-{}", std::process::id()));
        let mut s = TableStore::create(&dir, "app.t", cols(), 1_000_000).unwrap();
        s.load_copy_part(b"1\tone\n2\ttwo\n3\tthree\n", 100).unwrap();
        s.flush().unwrap(); // exercise flushed-segment delete marking

        s.apply(&change(Op::Update, 200, 2, "TWO", false)).unwrap();
        s.apply(&change(Op::Delete, 300, 3, "three", false)).unwrap();
        s.finalize().unwrap();

        let at = |lsn: u64| {
            let mut rows: Vec<(String, String)> = s
                .scan_at(lsn)
                .unwrap()
                .into_iter()
                .map(|r| (r[0].clone().unwrap(), r[1].clone().unwrap()))
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(at(100), vec![
            ("1".into(), "one".into()),
            ("2".into(), "two".into()),
            ("3".into(), "three".into()),
        ]);
        assert_eq!(at(250), vec![
            ("1".into(), "one".into()),
            ("2".into(), "TWO".into()),
            ("3".into(), "three".into()),
        ]);
        assert_eq!(at(300), vec![
            ("1".into(), "one".into()),
            ("2".into(), "TWO".into()),
        ]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unknown_update_key_fails_loudly() {
        let dir = std::env::temp_dir().join(format!("gdb-col2-{}", std::process::id()));
        let mut s = TableStore::create(&dir, "app.t", cols(), 1_000_000).unwrap();
        s.load_copy_part(b"1\tone\n", 100).unwrap();
        let err = s.apply(&change(Op::Update, 200, 99, "x", false));
        assert!(err.is_err(), "update for unknown key must be an invariant breach");
        std::fs::remove_dir_all(&dir).ok();
    }
}
