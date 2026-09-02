use graydb_r1::verdict::{compose_path, load_compose};

#[test]
fn compose_has_isolated_persistent_services_and_healthchecks() {
    let compose = load_compose().expect("Task 11 must provide bench/r1/compose.yml");
    for name in ["postgres", "graydb", "clickhouse"] {
        assert!(compose.services.contains_key(name));
        assert!(compose.services[name].healthcheck.is_some());
    }
    assert_eq!(
        compose.services["postgres"].memory_limit_bytes(),
        Some(3_u64 << 30)
    );
    assert_eq!(
        compose.services["graydb"].memory_limit_bytes(),
        Some(4_u64 << 30)
    );
    assert_eq!(
        compose.services["clickhouse"].memory_limit_bytes(),
        Some(4_u64 << 30)
    );
    let raw: serde_yaml::Value = serde_yaml::from_str(
        &std::fs::read_to_string(compose_path()).expect("reading R1 Compose contract"),
    )
    .expect("parsing R1 Compose contract");
    let services = raw["services"]
        .as_mapping()
        .expect("Compose services must be a mapping");
    for service in services.values() {
        for volume in service["volumes"].as_sequence().into_iter().flatten() {
            let volume = volume.as_str().expect("Compose volume must be a string");
            let source = volume
                .split_once(':')
                .expect("bind mount must have a source and destination")
                .0;
            assert!(source.starts_with("${R1_DATA_ROOT}"), "{volume}");
        }
    }
}
