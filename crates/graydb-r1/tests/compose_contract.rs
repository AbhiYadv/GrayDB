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
    assert!(raw["volumes"].is_null(), "named volumes are forbidden");

    for (name, port, image) in [
        ("postgres", "127.0.0.1:55432:5432", Some("postgres:17")),
        ("graydb", "127.0.0.1:57432:7432", None),
        (
            "clickhouse",
            "127.0.0.1:58123:8123",
            Some("clickhouse/clickhouse-server:25.8"),
        ),
    ] {
        let service = &services[&serde_yaml::Value::String(name.to_string())];
        let ports = service["ports"]
            .as_sequence()
            .expect("service ports must be a sequence")
            .iter()
            .map(|port| port.as_str().expect("service port must be a string"))
            .collect::<Vec<_>>();
        assert_eq!(ports, vec![port], "{name} must expose only its R1 port");
        if let Some(image) = image {
            assert_eq!(service["image"].as_str(), Some(image), "{name}");
        } else {
            assert!(service["build"].is_mapping(), "graydb must be built");
            assert_eq!(service["build"]["context"].as_str(), Some("../.."));
            assert_eq!(
                service["build"]["dockerfile"].as_str(),
                Some("bench/r1/Dockerfile")
            );
        }

        let volumes = service["volumes"]
            .as_sequence()
            .expect("every persistent service needs explicit bind mounts");
        assert!(!volumes.is_empty(), "{name} must not use anonymous storage");
        for volume in volumes {
            let volume = volume.as_str().expect("Compose volume must be a string");
            let source = volume
                .split_once(':')
                .expect("bind mount must have a source and destination")
                .0;
            assert!(source.starts_with("${R1_DATA_ROOT}/"), "{volume}");
        }
    }
}
