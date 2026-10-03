use super::*;
use tempfile::TempDir;
use tokio::time::Duration;

fn store(artifact: u64, session: u64, global: u64) -> (TempDir, Arc<OutputArtifactStore>) {
    let root = tempfile::tempdir().expect("temporary cache");
    let config = RecoverableExecOutputConfig {
        enabled: true,
        artifact_max_bytes: artifact,
        session_max_bytes: session,
        global_max_bytes: global,
        ..Default::default()
    };
    let store = Arc::new(OutputArtifactStore::new(
        root.path()
            .canonicalize()
            .expect("canonical temporary root")
            .join("output"),
        config,
    ));
    (root, store)
}
fn read_request(id: Uuid) -> QueryRequest {
    QueryRequest {
        artifact_id: id,
        stream: OutputStream::Stdout,
        environment_id: None,
        cursor: None,
        start_line: None,
        line_count: None,
        query: None,
        max_matches: None,
        context_lines: None,
    }
}
async fn flushed(capture: &ArtifactCapture) {
    for _ in 0..1000 {
        if lock(&capture.entry.state).flushed {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("writer did not finish");
}

#[tokio::test]
async fn captures_original_stream_bytes_and_sha256() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    let stdout = "α\r\nmiddle evidence\nlast".as_bytes();
    capture.observe(OutputStream::Stdout, stdout);
    capture.observe(OutputStream::Stderr, b"error\n");
    capture.record_command_status("completed");
    capture.streams_closed();
    flushed(&capture).await;
    let receipt = capture.receipt();
    assert!(receipt.complete);
    let stdout_receipt = receipt.stdout.expect("stdout");
    assert_eq!(
        stdout_receipt.observed_sha256,
        format!("{:x}", Sha256::digest(stdout))
    );
    assert_eq!(stdout_receipt.stored_sha256, stdout_receipt.observed_sha256);
    let mut raw = vec![0; stdout.len()];
    assert_eq!(
        read_at(&capture.entry.files[0], &mut raw, /*offset*/ 0).expect("read raw"),
        stdout.len()
    );
    assert_eq!(raw, stdout);
    let result = store
        .query(
            "thread".into(),
            vec!["local".into()],
            read_request(receipt.artifact_id.expect("id")),
            /*search*/ false,
        )
        .await
        .expect("query");
    assert_eq!(
        result
            .lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>(),
        vec!["α", "middle evidence", "last"]
    );
    assert!(result.complete);
}

#[tokio::test]
async fn artifact_quota_preserves_exact_prefix_and_omission_counts() {
    let (_root, store) = store(
        /*artifact*/ 5, /*session*/ 16384, /*global*/ 65536,
    );
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    capture.observe(OutputStream::Stdout, b"abcdefgh");
    capture.streams_closed();
    capture.record_command_status("completed");
    flushed(&capture).await;
    let receipt = capture.receipt();
    assert_eq!(receipt.artifact_status, "partial");
    assert!(!receipt.complete);
    let output = receipt.stdout.expect("stdout");
    assert_eq!(
        (
            output.observed_bytes,
            output.stored_bytes,
            output.omitted_bytes
        ),
        (8, 5, 3)
    );
    assert_eq!(
        output.stored_sha256,
        format!("{:x}", Sha256::digest(b"abcde"))
    );
}

#[tokio::test]
async fn session_quota_is_shared_by_descendants_without_sharing_read_authority() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 5120, /*global*/ 65536,
    );
    let capture = store
        .begin(
            "parent".into(),
            "shared-session".into(),
            "local".into(),
            "same-call".into(),
        )
        .await
        .expect("capture");
    capture.observe(OutputStream::Stdout, b"private\n");
    capture.streams_closed();
    capture.record_command_status("completed");
    flushed(&capture).await;
    assert!(
        store
            .begin(
                "child".into(),
                "shared-session".into(),
                "local".into(),
                "same-call".into()
            )
            .await
            .is_err()
    );
    let id = capture.receipt().artifact_id.expect("id");
    assert_eq!(
        store
            .query(
                "child".into(),
                vec!["local".into()],
                read_request(id),
                /*search*/ false
            )
            .await
            .err(),
        Some("artifact_owner_mismatch")
    );
}

#[tokio::test]
async fn global_quota_applies_across_store_instances() {
    let (root, first) = store(
        /*artifact*/ 1024, /*session*/ 5120, /*global*/ 5120,
    );
    let _capture = first
        .begin("one".into(), "one".into(), "local".into(), "call".into())
        .await
        .expect("capture");
    let second = Arc::new(OutputArtifactStore::new(
        root.path()
            .canonicalize()
            .expect("canonical temporary root")
            .join("output"),
        first.config.clone(),
    ));
    assert!(
        second
            .begin("two".into(), "two".into(), "local".into(), "call".into())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn repeated_calls_have_distinct_runs_and_environment_authority() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let a = store
        .begin(
            "thread".into(),
            "session".into(),
            "a".into(),
            "same-call".into(),
        )
        .await
        .expect("capture");
    let b = store
        .begin(
            "thread".into(),
            "session".into(),
            "b".into(),
            "same-call".into(),
        )
        .await
        .expect("capture");
    assert_ne!(a.entry.manifest.artifact_id, b.entry.manifest.artifact_id);
    assert_ne!(a.entry.manifest.run_id, b.entry.manifest.run_id);
    a.observe(OutputStream::Stdout, b"private\n");
    a.streams_closed();
    flushed(&a).await;
    assert_eq!(
        store
            .query(
                "thread".into(),
                vec!["b".into()],
                read_request(a.receipt().artifact_id.expect("id")),
                /*search*/ false
            )
            .await
            .err(),
        Some("artifact_owner_mismatch")
    );
}

#[tokio::test]
async fn utf8_across_chunks_and_binary_output_have_explicit_semantics() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let a = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "utf8".into(),
        )
        .await
        .expect("capture");
    let bytes = "中文\n".as_bytes();
    for byte in bytes {
        a.observe(OutputStream::Stdout, &[*byte]);
    }
    a.streams_closed();
    a.record_command_status("completed");
    flushed(&a).await;
    assert!(a.receipt().complete);
    let b = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "binary".into(),
        )
        .await
        .expect("capture");
    b.observe(OutputStream::Stdout, b"binary\0bytes");
    b.streams_closed();
    b.record_command_status("completed");
    flushed(&b).await;
    assert_eq!(b.receipt().reason.as_deref(), Some("unsupported_encoding"));
    assert!(b.receipt().artifact_id.is_none());
}

#[tokio::test]
async fn cancelled_capture_does_not_claim_complete_or_change_command_state() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    capture.observe(OutputStream::Stdout, b"evidence\n");
    capture.record_command_status("cancelled");
    capture.record_command_status("completed");
    capture.streams_closed();
    flushed(&capture).await;
    let receipt = capture.receipt();
    assert_eq!(receipt.command_status, "cancelled");
    assert!(!receipt.complete);
    capture.record_command_status("timed_out");
    capture.record_command_status("completed");
    assert_eq!(capture.receipt().command_status, "timed_out");
}

#[tokio::test]
async fn literal_search_crosses_long_line_fragments_and_limits_serialized_output() {
    let (_root, store) = store(
        /*artifact*/ 32768, /*session*/ 65536, /*global*/ 131072,
    );
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    let text = format!(
        "{}boundary-marker{}\n",
        "x".repeat(2044),
        "\"\t".repeat(5000)
    );
    for chunk in text.as_bytes().chunks(4096) {
        capture.observe(OutputStream::Stdout, chunk);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    capture.streams_closed();
    capture.record_command_status("completed");
    flushed(&capture).await;
    let id = capture.receipt().artifact_id.expect("id");
    let mut request = read_request(id);
    request.query = Some("boundary-marker".into());
    let result = store
        .query(
            "thread".into(),
            vec!["local".into()],
            request,
            /*search*/ true,
        )
        .await
        .expect("search");
    assert!(
        result
            .lines
            .iter()
            .any(|line| line.text.contains("boundary-marker"))
    );
    assert!(serde_json::to_vec(&result).expect("serialize").len() <= MAX_RETURN_BYTES);
    let result = store
        .query(
            "thread".into(),
            vec!["local".into()],
            read_request(id),
            /*search*/ false,
        )
        .await
        .expect("read");
    assert!(serde_json::to_vec(&result).expect("serialize").len() <= MAX_RETURN_BYTES);
}

#[tokio::test]
async fn cursor_cannot_be_forged_or_reused_for_another_query() {
    let (_root, store) = store(
        /*artifact*/ 32768, /*session*/ 65536, /*global*/ 131072,
    );
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    capture.observe(OutputStream::Stdout, b"one\ntwo\nthree\n");
    capture.streams_closed();
    flushed(&capture).await;
    let id = capture.receipt().artifact_id.expect("id");
    let mut request = read_request(id);
    request.line_count = Some(1);
    let result = store
        .query(
            "thread".into(),
            vec!["local".into()],
            request,
            /*search*/ false,
        )
        .await
        .expect("read");
    let cursor = result.next_cursor.expect("cursor");
    let mut forged = read_request(id);
    forged.cursor = Some(Uuid::new_v4());
    assert_eq!(
        store
            .query(
                "thread".into(),
                vec!["local".into()],
                forged,
                /*search*/ false
            )
            .await
            .err(),
        Some("invalid_cursor")
    );
    let mut changed = read_request(id);
    changed.cursor = Some(cursor);
    changed.query = Some("two".into());
    assert_eq!(
        store
            .query(
                "thread".into(),
                vec!["local".into()],
                changed,
                /*search*/ true
            )
            .await
            .err(),
        Some("invalid_cursor")
    );
    let mut valid = read_request(id);
    valid.cursor = Some(cursor);
    valid.line_count = Some(1);
    let result = store
        .query(
            "thread".into(),
            vec!["local".into()],
            valid,
            /*search*/ false,
        )
        .await
        .expect("read");
    assert_eq!(result.lines[0].text, "two");
}

#[tokio::test]
async fn expired_objects_refuse_reads_and_cleanup_respects_live_capture() {
    let (root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let mut config = store.config.clone();
    config.ttl_seconds = 1;
    let store = Arc::new(OutputArtifactStore::new(
        root.path()
            .canonicalize()
            .expect("canonical temporary root")
            .join("output"),
        config,
    ));
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    capture.observe(OutputStream::Stdout, b"evidence\n");
    capture.streams_closed();
    flushed(&capture).await;
    let id = capture.receipt().artifact_id.expect("id");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        store
            .query(
                "thread".into(),
                vec!["local".into()],
                read_request(id),
                /*search*/ false
            )
            .await
            .err(),
        Some("artifact_expired")
    );
    let _new = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "new".into(),
        )
        .await
        .expect("new capture");
    assert!(store.path(id, "stdout").exists());
    drop(capture);
    let _next = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "next".into(),
        )
        .await
        .expect("next capture");
    assert!(!store.path(id, "stdout").exists());
}

#[test]
fn arbitrary_paths_and_unknown_arguments_are_not_accepted() {
    for id in ["../secret", "/etc/passwd", "C:\\secret"] {
        let args = serde_json::json!({"artifact_id": id, "stream": "stdout"});
        assert!(serde_json::from_value::<QueryRequest>(args).is_err());
    }
    assert!(
        serde_json::from_value::<QueryRequest>(serde_json::json!({
            "artifact_id": Uuid::new_v4(), "stream": "stdout", "path": "/private"
        }))
        .is_err()
    );
}

#[test]
#[ignore = "child process fixture invoked explicitly by the pipe integration test"]
fn pipe_output_fixture() {
    let Ok(count_path) = std::env::var("CODEX_OUTPUT_FIXTURE_COUNT") else {
        return;
    };
    let mut count = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(count_path)
        .expect("fixture must execute exactly once");
    count.write_all(b"one").expect("execution count");
    let mut stdout = std::io::stdout().lock();
    let mut stderr = std::io::stderr().lock();
    stderr.write_all(b"stderr-evidence\n").expect("stderr");
    let block = b"x\n".repeat(4096);
    let delay = std::env::var("CODEX_OUTPUT_FIXTURE_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(2);
    for index in 0..200 {
        if index == 100 {
            stdout
                .write_all(b"unique-middle-evidence\n")
                .expect("marker");
        }
        stdout.write_all(&block).expect("output");
        std::thread::sleep(Duration::from_millis(delay));
    }
    stdout.flush().expect("flush");
    std::process::exit(17);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_pipe_captures_evidence_before_one_mib_head_tail_loss() {
    let (root, store) = store(4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024);
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    let executable = std::env::current_exe().expect("test executable");
    let args = vec![
        "--ignored".into(),
        "--exact".into(),
        "unified_exec::output_artifact::tests::pipe_output_fixture".into(),
        "--nocapture".into(),
    ];
    let mut environment: HashMap<String, String> = std::env::vars().collect();
    environment.insert(
        "CODEX_OUTPUT_FIXTURE_COUNT".into(),
        root.path().join("count").to_string_lossy().into_owned(),
    );
    let spawned = codex_utils_pty::spawn_pipe_process_no_stdin(
        executable.as_os_str(),
        &args,
        root.path(),
        &environment,
        &None,
        &[],
    )
    .await
    .expect("real pipe");
    let process = super::super::process::UnifiedExecProcess::from_spawned_with_capture(
        spawned,
        codex_sandboxing::SandboxType::None,
        Box::new(super::super::NoopSpawnLifecycle),
        Some(Arc::clone(&capture)),
        /*fallback*/ None,
    )
    .await
    .expect("managed pipe");
    for _ in 0..1000 {
        if process.has_exited()
            && process
                .output_handles()
                .output_closed
                .load(Ordering::Acquire)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    flushed(&capture).await;
    assert_eq!(process.exit_code(), Some(17));
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("execution count"),
        b"one"
    );
    let bounded = process
        .output_handles()
        .output_buffer
        .lock()
        .await
        .transcript
        .to_bytes();
    assert!(!String::from_utf8_lossy(&bounded).contains("unique-middle-evidence"));
    let receipt = capture.receipt();
    assert!(receipt.complete, "{receipt:?}");
    let id = receipt.artifact_id.expect("id");
    let mut cursor = None;
    let mut found = false;
    for _ in 0..20 {
        let mut request = read_request(id);
        request.query = Some("unique-middle-evidence".into());
        request.cursor = cursor;
        let result = store
            .query(
                "thread".into(),
                vec!["local".into()],
                request,
                /*search*/ true,
            )
            .await
            .expect("search original pipe");
        found |= result
            .lines
            .iter()
            .any(|line| line.text == "unique-middle-evidence");
        cursor = result.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(found);
    let stderr = receipt.stderr.expect("stderr receipt");
    assert_eq!(
        stderr.stored_sha256,
        format!("{:x}", Sha256::digest(b"stderr-evidence\n"))
    );
    let stdout = receipt.stdout.expect("stdout receipt");
    let mut raw = vec![0; stdout.stored_bytes as usize];
    assert_eq!(
        read_at(&capture.entry.files[0], &mut raw, /*offset*/ 0).expect("raw output"),
        raw.len()
    );
    assert_eq!(stdout.stored_sha256, format!("{:x}", Sha256::digest(&raw)));
    assert_eq!(stdout.observed_sha256, stdout.stored_sha256);
    let logical_bytes: u64 = std::fs::read_dir(&store.root)
        .expect("cache inventory")
        .map(|entry| entry.expect("entry").metadata().expect("metadata").len())
        .sum();
    assert!(logical_bytes <= capture.entry.manifest.reserved_bytes);
    writeln!(
        std::io::stdout(),
        "resource: logical_cache_bytes={logical_bytes} reserved_bytes={} raw_stored_bytes={}",
        capture.entry.manifest.reserved_bytes,
        stdout.stored_bytes + stderr.stored_bytes
    )
    .expect("resource evidence");
}

#[tokio::test]
async fn search_retains_context_after_match_limit_and_across_scan_pages() {
    for padding in [0, (MAX_SCAN_BYTES - b"before\nneedle\n".len()) / 2] {
        let (_root, store) = store(1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024);
        let capture = store
            .begin(
                "thread".into(),
                "session".into(),
                "local".into(),
                "call".into(),
            )
            .await
            .expect("capture");
        let text = format!("{}before\nneedle\nafter\n", "x\n".repeat(padding));
        for bytes in text.as_bytes().chunks(MAX_CHUNK_BYTES) {
            capture.observe(OutputStream::Stdout, bytes);
            while capture.queue_bytes.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        capture.streams_closed();
        capture.record_command_status("completed");
        flushed(&capture).await;
        let id = capture.receipt().artifact_id.expect("id");
        let mut cursor = None;
        let mut evidence = Vec::new();
        for _ in 0..4 {
            let mut request = read_request(id);
            request.query = Some("needle".into());
            request.max_matches = Some(1);
            request.context_lines = Some(1);
            request.cursor = cursor;
            let response = store
                .query(
                    "thread".into(),
                    vec!["local".into()],
                    request,
                    /*search*/ true,
                )
                .await
                .expect("search");
            evidence.extend(response.lines.into_iter().map(|line| line.text));
            cursor = response.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(evidence, ["before", "needle", "after"]);
    }
}

#[tokio::test]
async fn expired_unleased_object_reclaims_exact_global_quota_on_first_admission() {
    let root = tempfile::tempdir().expect("temporary cache");
    let config = RecoverableExecOutputConfig {
        enabled: true,
        artifact_max_bytes: 1024,
        session_max_bytes: 5120,
        global_max_bytes: 5120,
        ttl_seconds: 1,
        ..Default::default()
    };
    let store = Arc::new(OutputArtifactStore::new(
        root.path()
            .canonicalize()
            .expect("canonical temporary root")
            .join("output"),
        config,
    ));
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "old".into(),
        )
        .await
        .expect("first admission");
    capture.observe(OutputStream::Stdout, b"evidence\n");
    capture.streams_closed();
    capture.record_command_status("completed");
    flushed(&capture).await;
    drop(capture);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let fresh = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "fresh".into(),
        )
        .await
        .expect("first admission after expiry");
    fresh.streams_closed();
    flushed(&fresh).await;
}

#[tokio::test]
async fn live_read_does_not_move_the_writer_file_position() {
    let (_root, store) = store(1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024);
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .expect("capture");
    let before = "a\n".repeat(200000);
    for chunk in before.as_bytes().chunks(MAX_CHUNK_BYTES) {
        capture.observe(OutputStream::Stdout, chunk);
        while capture.queue_bytes.load(Ordering::Acquire) != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let id = capture.receipt().artifact_id.expect("running id");
    store
        .query(
            "thread".into(),
            vec!["local".into()],
            read_request(id),
            /*search*/ false,
        )
        .await
        .expect("live read");
    capture.observe(OutputStream::Stdout, b"after-live-read\n");
    capture.streams_closed();
    capture.record_command_status("completed");
    flushed(&capture).await;
    let expected = format!("{before}after-live-read\n");
    let mut actual = vec![0; expected.len()];
    assert_eq!(
        read_at(&capture.entry.files[0], &mut actual, /*offset*/ 0).expect("raw read"),
        expected.len()
    );
    assert_eq!(actual, expected.as_bytes());
    assert_eq!(
        capture.receipt().stdout.expect("stdout").stored_sha256,
        format!("{:x}", Sha256::digest(expected.as_bytes()))
    );
}

fn paused_capture(
    store: &Arc<OutputArtifactStore>,
) -> (Arc<ArtifactCapture>, mpsc::Receiver<QueuedChunk>) {
    let entry = store
        .allocate(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .expect("allocation");
    lock(&store.entries).insert(entry.manifest.artifact_id, Arc::clone(&entry));
    let (sender, receiver) = mpsc::channel(32);
    let capture = Arc::new(ArtifactCapture {
        entry,
        sender: Mutex::new(Some(sender)),
        queue_bytes: Arc::clone(&store.queue_bytes),
        max_bytes: store.config.artifact_max_bytes,
    });
    (capture, receiver)
}
fn start_paused_writer(capture: &Arc<ArtifactCapture>, receiver: mpsc::Receiver<QueuedChunk>) {
    OutputArtifactStore::spawn_writer(
        Arc::clone(&capture.entry),
        [
            capture.entry.files[0].try_clone().expect("stdout"),
            capture.entry.files[1].try_clone().expect("stderr"),
        ],
        receiver,
    );
}
async fn native_fixture(
    root: &std::path::Path,
    capture: Option<Arc<ArtifactCapture>>,
    fallback: Option<ArtifactReceipt>,
) -> super::super::process::UnifiedExecProcess {
    let executable = std::env::current_exe().expect("test executable");
    let args = vec![
        "--ignored".into(),
        "--exact".into(),
        "unified_exec::output_artifact::tests::pipe_output_fixture".into(),
        "--nocapture".into(),
    ];
    let mut environment: HashMap<String, String> = std::env::vars().collect();
    environment.insert("CODEX_OUTPUT_FIXTURE_DELAY_MS".into(), "5".into());
    environment.insert(
        "CODEX_OUTPUT_FIXTURE_COUNT".into(),
        root.join("count").to_string_lossy().into_owned(),
    );
    let spawned = codex_utils_pty::spawn_pipe_process_no_stdin(
        executable.as_os_str(),
        &args,
        root,
        &environment,
        &None,
        &[],
    )
    .await
    .expect("real child");
    super::super::process::UnifiedExecProcess::from_spawned_with_capture(
        spawned,
        codex_sandboxing::SandboxType::None,
        Box::new(super::super::NoopSpawnLifecycle),
        capture,
        fallback,
    )
    .await
    .expect("managed child")
}
async fn exited(process: &super::super::process::UnifiedExecProcess) {
    for _ in 0..2000 {
        if process.has_exited()
            && process
                .output_handles()
                .output_closed
                .load(Ordering::Acquire)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("native child did not exit and drain");
}

#[tokio::test]
async fn pending_writes_keep_a_valid_reference_after_command_completion() {
    let (_root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    let (capture, receiver) = paused_capture(&store);
    capture.observe(OutputStream::Stdout, b"pending evidence\n");
    capture.record_command_status("completed");
    capture.streams_closed();
    let receipt = capture.receipt();
    let id = receipt.artifact_id.expect("reference before first write");
    assert_eq!(receipt.artifact_status, "recording");
    assert!(!receipt.complete);
    let empty = store
        .query(
            "thread".into(),
            vec!["local".into()],
            read_request(id),
            /*search*/ false,
        )
        .await
        .expect("valid pending lookup");
    assert_eq!(empty.snapshot_bytes, 0);
    start_paused_writer(&capture, receiver);
    flushed(&capture).await;
    let ready = store
        .query(
            "thread".into(),
            vec!["local".into()],
            read_request(id),
            /*search*/ false,
        )
        .await
        .expect("same reference after write");
    assert!(ready.recovery.complete);
    assert_eq!(ready.lines[0].text, "pending evidence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pipe_queue_failure_preserves_exit_and_single_execution() {
    let (root, store) = store(4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024);
    let (capture, receiver) = paused_capture(&store);
    let process = native_fixture(
        root.path(),
        Some(Arc::clone(&capture)),
        /*fallback*/ None,
    )
    .await;
    exited(&process).await;
    assert_eq!(process.exit_code(), Some(17));
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("count"),
        b"one"
    );
    assert_eq!(capture.receipt().reason.as_deref(), Some("queue_full"));
    assert!(store.queue_bytes.load(Ordering::Acquire) <= MAX_QUEUE_BYTES);
    start_paused_writer(&capture, receiver);
    flushed(&capture).await;
    assert_eq!(store.queue_bytes.load(Ordering::Acquire), 0);
    let receipt = capture.receipt();
    assert_eq!(receipt.command_status, "completed");
    assert!(!receipt.complete);
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        let state = lock(&capture.entry.state);
        let s = &state.streams[stream.index()];
        let mut raw = vec![0; s.stored as usize];
        assert_eq!(
            read_at(
                &capture.entry.files[stream.index()],
                &mut raw,
                /*offset*/ 0
            )
            .expect("prefix"),
            raw.len()
        );
        assert_eq!(
            format!("{:x}", s.stored_hash.clone().finalize()),
            format!("{:x}", Sha256::digest(&raw))
        );
        assert!(s.stored <= s.observed);
    }
    assert!(receipt.stdout.expect("stdout").omitted_bytes > 1024 * 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pipe_write_failure_preserves_exit_and_reports_unstored_bytes() {
    let (root, store) = store(4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024);
    let (capture, receiver) = paused_capture(&store);
    let readonly = File::open(store.path(capture.entry.manifest.artifact_id, "stdout"))
        .expect("read-only OS handle");
    OutputArtifactStore::spawn_writer(
        Arc::clone(&capture.entry),
        [
            readonly,
            capture.entry.files[1].try_clone().expect("stderr"),
        ],
        receiver,
    );
    let process = native_fixture(
        root.path(),
        Some(Arc::clone(&capture)),
        /*fallback*/ None,
    )
    .await;
    exited(&process).await;
    flushed(&capture).await;
    assert_eq!(process.exit_code(), Some(17));
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("count"),
        b"one"
    );
    let receipt = capture.receipt();
    assert_eq!(receipt.reason.as_deref(), Some("write_failed"));
    assert_eq!(receipt.command_status, "completed");
    assert!(!receipt.complete);
    let stdout = receipt.stdout.expect("stdout");
    assert_eq!(stdout.stored_bytes, 0);
    assert_eq!(stdout.omitted_bytes, stdout.observed_bytes);
    assert!(stdout.observed_bytes > 1024 * 1024);
    assert_eq!(store.queue_bytes.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn aggregate_queued_bytes_never_exceed_one_mib_and_drop_releases_them() {
    let (_root, store) = store(1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024);
    let mut captures = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..5 {
        let (capture, receiver) = paused_capture(&store);
        for _ in 0..32 {
            capture.observe(OutputStream::Stdout, &[b'x'; MAX_CHUNK_BYTES]);
        }
        assert!(store.queue_bytes.load(Ordering::Acquire) <= MAX_QUEUE_BYTES);
        captures.push(capture);
        receivers.push(receiver);
    }
    assert_eq!(store.queue_bytes.load(Ordering::Acquire), MAX_QUEUE_BYTES);
    assert_eq!(captures[4].receipt().reason.as_deref(), Some("queue_full"));
    writeln!(
        std::io::stdout(),
        "resource: aggregate_queue_bytes={} captures={}",
        store.queue_bytes.load(Ordering::Acquire),
        captures.len()
    )
    .expect("resource evidence");
    drop(captures);
    drop(receivers);
    assert_eq!(store.queue_bytes.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pipe_timeout_stops_writer_and_keeps_hashes_nonfinal() {
    for enabled in [false, true] {
        let (root, store) = store(4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024);
        let capture = if enabled {
            Some(
                store
                    .begin(
                        "thread".into(),
                        "session".into(),
                        "local".into(),
                        "call".into(),
                    )
                    .await
                    .expect("capture"),
            )
        } else {
            None
        };
        let fallback = (!enabled).then(|| ArtifactReceipt::unavailable("unsupported_backend"));
        let process = native_fixture(root.path(), capture.clone(), fallback).await;
        assert!(!process.has_exited());
        process.mark_timed_out();
        process.terminate_confirmed().await.expect("terminate");
        if let Some(capture) = &capture {
            flushed(capture).await;
        }
        let receipt = process.artifact_receipt().expect("receipt");
        assert_eq!(process.exit_code(), Some(124));
        assert_eq!(receipt.command_status, "timed_out");
        assert!(!receipt.complete);
        if enabled {
            assert!(!receipt.stdout.expect("stdout").hash_final);
        } else {
            assert!(receipt.artifact_id.is_none());
            assert_eq!(receipt.reason.as_deref(), Some("unsupported_backend"));
        }
        assert_eq!(store.queue_bytes.load(Ordering::Acquire), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pipe_cancellation_stops_writer_and_keeps_hashes_nonfinal() {
    for enabled in [false, true] {
        let (root, store) = store(4 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024);
        let capture = if enabled {
            Some(
                store
                    .begin(
                        "thread".into(),
                        "session".into(),
                        "local".into(),
                        "call".into(),
                    )
                    .await
                    .expect("capture"),
            )
        } else {
            None
        };
        let fallback = (!enabled).then(|| ArtifactReceipt::unavailable("unsupported_backend"));
        let process = native_fixture(root.path(), capture.clone(), fallback).await;
        assert!(!process.has_exited());
        process.terminate_confirmed().await.expect("terminate");
        if let Some(capture) = &capture {
            flushed(capture).await;
        }
        let receipt = process.artifact_receipt().expect("receipt");
        assert_ne!(process.exit_code(), Some(124));
        assert!(process.has_exited());
        assert_eq!(receipt.command_status, "cancelled");
        assert!(!receipt.complete);
        if enabled {
            assert!(!receipt.stdout.expect("stdout").hash_final);
        } else {
            assert!(receipt.artifact_id.is_none());
            assert_eq!(receipt.reason.as_deref(), Some("unsupported_backend"));
        }
        assert_eq!(store.queue_bytes.load(Ordering::Acquire), 0);
    }
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_permission_fallback_keeps_original_command_exit() {
    let (root, store) = store(
        /*artifact*/ 1024, /*session*/ 16384, /*global*/ 65536,
    );
    create_private_local_cache_directory(&store.root).expect("cache");
    let path = store.root.join("quota.lock");
    drop(open_local_cache_file_no_follow(&path, /*create_new*/ true).expect("lock"));
    let original_permissions = std::fs::metadata(&path).expect("metadata").permissions();
    let mut permissions = original_permissions.clone();
    permissions.set_readonly(true);
    std::fs::set_permissions(&path, permissions).expect("readonly attribute");
    let error = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "call".into(),
        )
        .await
        .err()
        .expect("permission failure");
    std::fs::set_permissions(&path, original_permissions).expect("restore fixture attribute");
    assert_eq!(error, "storage_unavailable");
    let process = native_fixture(
        root.path(),
        /*capture*/ None,
        Some(ArtifactReceipt::unavailable(error)),
    )
    .await;
    exited(&process).await;
    assert_eq!(process.exit_code(), Some(17));
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("count"),
        b"one"
    );
    let receipt = process.artifact_receipt().expect("fallback");
    assert_eq!(receipt.command_status, "completed");
    assert!(receipt.artifact_id.is_none());
    assert_eq!(receipt.reason.as_deref(), Some("storage_unavailable"));
}

#[test]
#[ignore = "native child fixture invoked by cross-process quota test"]
fn quota_admission_fixture() {
    let path = std::env::var_os("CODEX_ARTIFACT_ROOT").expect("fixture root");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let admitted = runtime.block_on(async {
        let config = RecoverableExecOutputConfig {
            enabled: true,
            artifact_max_bytes: 1024,
            session_max_bytes: 5120,
            global_max_bytes: 5120,
            ttl_seconds: 1,
            ..Default::default()
        };
        let store = Arc::new(OutputArtifactStore::new(path.into(), config));
        let Ok(capture) = store
            .begin(
                "child-thread".into(),
                "other-session".into(),
                "local".into(),
                "child".into(),
            )
            .await
        else {
            return false;
        };
        capture.streams_closed();
        flushed(&capture).await;
        true
    });
    writeln!(
        std::io::stdout(),
        "admission:{}",
        if admitted { "accepted" } else { "denied" }
    )
    .expect("admission evidence");
    std::process::exit(if admitted { 0 } else { 23 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_process_quota_and_expired_active_lease_are_enforced() {
    let root = tempfile::tempdir().expect("fixture root");
    let cache = root
        .path()
        .canonicalize()
        .expect("canonical temporary root")
        .join("output");
    let config = RecoverableExecOutputConfig {
        enabled: true,
        artifact_max_bytes: 1024,
        session_max_bytes: 5120,
        global_max_bytes: 5120,
        ttl_seconds: 1,
        ..Default::default()
    };
    let store = Arc::new(OutputArtifactStore::new(cache.clone(), config));
    let capture = store
        .begin(
            "thread".into(),
            "session".into(),
            "local".into(),
            "parent".into(),
        )
        .await
        .expect("parent");
    capture.streams_closed();
    flushed(&capture).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let run_child = |cache: std::path::PathBuf| {
        std::process::Command::new(std::env::current_exe().expect("executable"))
            .args([
                "--ignored",
                "--exact",
                "unified_exec::output_artifact::tests::quota_admission_fixture",
                "--nocapture",
            ])
            .env("CODEX_ARTIFACT_ROOT", cache)
            .output()
            .expect("real quota child")
    };
    let first_cache = cache.clone();
    let denied = tokio::task::spawn_blocking(move || run_child(first_cache))
        .await
        .expect("child task");
    assert_eq!(denied.status.code(), Some(23));
    assert!(String::from_utf8_lossy(&denied.stdout).contains("admission:denied"));
    drop(capture);
    drop(store);
    let accepted = tokio::task::spawn_blocking(move || run_child(cache))
        .await
        .expect("child task");
    assert_eq!(
        accepted.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&accepted.stdout)
    );
    assert!(String::from_utf8_lossy(&accepted.stdout).contains("admission:accepted"));
}

#[test]
fn storage_full_boundary_preserves_the_exact_successful_prefix() {
    struct FullDisk {
        stored: Vec<u8>,
    }
    impl Write for FullDisk {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let available = 5_usize.saturating_sub(self.stored.len());
            if available == 0 {
                return Err(io::Error::from(io::ErrorKind::StorageFull));
            }
            let count = bytes.len().min(available);
            self.stored.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut sink = FullDisk { stored: Vec::new() };
    assert_eq!(write_prefix(&mut sink, b"original bytes"), 5);
    assert_eq!(sink.stored, b"origi");
}
