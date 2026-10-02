use super::*;
use crate::context_manager::ContextManager;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use crate::session::tests::make_session_and_context_with_auth_and_config_and_rx;
use crate::session::tests::mcp_config_for_test;
use crate::session::turn_context::TurnContext;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolInvocation;
use crate::tools::registry::ToolRegistry;
use crate::tools::spec_plan::build_core_tool_registry;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_features::Feature;
use codex_protocol::models::ResponseItem;
use codex_utils_output_truncation::TruncationPolicy;
use std::sync::Arc;
use tokio::sync::Mutex;

async fn setup(
    enabled: bool,
    cache: std::path::PathBuf,
) -> (Arc<Session>, Arc<TurnContext>, ToolRegistry) {
    let (mut session, turn, events) = make_session_and_context_with_auth_and_config_and_rx(
        codex_login::CodexAuth::from_api_key("Test API Key"),
        Vec::new(),
        |config| {
            config.recoverable_exec_output.enabled = enabled;
            config
                .features
                .enable(Feature::UnifiedExec)
                .expect("unified exec feature");
        },
    )
    .await;
    Arc::get_mut(&mut session)
        .expect("unique test session")
        .services
        .unified_exec_manager = crate::unified_exec::UnifiedExecProcessManager::default()
        .with_output_artifacts(cache.clone(), turn.config.recoverable_exec_output.clone());
    #[cfg(windows)]
    {
        // Use the existing command-approval protocol for this exact native fixture.
        // The default Windows test profile requires approval without a sandbox backend.
        *session.active_turn.lock().await = Some(crate::state::ActiveTurn::default());
        let args = native_command(cache.parent().expect("fixture root"));
        let shell = crate::shell::get_shell_by_model_provided_path(&std::path::PathBuf::from(
            args["shell"].as_str().expect("shell"),
        ));
        let expected = shell.derive_exec_args(
            args["cmd"].as_str().expect("command"),
            /*use_login_shell*/ false,
        );
        let weak = Arc::downgrade(&session);
        tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                if let codex_protocol::protocol::EventMsg::ExecApprovalRequest(request) = event.msg
                {
                    assert_eq!(
                        request.command, expected,
                        "only the exact fixture receives test approval"
                    );
                    let Some(session) = weak.upgrade() else {
                        break;
                    };
                    session
                        .notify_approval(
                            request.approval_id.as_deref().unwrap_or(&request.call_id),
                            codex_protocol::protocol::ReviewDecision::Approved,
                        )
                        .await;
                }
            }
        });
    }
    #[cfg(not(windows))]
    drop(events);
    let mcp = codex_mcp::McpBinding::empty(mcp_config_for_test(&turn.config));
    let registry = build_core_tool_registry(
        &turn,
        turn.model_info(),
        &turn.initial_environments,
        &mcp,
        /*tool_suggest_candidates*/ None,
        /*wait_for_environment_tool_config*/ None,
    );
    (session, turn, registry)
}
async fn invoke(
    registry: &ToolRegistry,
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    name: &str,
    arguments: Value,
) -> Result<crate::tools::registry::AnyToolResult, FunctionCallError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        registry.dispatch_any_with_state(
            ToolInvocation {
                session: Arc::clone(session),
                turn: Arc::clone(turn),
                step_context: StepContext::for_test(Arc::clone(turn)),
                cancellation_token: tokio_util::sync::CancellationToken::new(),
                tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
                call_id: format!("{name}-call"),
                tool_name: ToolName::plain(name),
                source: ToolCallSource::Direct,
                payload: ToolPayload::Function {
                    arguments: arguments.to_string(),
                },
            },
            /*call_state*/ None,
        ),
    )
    .await
    .expect("fixture tool completes within 20 seconds")
}
fn response_text(envelope: &codex_history::ResponseItemEnvelope) -> String {
    let ResponseItem::FunctionCallOutput { output, .. } = &envelope.item else {
        panic!("function output");
    };
    output.body.to_text().expect("text")
}

#[tokio::test]
async fn recovery_tool_registration_defaults_off_and_can_be_enabled() {
    let root = tempfile::tempdir().expect("root");
    for enabled in [false, true] {
        let cache = root.path().join(if enabled { "on" } else { "off" });
        let (_session, _turn, registry) = setup(enabled, cache.clone()).await;
        for name in ["read_exec_output", "search_exec_output"] {
            assert_eq!(registry.tool(&ToolName::plain(name)).is_some(), enabled);
        }
        assert!(!cache.exists(), "registration does not create cache");
    }
}

#[cfg(windows)]
fn native_command(root: &std::path::Path) -> Value {
    let executable = std::env::current_exe()
        .expect("test executable")
        .to_string_lossy()
        .replace('\'', "''");
    let count = root.join("count").to_string_lossy().replace('\'', "''");
    let shell = std::path::PathBuf::from(std::env::var_os("SystemRoot").expect("Windows root"))
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
    serde_json::json!({
        "cmd": format!("$env:CODEX_OUTPUT_FIXTURE_COUNT = '{count}'; & '{executable}' --ignored --exact unified_exec::output_artifact::tests::pipe_output_fixture --nocapture; exit $LASTEXITCODE"),
        "shell": shell, "tty": false, "login": false, "yield_time_ms": 1000,
        "workdir": root.to_string_lossy(), "max_output_tokens": 0,
    })
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_exec_registry_lookup_and_history_preserve_evidence_with_tiny_budgets() {
    let root = tempfile::tempdir().expect("root");
    let (session, turn, registry) = setup(/*enabled*/ true, root.path().join("cache")).await;
    let result = invoke(
        &registry,
        &session,
        &turn,
        "exec_command",
        native_command(root.path()),
    )
    .await
    .expect("real exec handler");
    let mut envelope = result.into_response();
    let mut value: Value = serde_json::from_str(&response_text(&envelope)).expect("exec envelope");
    for _ in 0..10 {
        let Some(id) = value["session_id"].as_i64() else {
            break;
        };
        envelope = invoke(&registry, &session, &turn, "write_stdin", serde_json::json!({"session_id": id,"chars":"","yield_time_ms":1000,"max_output_tokens":0})).await.expect("real polling").into_response();
        value = serde_json::from_str(&response_text(&envelope)).expect("poll envelope");
    }
    assert_eq!(value["exit_code"], 17, "{value}");
    assert!(value["session_id"].is_null());
    let id = value["recovery"]["artifact_id"]
        .as_str()
        .expect("recoverable id")
        .to_owned();
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("count"),
        b"one"
    );
    for budget in [0, 1] {
        let mut history = ContextManager::default();
        let mut items = vec![envelope.clone()];
        history.record_annotated_items(&mut items, TruncationPolicy::Tokens(budget));
        let retained = &history.annotated_items()[0];
        let parsed: Value =
            serde_json::from_str(&response_text(retained)).expect("history JSON stays valid");
        assert_eq!(parsed["recovery"]["artifact_id"], id);
        assert_eq!(parsed["exit_code"], 17);
    }
    let mut cursor = Value::Null;
    let mut found = false;
    for _ in 0..20 {
        let mut args = serde_json::json!({"artifact_id":id,"stream":"stdout","query":"unique-middle-evidence"});
        if !cursor.is_null() {
            args["cursor"] = cursor.clone();
        }
        let result = invoke(&registry, &session, &turn, "search_exec_output", args)
            .await
            .expect("registered search");
        let typed = result.result.code_mode_result(&result.payload);
        let response = result.into_response();
        let text = response_text(&response);
        assert!(text.len() <= 8000);
        let parsed: Value = serde_json::from_str(&text).expect("query JSON");
        assert_eq!(parsed, typed);
        for budget in [0, 1] {
            let mut history = ContextManager::default();
            let mut items = vec![response.clone()];
            history.record_annotated_items(&mut items, TruncationPolicy::Tokens(budget));
            let retained: Value =
                serde_json::from_str(&response_text(&history.annotated_items()[0]))
                    .expect("query history JSON");
            assert_eq!(retained, parsed);
        }
        found |= parsed["lines"]
            .as_array()
            .expect("lines")
            .iter()
            .any(|line| {
                line["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("unique-middle-evidence"))
            });
        cursor = parsed["next_cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    assert!(found, "original middle evidence through registered tools");
    let read = invoke(
        &registry,
        &session,
        &turn,
        "read_exec_output",
        serde_json::json!({"artifact_id":id,"stream":"stderr","line_count":5}),
    )
    .await
    .expect("registered read")
    .into_response();
    let stderr: Value = serde_json::from_str(&response_text(&read)).expect("read JSON");
    assert!(
        stderr["lines"]
            .as_array()
            .expect("lines")
            .iter()
            .any(|line| line["text"]
                .as_str()
                .is_some_and(|text| text.contains("stderr-evidence")))
    );
    let (mut other_session, other_turn) = make_session_and_context().await;
    assert_ne!(other_session.thread_id(), session.thread_id());
    other_session.services.unified_exec_manager.output_artifacts = session
        .services
        .unified_exec_manager
        .output_artifacts
        .clone();
    let wrong_owner = invoke(
        &registry,
        &Arc::new(other_session),
        &Arc::new(other_turn),
        "read_exec_output",
        serde_json::json!({"artifact_id":id,"stream":"stdout"}),
    )
    .await;
    assert!(
        matches!(wrong_owner, Err(FunctionCallError::RespondToModel(message)) if message.contains("artifact_owner_mismatch"))
    );
    let wrong_environment = invoke(
        &registry,
        &session,
        &turn,
        "read_exec_output",
        serde_json::json!({"artifact_id":id,"stream":"stdout","environment_id":"other"}),
    )
    .await;
    assert!(
        matches!(wrong_environment, Err(FunctionCallError::RespondToModel(message)) if message.contains("artifact_owner_mismatch"))
    );
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_exec_default_off_preserves_old_result_and_creates_no_cache() {
    let root = tempfile::tempdir().expect("root");
    let cache = root.path().join("cache");
    let (session, turn, registry) = setup(/*enabled*/ false, cache.clone()).await;
    let mut args = native_command(root.path());
    args["max_output_tokens"] = 100.into();
    let mut result = invoke(&registry, &session, &turn, "exec_command", args)
        .await
        .expect("original exec");
    let mut value = result.result.code_mode_result(&result.payload);
    for _ in 0..10 {
        let Some(id) = value["session_id"].as_i64() else {
            break;
        };
        result = invoke(&registry, &session, &turn, "write_stdin", serde_json::json!({"session_id":id,"chars":"","yield_time_ms":1000,"max_output_tokens":100})).await.expect("original polling");
        value = result.result.code_mode_result(&result.payload);
    }
    assert_eq!(value["exit_code"], 17);

    assert!(value.get("recovery").is_none());
    let text = response_text(&result.into_response());
    assert!(text.contains("Process exited with code 17"));
    assert!(!text.contains("\"artifact_id\""));
    assert!(!cache.exists());
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("single execution"),
        b"one"
    );
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pty_registry_reports_unsupported_and_keeps_original_exit() {
    let root = tempfile::tempdir().expect("root");
    let cache = root.path().join("cache");
    let (session, turn, registry) = setup(/*enabled*/ true, cache.clone()).await;
    let mut args = native_command(root.path());
    args["tty"] = true.into();
    let mut result = invoke(&registry, &session, &turn, "exec_command", args)
        .await
        .expect("real PTY fallback");
    let mut value = result.result.code_mode_result(&result.payload);
    for _ in 0..10 {
        let Some(id) = value["session_id"].as_i64() else {
            break;
        };
        result = invoke(&registry, &session, &turn, "write_stdin", serde_json::json!({"session_id":id,"chars":"","yield_time_ms":1000,"max_output_tokens":0})).await.expect("PTY polling");
        value = result.result.code_mode_result(&result.payload);
    }
    assert_eq!(value["exit_code"], 17, "{value}");
    assert_eq!(value["recovery"]["command_status"], "completed");
    assert_eq!(value["recovery"]["reason"], "unsupported_backend");
    assert!(value["recovery"].get("artifact_id").is_none());
    assert!(!cache.exists());
    assert_eq!(
        std::fs::read(root.path().join("count")).expect("single execution"),
        b"one"
    );
}
