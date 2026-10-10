use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::output_artifact::QueryRequest;
use codex_protocol::models::ResponseInputItem;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) struct OutputArtifactHandler {
    search: bool,
}
impl OutputArtifactHandler {
    pub(crate) fn read() -> Self {
        Self { search: false }
    }
    pub(crate) fn search() -> Self {
        Self { search: true }
    }
    fn name(&self) -> &'static str {
        if self.search {
            "search_exec_output"
        } else {
            "read_exec_output"
        }
    }
}
struct ArtifactQueryOutput(Value);
impl ToolOutput for ArtifactQueryOutput {
    fn log_output(&self) -> String {
        "bounded exec output lookup".into()
    }
    fn success_for_logging(&self) -> bool {
        true
    }
    fn fallback_token_limit_override(&self) -> Option<usize> {
        Some(self.0.to_string().len().div_ceil(4))
    }
    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        FunctionToolOutput::from_text(self.0.to_string(), Some(true))
            .to_response_item(call_id, payload)
    }
    fn code_mode_result(&self, _payload: &ToolPayload) -> Value {
        self.0.clone()
    }
}
impl ToolExecutor<ToolInvocation> for OutputArtifactHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(self.name())
    }
    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }
    fn spec(&self) -> ToolSpec {
        let mut properties = BTreeMap::from([
            (
                "artifact_id".into(),
                JsonSchema::string(Some("Opaque ID from this thread's exec receipt.".into())),
            ),
            (
                "stream".into(),
                JsonSchema::string(Some("stdout or stderr.".into())),
            ),
            (
                "environment_id".into(),
                JsonSchema::string(Some(
                    "Optional selected environment; must own the artifact.".into(),
                )),
            ),
            (
                "cursor".into(),
                JsonSchema::string(Some(
                    "Opaque next_cursor from the previous lookup. Reuse the same query and stream."
                        .into(),
                )),
            ),
        ]);
        if self.search {
            properties.insert(
                "query".into(),
                JsonSchema::string(Some(
                    "Literal single-line match, 1..512 UTF-8 bytes.".into(),
                )),
            );
            properties.insert(
                "max_matches".into(),
                JsonSchema::number(Some("1..50 matches; default 20.".into())),
            );
            properties.insert(
                "context_lines".into(),
                JsonSchema::number(Some("0..5 surrounding lines; default 0.".into())),
            );
        } else {
            properties.insert(
                "start_line".into(),
                JsonSchema::number(Some("One-based line number, default 1.".into())),
            );
            properties.insert(
                "line_count".into(),
                JsonSchema::number(Some(
                    "1..100 lines, default 50. Long lines are returned in fragments.".into(),
                )),
            );
        }
        let mut required = vec!["artifact_id".into(), "stream".into()];
        if self.search {
            required.push("query".into());
        }
        ToolSpec::Function(ResponsesApiTool {
            name: self.name().into(),
            description: if self.search {
                "Search original local pipe output literally. Reads at most 256 KiB and returns bounded original lines; follow next_cursor to continue a snapshot. Requires an artifact owned by this thread and a selected environment."
            } else {
                "Read bounded original lines from local pipe output. Reads at most 256 KiB and returns at most 8000 bytes; follow next_cursor for more. Long lines have continued=true. No arbitrary file paths."
            }.into(),
            strict: false, defer_loading: None,
            parameters: JsonSchema::object(properties, Some(required), Some(false.into())),
            output_schema: None,
        })
    }
    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let ToolPayload::Function { arguments } = &invocation.payload else {
                return Err(FunctionCallError::RespondToModel(
                    "exec output lookup requires function arguments".into(),
                ));
            };
            if arguments.len() > 4096 {
                return Err(FunctionCallError::RespondToModel(
                    "exec output arguments exceed 4096 bytes".into(),
                ));
            }
            let request: QueryRequest = parse_arguments(arguments)?;
            let store = invocation
                .session
                .services
                .unified_exec_manager
                .output_artifacts
                .as_ref()
                .ok_or_else(|| {
                    FunctionCallError::RespondToModel("recoverable exec output is disabled".into())
                })?;
            let environments = invocation
                .step_context
                .environments
                .turn_environments()
                .map(|environment| environment.selection.environment_id.clone())
                .collect();
            let result = store
                .query(
                    invocation.session.thread_id().to_string(),
                    environments,
                    request,
                    self.search,
                )
                .await
                .map_err(|reason| {
                    FunctionCallError::RespondToModel(format!(
                        "exec output lookup failed: {reason}"
                    ))
                })?;
            let value = serde_json::to_value(result).map_err(|error| {
                FunctionCallError::RespondToModel(format!("exec output receipt failed: {error}"))
            })?;
            Ok(boxed_tool_output(ArtifactQueryOutput(value)))
        })
    }
}
impl CoreToolRuntime for OutputArtifactHandler {}

#[cfg(test)]
#[path = "output_artifact_tests.rs"]
mod tests;
