//! C FFI interface for acp-core
//!
//! Architecture (embedded HTTP MCP server):
//! 1. DuckDB extension calls acp_generate_sql()
//! 2. We start an embedded HTTP MCP server on a random port
//! 3. We spawn the ACP agent with McpServer::Http pointing to our server
//! 4. Agent uses run_sql/final_query tools -> HTTP -> embedded server -> SQL callback -> DuckDB
//! 5. Agent calls final_query, we capture the SQL and return it

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use agent_client_protocol as acp;
use acp::Agent;
use async_trait::async_trait;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::mcp_server::{start_mcp_http_server, SqlCallback};

/// Callback type for executing SQL queries from C++
pub type QueryCallback = extern "C" fn(sql: *const c_char, context: *mut std::ffi::c_void) -> *const c_char;

/// Resolve agent command to actual executable
fn resolve_agent_command(agent: &str, debug: bool) -> (String, Vec<String>) {
    if agent.starts_with('/') || agent.starts_with("./") || agent.starts_with("../") {
        if debug {
            eprintln!("acp: using agent path '{}'", agent);
        }
        return (agent.to_string(), vec![]);
    }

    let expanded = match agent {
        "claude-code" | "claude" => "claude-code-acp",
        other => other,
    };

    if which::which(expanded).is_ok() {
        if debug {
            eprintln!("acp: found agent '{}' in PATH", expanded);
        }
        return (expanded.to_string(), vec![]);
    }

    if which::which("bunx").is_ok() {
        if debug {
            eprintln!("acp: using bunx to run '{}'", expanded);
        }
        return ("bunx".to_string(), vec![expanded.to_string()]);
    }

    if which::which("npx").is_ok() {
        if debug {
            eprintln!("acp: using npx to run '{}'", expanded);
        }
        return ("npx".to_string(), vec![expanded.to_string()]);
    }

    if debug {
        eprintln!("acp: agent '{}' not found, npx/bunx not available", expanded);
    }
    (expanded.to_string(), vec![])
}

/// Generate SQL from natural language query using ACP
#[no_mangle]
pub extern "C" fn acp_generate_sql(
    natural_language_query: *const c_char,
    agent_command: *const c_char,
    debug: bool,
    timeout_secs: i32,
    mode: i32,
    safe_mode: bool,
    callback: QueryCallback,
    callback_context: *mut std::ffi::c_void,
) -> *mut c_char {
    let query = match unsafe { CStr::from_ptr(natural_language_query) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };

    let agent_setting = if agent_command.is_null() {
        env::var("ACP_AGENT").unwrap_or_else(|_| "claude-code".to_string())
    } else {
        match unsafe { CStr::from_ptr(agent_command) }.to_str() {
            Ok(s) if !s.is_empty() => s.to_string(),
            _ => env::var("ACP_AGENT").unwrap_or_else(|_| "claude-code".to_string()),
        }
    };

    let (agent_cmd, agent_args) = resolve_agent_command(&agent_setting, debug);

    let timeout = if timeout_secs > 0 {
        Some(std::time::Duration::from_secs(timeout_secs as u64))
    } else {
        None
    };

    let is_tvf = mode == 1;

    if debug {
        eprintln!("acp: starting, query='{}', agent='{}', safe_mode={}", query, agent_cmd, safe_mode);
    }

    match generate_sql_impl(&agent_cmd, &agent_args, &query, debug, timeout, is_tvf, safe_mode, callback, callback_context) {
        Ok(sql) => {
            if debug {
                eprintln!("acp: success, sql={}", sql);
            }
            CString::new(sql)
                .map(|s| s.into_raw())
                .unwrap_or(std::ptr::null_mut())
        }
        Err(e) => {
            eprintln!("acp: FAILED with error: {}", e);
            std::ptr::null_mut()
        }
    }
}

fn generate_sql_impl(
    agent_cmd: &str,
    agent_args: &[String],
    query: &str,
    debug: bool,
    timeout: Option<std::time::Duration>,
    is_tvf: bool,
    safe_mode: bool,
    callback: QueryCallback,
    callback_context: *mut std::ffi::c_void,
) -> Result<String, String> {
    let callback_ptr = callback as usize;
    let context_ptr = callback_context as usize;

    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create runtime: {}", e))?;

    let query = query.to_string();
    let agent_cmd = agent_cmd.to_string();
    let agent_args = agent_args.to_vec();

    rt.block_on(async move {
        // Create SQL callback that invokes the C++ callback
        let sql_callback: SqlCallback = Arc::new(move |sql: &str| {
            let callback: QueryCallback = unsafe { std::mem::transmute(callback_ptr) };
            let context = context_ptr as *mut std::ffi::c_void;

            if let Ok(c_sql) = CString::new(sql) {
                let result_ptr = callback(c_sql.as_ptr(), context);
                if result_ptr.is_null() {
                    r#"{"error": "Callback returned null"}"#.to_string()
                } else {
                    let result = unsafe { CStr::from_ptr(result_ptr) }
                        .to_string_lossy()
                        .into_owned();
                    unsafe { libc::free(result_ptr as *mut std::ffi::c_void) };
                    result
                }
            } else {
                r#"{"error": "Invalid SQL string"}"#.to_string()
            }
        });

        // Start embedded HTTP MCP server
        if debug {
            eprintln!("acp: starting embedded HTTP MCP server (safe_mode={})", safe_mode);
        }
        let (port, shutdown_tx) = start_mcp_http_server(sql_callback, is_tvf, safe_mode).await
            .map_err(|e| format!("Failed to start MCP server: {}", e))?;

        if debug {
            eprintln!("acp: MCP server running on port {}", port);
        }

        let acp_future = run_acp_flow(&agent_cmd, &agent_args, &query, port, debug, safe_mode);

        let result = if let Some(timeout_duration) = timeout {
            match tokio::time::timeout(timeout_duration, acp_future).await {
                Ok(res) => res,
                Err(_) => Err(format!("Agent timed out after {} seconds", timeout_duration.as_secs())),
            }
        } else {
            acp_future.await
        };

        // Shutdown the MCP server
        let _ = shutdown_tx.send(());

        result
    })
}

async fn run_acp_flow(
    agent_cmd: &str,
    agent_args: &[String],
    query: &str,
    mcp_port: u16,
    debug: bool,
    safe_mode: bool,
) -> Result<String, String> {
    let final_sql = Arc::new(Mutex::new(None::<String>));
    let cancelled = Arc::new(AtomicBool::new(false));

    let client = AcpClient {
        final_sql: final_sql.clone(),
        cancelled: cancelled.clone(),
        debug,
    };

    if debug {
        if agent_args.is_empty() {
            eprintln!("acp: spawning agent '{}'", agent_cmd);
        } else {
            eprintln!("acp: spawning agent '{}' with args {:?}", agent_cmd, agent_args);
        }
    }
    let mut child = Command::new(agent_cmd)
        .args(agent_args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("Failed to spawn agent '{}': {}", agent_cmd, e))?;

    let outgoing = child.stdin.take()
        .ok_or("Failed to get agent stdin")?
        .compat_write();
    let incoming = child.stdout.take()
        .ok_or("Failed to get agent stdout")?
        .compat();

    if debug {
        if let Some(stderr) = child.stderr.take() {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("acp agent stderr: {}", line);
                }
            });
        }
    }

    let local = tokio::task::LocalSet::new();
    let query = query.to_string();

    let prompt_result = local.run_until(async move {
        let (conn, handle_io) = acp::ClientSideConnection::new(
            client,
            outgoing,
            incoming,
            |fut| { tokio::task::spawn_local(fut); },
        );

        tokio::task::spawn_local(handle_io);

        if debug {
            eprintln!("acp: sending initialize request");
        }
        let init_res = conn.initialize(
            acp::InitializeRequest::new(acp::ProtocolVersion::LATEST)
        ).await.map_err(|e| format!("ACP initialize failed: {}", e))?;

        if debug {
            eprintln!("acp: initialize ok, protocol={:?}", init_res.protocol_version);
            eprintln!("acp: agent capabilities: {:?}", init_res.agent_capabilities);
        }

        // Configure HTTP MCP server
        let mcp_url = format!("http://127.0.0.1:{}/mcp", mcp_port);
        let mcp_server = acp::McpServerHttp::new("duckdb", &mcp_url);
        let mcp_servers = vec![acp::McpServer::Http(mcp_server)];

        let cwd = env::current_dir()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|_| std::path::PathBuf::from("/"));

        if debug {
            eprintln!("acp: creating session with HTTP MCP server at {}", mcp_url);
        }

        let mode_rules = if safe_mode {
            "- Generate ONLY read-only SELECT queries. Never INSERT, UPDATE, DELETE, DROP, CREATE, etc.\n"
        } else {
            "- You may use any SQL statements including INSERT, UPDATE, DELETE, CREATE, etc.\n"
        };

        let system_prompt = format!(
            "\n\n\
            You are operating as a SQL generation assistant within DuckDB. \
            You have access to a single MCP server called 'duckdb' with two tools:\n\
            1. `run_sql` - Execute SQL to explore the catalog/schema and test queries\n\
            2. `final_query` - Submit your final SQL answer (REQUIRED at the end)\n\n\
            CRITICAL RULES:\n\
            - You can ONLY use the MCP tools provided. No file access, no terminal, no other capabilities.\n\
            {}\
            - Always call `final_query` with your answer - this is how results are returned to the user.\n\n\
            WORKFLOW:\n\
            1. EXPLORE THE CATALOG FIRST! Use information_schema to discover tables and columns:\n\
               - SELECT table_schema, table_name FROM information_schema.tables;\n\
               - SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'x';\n\
               - SHOW ALL TABLES; or DESCRIBE table_name;\n\
            2. Look at sample data: SELECT * FROM table LIMIT 5;\n\
            3. Build and test your query\n\
            4. Submit via final_query\n\n\
            Be efficient but thorough in exploring what data is available before writing your query.",
            mode_rules
        );

        let session_meta: serde_json::Map<String, serde_json::Value> = serde_json::from_value(serde_json::json!({
            "disableBuiltInTools": true,
            "systemPrompt": {
                "append": system_prompt
            }
        })).unwrap();

        let new_sess = conn.new_session(
            acp::NewSessionRequest::new(cwd)
                .mcp_servers(mcp_servers)
                .meta(session_meta)
        ).await.map_err(|e| format!("ACP new_session failed: {}", e))?;

        if debug {
            eprintln!("acp: session created, id={:?}", new_sess.session_id);
        }

        let prompt = query.to_string();

        if debug {
            eprintln!("acp: sending prompt");
        }
        let prompt_res = conn.prompt(
            acp::PromptRequest::new(new_sess.session_id.clone(), vec![prompt.into()])
        ).await.map_err(|e| format!("ACP prompt failed: {}", e))?;

        if debug {
            eprintln!("acp: prompt done, stop_reason={:?}", prompt_res.stop_reason);
        }

        Ok::<(), String>(())
    }).await;

    child.kill().await.ok();
    prompt_result?;

    let sql = final_sql.lock().unwrap().clone();
    match sql {
        Some(s) => {
            if debug {
                eprintln!("acp: final SQL: {}", s);
            }
            Ok(s)
        }
        None => Err("Agent did not call final_query tool. No SQL returned.".to_string()),
    }
}

struct AcpClient {
    final_sql: Arc<Mutex<Option<String>>>,
    cancelled: Arc<AtomicBool>,
    debug: bool,
}

#[async_trait(?Send)]
impl acp::Client for AcpClient {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> anyhow::Result<acp::RequestPermissionResponse, acp::Error> {
        if self.debug {
            eprintln!("acp: permission requested, options={}", args.options.len());
        }

        if self.cancelled.load(Ordering::SeqCst) {
            return Ok(acp::RequestPermissionResponse::new(acp::RequestPermissionOutcome::Cancelled));
        }

        use acp::PermissionOptionKind as K;
        let choice = args.options.iter()
            .find(|o| matches!(o.kind, K::AllowOnce))
            .cloned()
            .or_else(|| args.options.first().cloned());

        match choice {
            Some(o) => {
                if self.debug {
                    eprintln!("acp: granting permission option_id={}", o.option_id.0);
                }
                Ok(acp::RequestPermissionResponse::new(
                    acp::RequestPermissionOutcome::Selected(
                        acp::SelectedPermissionOutcome::new(o.option_id)
                    )
                ))
            }
            None => Err(acp::Error::invalid_params()),
        }
    }

    async fn write_text_file(&self, _args: acp::WriteTextFileRequest) -> anyhow::Result<acp::WriteTextFileResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn read_text_file(&self, _args: acp::ReadTextFileRequest) -> anyhow::Result<acp::ReadTextFileResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn create_terminal(&self, _args: acp::CreateTerminalRequest) -> anyhow::Result<acp::CreateTerminalResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn terminal_output(&self, _args: acp::TerminalOutputRequest) -> anyhow::Result<acp::TerminalOutputResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn release_terminal(&self, _args: acp::ReleaseTerminalRequest) -> anyhow::Result<acp::ReleaseTerminalResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn wait_for_terminal_exit(&self, _args: acp::WaitForTerminalExitRequest) -> anyhow::Result<acp::WaitForTerminalExitResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn kill_terminal_command(&self, _args: acp::KillTerminalCommandRequest) -> anyhow::Result<acp::KillTerminalCommandResponse, acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> anyhow::Result<(), acp::Error> {
        use acp::SessionUpdate as SU;

        if self.cancelled.load(Ordering::SeqCst) {
            return Ok(());
        }

        match args.update {
            SU::AgentMessageChunk(chunk) => {
                if self.debug {
                    let text = match chunk.content {
                        acp::ContentBlock::Text(t) => t.text,
                        acp::ContentBlock::Image(_) => "<image>".into(),
                        acp::ContentBlock::Audio(_) => "<audio>".into(),
                        acp::ContentBlock::ResourceLink(link) => link.uri,
                        acp::ContentBlock::Resource(_) => "<resource>".into(),
                        _ => "<unknown>".into(),
                    };
                    eprintln!("acp: agent message: {}", text);
                }
            }
            SU::ToolCall(tc) => {
                if self.debug {
                    eprintln!("acp: tool_call id={} title='{}' status={:?}", tc.tool_call_id.0, tc.title, tc.status);
                }

                if tc.title.contains("final_query") {
                    if let Some(raw_input) = &tc.raw_input {
                        if let Some(sql) = raw_input.get("sql").and_then(|v| v.as_str()) {
                            if self.debug {
                                eprintln!("acp: captured final_query SQL: {}", sql);
                            }
                            *self.final_sql.lock().unwrap() = Some(sql.to_string());
                        }
                    }
                }
            }
            SU::ToolCallUpdate(upd) => {
                if self.debug {
                    if let Some(status) = &upd.fields.status {
                        eprintln!("acp: tool_call_update id={} status={:?}", upd.tool_call_id.0, status);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn acp_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}
