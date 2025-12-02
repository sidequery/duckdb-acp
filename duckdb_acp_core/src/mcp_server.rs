//! Embedded HTTP MCP Server for DuckDB
//!
//! This MCP server runs in-process as an HTTP server and provides SQL execution tools
//! to the ACP agent. It uses a callback to execute SQL directly in the DuckDB connection.

use std::sync::Arc;

use axum::Router;
use rmcp::{
    handler::server::ServerHandler,
    model::*,
    transport::streamable_http_server::{
        StreamableHttpService, StreamableHttpServerConfig,
        session::local::LocalSessionManager,
    },
    ErrorData as McpError,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Callback type for executing SQL queries
pub type SqlCallback = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Get instructions based on mode
fn get_instructions(is_tvf: bool) -> String {
    if is_tvf {
        "DuckDB MCP Server for the claude() table-valued function (TVF).\n\n\
        You are being invoked from: SELECT * FROM claude('user question')\n\
        Your job is to generate a SQL query that answers the user's question.\n\
        The result will be returned directly as a table to the user.\n\n\
        TOOLS:\n\
        1) `run_sql` - Execute SQL to explore schema and test queries\n\
        2) `final_query` - YOU MUST CALL THIS at the end with your final SQL answer\n\n\
        RESTRICTIONS (READ-ONLY MODE):\n\
        - ONLY use SELECT queries to read data\n\
        - DO NOT use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE\n\
        - DO NOT use COPY, IMPORT, EXPORT, ATTACH, DETACH\n\
        - DO NOT modify schema or data in any way\n\n\
        Workflow:\n\
        1. Use run_sql('SHOW TABLES') to see available tables\n\
        2. Use run_sql('DESCRIBE table_name') to see columns\n\
        3. Use run_sql to test your SELECT query\n\
        4. ALWAYS call final_query with the SELECT query that answers the question".to_string()
    } else {
        "DuckDB MCP Server for ACP/CLAUDE SQL generation.\n\n\
        You are being invoked from: ACP <natural language query> or CLAUDE <query>\n\
        Your job is to generate a SQL query that answers the user's question.\n\n\
        TOOLS:\n\
        1) `run_sql` - Execute SQL to explore schema and test queries\n\
        2) `final_query` - YOU MUST CALL THIS at the end with your final SQL answer\n\n\
        RESTRICTIONS (READ-ONLY MODE):\n\
        - ONLY use SELECT queries to read data\n\
        - DO NOT use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE\n\
        - DO NOT use COPY, IMPORT, EXPORT, ATTACH, DETACH\n\
        - DO NOT modify schema or data in any way\n\
        - If asked to modify data, explain you can only read\n\n\
        Workflow:\n\
        1. Use run_sql('SHOW TABLES') to see available tables\n\
        2. Use run_sql('DESCRIBE table_name') to see columns\n\
        3. Use run_sql to test your SELECT query\n\
        4. ALWAYS call final_query with the SELECT query that answers the question\n\n\
        Tips:\n\
        - DuckDB SQL dialect\n\
        - Keep LIMIT small during exploration".to_string()
    }
}

#[derive(Clone)]
pub struct DuckDbMcpService {
    sql_callback: SqlCallback,
    is_tvf: bool,
}

impl DuckDbMcpService {
    pub fn new(sql_callback: SqlCallback, is_tvf: bool) -> Self {
        Self { sql_callback, is_tvf }
    }
}

#[derive(Deserialize)]
struct RunSqlParams {
    sql: String,
}

impl ServerHandler for DuckDbMcpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(get_instructions(self.is_tvf)),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let run_sql_schema = serde_json::json!({
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL query to execute"
                }
            },
            "required": ["sql"],
            "additionalProperties": false
        });

        let run_sql_tool = Tool::new(
            "run_sql",
            "Execute a DuckDB SQL query for exploration. Use this to explore the schema (SHOW TABLES, DESCRIBE table_name) and test SELECT queries. READ-ONLY: Do not use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, or any mutation queries. Returns results as JSON.",
            rmcp::model::object(run_sql_schema),
        );

        let final_query_schema = serde_json::json!({
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The final SQL query that answers the user's question"
                }
            },
            "required": ["sql"],
            "additionalProperties": false
        });

        let final_query_tool = Tool::new(
            "final_query",
            "REQUIRED: You MUST call this tool at the end of every conversation with the final SELECT query that answers the user's question. READ-ONLY: Only SELECT queries are allowed. Do not submit INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, or any mutation queries.",
            rmcp::model::object(final_query_schema),
        );

        std::future::ready(Ok(ListToolsResult {
            tools: vec![run_sql_tool, final_query_tool],
            next_cursor: None,
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParam,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let sql_callback = self.sql_callback.clone();

        async move {
            let args = request
                .arguments
                .ok_or_else(|| McpError::invalid_params("missing arguments", None))?;

            match request.name.as_ref() {
                "run_sql" => {
                    let params: RunSqlParams = serde_json::from_value(Value::Object(args))
                        .map_err(|e| McpError::invalid_params(format!("bad arguments: {e}"), None))?;

                    let result = sql_callback(&params.sql);
                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }
                "final_query" => {
                    let params: RunSqlParams = serde_json::from_value(Value::Object(args))
                        .map_err(|e| McpError::invalid_params(format!("bad arguments: {e}"), None))?;

                    // Return a special marker that the FFI layer can detect
                    let result = serde_json::json!({
                        "FINAL_SQL": params.sql
                    });
                    Ok(CallToolResult::success(vec![Content::text(result.to_string())]))
                }
                _ => Err(McpError::invalid_params(
                    format!("Unknown tool: {}", request.name),
                    None,
                )),
            }
        }
    }
}

/// Start an embedded HTTP MCP server on a random port
/// Returns the port number and a shutdown sender
pub async fn start_mcp_http_server(
    sql_callback: SqlCallback,
    is_tvf: bool,
) -> Result<(u16, oneshot::Sender<()>), String> {
    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("Failed to bind MCP server: {}", e))?;

    let addr = listener.local_addr()
        .map_err(|e| format!("Failed to get local addr: {}", e))?;
    let port = addr.port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Create the MCP service
    let session_manager = Arc::new(LocalSessionManager::default());
    let config = StreamableHttpServerConfig {
        sse_keep_alive: Some(std::time::Duration::from_secs(15)),
        stateful_mode: false, // Stateless mode - simpler for our use case
    };

    let mcp_service = StreamableHttpService::new(
        move || Ok(DuckDbMcpService::new(sql_callback.clone(), is_tvf)),
        session_manager,
        config,
    );

    // Create axum router with the MCP service
    let app = Router::new()
        .fallback_service(tower::ServiceBuilder::new().service(mcp_service));

    // Spawn the server
    tokio::spawn(async move {
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            });

        if let Err(e) = server.await {
            eprintln!("MCP HTTP server error: {}", e);
        }
    });

    Ok((port, shutdown_tx))
}
