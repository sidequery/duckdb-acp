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
use tokio::sync::{oneshot, mpsc};

/// Result from the final_query tool containing SQL and optional metadata
#[derive(Debug, Clone)]
pub struct FinalQueryResult {
    pub sql: String,
    pub summary: Option<String>,
    pub datasources: Option<String>,
}

/// Callback type for executing SQL queries
pub type SqlCallback = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Get instructions based on mode
fn get_instructions(is_tvf: bool, safe_mode: bool) -> String {
    let restrictions = if safe_mode {
        "RESTRICTIONS (READ-ONLY MODE):\n\
        - ONLY use SELECT queries to read data\n\
        - DO NOT use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE\n\
        - DO NOT use COPY, IMPORT, EXPORT, ATTACH, DETACH\n\
        - DO NOT modify schema or data in any way\n\n"
    } else {
        "MODE: Full access (mutations allowed)\n\
        You may use any SQL statements including INSERT, UPDATE, DELETE, CREATE, etc.\n\n"
    };

    let catalog_exploration = "\
        CATALOG EXPLORATION (do this first!):\n\
        DuckDB has a rich information_schema. Use these queries to understand the database:\n\
        - SELECT * FROM information_schema.schemata; -- list all schemas\n\
        - SELECT table_schema, table_name, table_type FROM information_schema.tables; -- all tables\n\
        - SELECT * FROM information_schema.columns WHERE table_name = 'x'; -- columns for table\n\
        - DESCRIBE table_name; -- quick way to see columns\n\
        - SHOW TABLES; -- tables in current schema\n\
        - SHOW ALL TABLES; -- tables in all schemas\n\
        - SELECT database_name, schema_name, function_name FROM duckdb_functions() WHERE function_type = 'table'; -- table functions\n\n\
        IMPORTANT: Always explore the catalog first to understand what data is available!\n\n";

    if is_tvf {
        format!(
            "DuckDB MCP Server for the claude() table-valued function (TVF).\n\n\
            You are being invoked from: SELECT * FROM claude('user question')\n\
            Your job is to generate a SQL query that answers the user's question.\n\
            The result will be returned directly as a table to the user.\n\n\
            TOOLS:\n\
            1) `run_sql` - Execute SQL to explore the catalog/schema and test queries\n\
            2) `final_query` - YOU MUST CALL THIS at the end with your final SQL answer\n\n\
            {restrictions}\
            {catalog}\
            Workflow:\n\
            1. EXPLORE: Use run_sql to query information_schema and understand available tables/columns\n\
            2. INVESTIGATE: Look at sample data with SELECT * FROM table LIMIT 5\n\
            3. BUILD: Construct and test your query\n\
            4. SUBMIT: ALWAYS call final_query with the SQL that answers the question",
            restrictions = restrictions,
            catalog = catalog_exploration
        )
    } else {
        format!(
            "DuckDB MCP Server for ACP/CLAUDE SQL generation.\n\n\
            You are being invoked from: ACP <natural language query> or CLAUDE <query>\n\
            Your job is to generate a SQL query that answers the user's question.\n\n\
            TOOLS:\n\
            1) `run_sql` - Execute SQL to explore the catalog/schema and test queries\n\
            2) `final_query` - YOU MUST CALL THIS at the end with your final SQL answer\n\n\
            {restrictions}\
            {catalog}\
            Workflow:\n\
            1. EXPLORE: Use run_sql to query information_schema and understand available tables/columns\n\
            2. INVESTIGATE: Look at sample data with SELECT * FROM table LIMIT 5\n\
            3. BUILD: Construct and test your query\n\
            4. SUBMIT: ALWAYS call final_query with the SQL that answers the question\n\n\
            Tips:\n\
            - DuckDB SQL dialect (supports CTEs, window functions, UNNEST, etc.)\n\
            - Keep LIMIT small during exploration\n\
            - Check duckdb_functions() for available functions",
            restrictions = restrictions,
            catalog = catalog_exploration
        )
    }
}

#[derive(Clone)]
pub struct DuckDbMcpService {
    sql_callback: SqlCallback,
    is_tvf: bool,
    safe_mode: bool,
    show_sql: bool,
    final_result_tx: mpsc::Sender<FinalQueryResult>,
}

impl DuckDbMcpService {
    pub fn new(sql_callback: SqlCallback, is_tvf: bool, safe_mode: bool, show_sql: bool, final_result_tx: mpsc::Sender<FinalQueryResult>) -> Self {
        Self { sql_callback, is_tvf, safe_mode, show_sql, final_result_tx }
    }
}

#[derive(Deserialize)]
struct RunSqlParams {
    sql: String,
}

#[derive(Deserialize)]
struct FinalQueryParams {
    sql: String,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    datasources: Option<String>,
}

impl ServerHandler for DuckDbMcpService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(get_instructions(self.is_tvf, self.safe_mode)),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let safe_mode = self.safe_mode;

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

        let run_sql_desc = if safe_mode {
            "Execute a DuckDB SQL query for exploration. Use this to explore the schema (SHOW TABLES, DESCRIBE table_name) and test queries. READ-ONLY MODE: Only SELECT queries allowed."
        } else {
            "Execute a DuckDB SQL query. Use this to explore the schema (SHOW TABLES, DESCRIBE table_name), test queries, and execute any SQL statements."
        };

        let run_sql_tool = Tool::new(
            "run_sql",
            run_sql_desc,
            rmcp::model::object(run_sql_schema),
        );

        let final_query_schema = serde_json::json!({
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The final SQL query that answers the user's question"
                },
                "summary": {
                    "type": "string",
                    "description": "A brief summary of the analysis: what the query does and key insights"
                },
                "datasources": {
                    "type": "string",
                    "description": "Description of data sources used and how calculations were performed"
                }
            },
            "required": ["sql"],
            "additionalProperties": false
        });

        let final_query_desc = if safe_mode {
            "REQUIRED: You MUST call this tool at the end with the final query that answers the user's question. Include 'summary' with a brief analysis and 'datasources' explaining the tables/calculations used. READ-ONLY MODE: Only SELECT queries allowed."
        } else {
            "REQUIRED: You MUST call this tool at the end with the final SQL that answers the user's question. Include 'summary' with a brief analysis and 'datasources' explaining the tables/calculations used."
        };

        let final_query_tool = Tool::new(
            "final_query",
            final_query_desc,
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

            let show_sql = self.show_sql;

            match request.name.as_ref() {
                "run_sql" => {
                    let params: RunSqlParams = serde_json::from_value(Value::Object(args))
                        .map_err(|e| McpError::invalid_params(format!("bad arguments: {e}"), None))?;

                    if show_sql {
                        eprintln!("\n[Explore SQL]\n{}", params.sql);
                        let _ = std::io::Write::flush(&mut std::io::stderr());
                    }

                    let result = sql_callback(&params.sql);
                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }
                "final_query" => {
                    let params: FinalQueryParams = serde_json::from_value(Value::Object(args))
                        .map_err(|e| McpError::invalid_params(format!("bad arguments: {e}"), None))?;

                    // Send the result through the channel
                    let final_result = FinalQueryResult {
                        sql: params.sql.clone(),
                        summary: params.summary,
                        datasources: params.datasources,
                    };
                    let _ = self.final_result_tx.try_send(final_result);

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
/// Returns the port number, a shutdown sender, and a receiver for final query results
pub async fn start_mcp_http_server(
    sql_callback: SqlCallback,
    is_tvf: bool,
    safe_mode: bool,
    show_sql: bool,
) -> Result<(u16, oneshot::Sender<()>, mpsc::Receiver<FinalQueryResult>), String> {
    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("Failed to bind MCP server: {}", e))?;

    let addr = listener.local_addr()
        .map_err(|e| format!("Failed to get local addr: {}", e))?;
    let port = addr.port();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (final_result_tx, final_result_rx) = mpsc::channel::<FinalQueryResult>(1);

    // Create the MCP service
    let session_manager = Arc::new(LocalSessionManager::default());
    let config = StreamableHttpServerConfig {
        sse_keep_alive: Some(std::time::Duration::from_secs(15)),
        stateful_mode: false, // Stateless mode - simpler for our use case
    };

    let mcp_service = StreamableHttpService::new(
        move || Ok(DuckDbMcpService::new(sql_callback.clone(), is_tvf, safe_mode, show_sql, final_result_tx.clone())),
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

    Ok((port, shutdown_tx, final_result_rx))
}
