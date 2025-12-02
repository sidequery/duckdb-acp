//! ACP Core: Natural language to SQL using ACP (Agent Client Protocol)
//!
//! This crate implements an ACP client that:
//! 1. Starts an embedded HTTP MCP server on a random port
//! 2. Spawns an ACP agent (e.g., claude-code-acp) with MCP server URL
//! 3. Agent uses run_sql tool -> HTTP MCP server -> SQL callback -> DuckDB
//! 4. Agent calls final_query, we capture the SQL and return it to DuckDB

mod ffi;
mod mcp_server;

pub use ffi::*;
