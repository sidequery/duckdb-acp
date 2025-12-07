# DuckDB ACP

A DuckDB extension that enables natural language to SQL using the [Agent Client Protocol (ACP)](https://agentclientprotocol.com/). Query your data with plain English.

## Features

- **Natural language queries**: Write queries in plain English instead of SQL
- **Statement syntax**: `CLAUDE show me the top 10 customers by revenue`
- **Table function**: `SELECT * FROM claude('what products sold the most last month?')`
- **Schema-aware**: The agent explores your database schema to generate accurate queries
- **Safe mode**: Blocks mutation queries (INSERT, UPDATE, DELETE) by default

## Supported Agents

- [x] [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- [ ] Codex
- [ ] Gemini

## Quick Start

```sql
-- Load the extension
LOAD 'acp.duckdb_extension';

-- Create some sample data
CREATE TABLE sales (id INT, product VARCHAR, amount DECIMAL, sale_date DATE);
INSERT INTO sales VALUES
    (1, 'Widget', 99.99, '2024-01-15'),
    (2, 'Gadget', 149.99, '2024-01-16'),
    (3, 'Widget', 99.99, '2024-01-17');

-- Query with natural language (statement syntax)
CLAUDE what is the total revenue by product?

-- Or use the table function
SELECT * FROM claude('which product has the highest average sale amount?');
```

## Requirements

- DuckDB 1.1.0+
- [bun](https://bun.sh/) or [Node.js](https://nodejs.org/) (for npx)
- Valid Anthropic API credentials configured for Claude Code

## Configuration

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `acp_agent` | VARCHAR | `claude-code` | Agent command or path |
| `acp_safe_mode` | BOOLEAN | `true` | Block mutation queries |
| `acp_debug` | BOOLEAN | `false` | Enable verbose debug output |
| `acp_show_messages` | BOOLEAN | `false` | Stream agent thinking to output |
| `acp_show_sql` | BOOLEAN | `false` | Print generated SQL before executing |
| `acp_show_summary` | BOOLEAN | `false` | Show analysis summary from agent |
| `acp_show_datasources` | BOOLEAN | `false` | Show datasources and calculations |
| `acp_timeout` | INTEGER | `300` | Timeout in seconds |

```sql
-- Disable safe mode to allow mutations (use with caution)
SET acp_safe_mode = false;

-- See the agent's thinking process
SET acp_show_messages = true;

-- See the generated SQL before it runs
SET acp_show_sql = true;

-- Show analysis summary
SET acp_show_summary = true;

-- Show datasources and calculations
SET acp_show_datasources = true;

-- Enable verbose debug output
SET acp_debug = true;

-- Increase timeout for complex queries
SET acp_timeout = 600;
```

## Safety

By default, all mutation queries all blocked (unless you override the `acp_safe_mode` setting in DuckDB exposed by this extension. All default agent tools are blocked; the underlying agaent only has access to run SQL against the host DuckDB database.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DuckDB Process                             │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    ACP Extension (C++)                        │  │
│  │  - Parses CLAUDE statements and claude() function calls       │  │
│  │  - Invokes Rust FFI for agent communication                   │  │
│  │  - Executes generated SQL and returns results                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                │                                    │
│                                ▼                                    │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                Rust Core (duckdb_acp_core)                    │  │
│  │  - ACP client: spawns agent, communicates via stdio           │  │
│  │  - MCP server: HTTP server exposing run_sql, final_sql tools  │  │
│  │  - Tool execution via SQL callback to C++                     │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                    │ stdio (ACP)           ▲                        │
│                    ▼                       │ HTTP (MCP)             │
│             ┌─────────────┐                │                        │
│             │ Claude Code │────────────────┘                        │
│             └─────────────┘                                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### How It Works

1. **Query parsing**: The extension intercepts `CLAUDE <query>` statements or `claude('query')` function calls
2. **MCP server startup**: An HTTP MCP server starts on a random localhost port, providing two tools:
   - `run_sql`: Execute SQL for schema exploration and query testing
   - `final_query`: Submit the final SQL answer
3. **Agent invocation**: The `claude-code-acp` agent is spawned via stdio and given access to the MCP server
4. **Schema exploration**: The agent uses `run_sql` to discover tables, columns, and data patterns
5. **Query generation**: The agent constructs and tests SQL queries iteratively
6. **Result capture**: When the agent calls `final_query`, the SQL is captured and executed
7. **Cleanup**: The agent process and MCP server are terminated

### Key Design Decisions

- **Embedded MCP server**: The MCP server runs in-process, eliminating external binary dependencies
- **HTTP transport**: Uses HTTP MCP transport (not stdio) for reliable tool communication
- **Safe mode default**: Mutation queries are blocked by default to prevent accidental data modification
- **Timeout protection**: Agent sessions have a configurable timeout to prevent runaway queries

## Building from Source

### Prerequisites

- CMake 3.15+
- C++17 compiler
- Rust 1.70+
- DuckDB source (as submodule)

### Build Steps

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/yourusername/duckdb-acp.git
cd duckdb-acp

# Build
make

# The extension will be at:
# build/release/extension/acp/acp.duckdb_extension
```

### Project Structure

```
.
├── src/
│   ├── acp_extension.cpp      # DuckDB extension entry point
│   └── include/
│       └── acp_extension.hpp  # Extension header
├── duckdb_acp_core/           # Rust core library
│   ├── src/
│   │   ├── lib.rs             # Library entry
│   │   ├── ffi.rs             # C FFI interface
│   │   └── mcp_server.rs      # Embedded HTTP MCP server
│   └── Cargo.toml
├── duckdb/                    # DuckDB submodule
└── CMakeLists.txt
```

## Limitations

- Requires an active internet connection for the Claude API
- Query latency depends on agent response time (typically 5-30 seconds)
- Complex analytical queries may require multiple agent iterations
- The agent cannot access external files or network resources

## License

MIT

## Acknowledgments

- [DuckDB](https://duckdb.org/) for the excellent embeddable database
- [Zed Industries](https://zed.dev/) for the [ACP specification](https://github.com/zed-industries/agent-client-protocol)
- [Anthropic](https://www.anthropic.com/) for Claude and MCP
- [rmcp](https://github.com/anthropics/rmcp) for the Rust MCP implementation
