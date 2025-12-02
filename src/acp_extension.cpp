#define DUCKDB_EXTENSION_MAIN

#include "acp_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"

#include <sstream>
#include <cstring>

#include "duckdb/function/table_function.hpp"

// Forward declaration of Rust FFI function
extern "C" {
    // Callback type for executing queries from Rust
    typedef const char* (*QueryCallback)(const char* sql, void* context);

    // Main Rust function to generate SQL from natural language
    // Returns allocated string that must be freed with acp_free_string
    // mode: 0 = statement (ACP/CLAUDE prefix), 1 = tvf (table function)
    char* acp_generate_sql(const char* natural_language_query,
                             const char* agent_command,
                             bool debug,
                             int32_t timeout_secs,
                             int32_t mode,
                             QueryCallback callback,
                             void* callback_context);

    // Free string allocated by Rust
    void acp_free_string(char* s);
}

namespace duckdb {

// Global state to hold database instance for callbacks
struct AcpGlobalState {
    weak_ptr<DatabaseInstance> db_instance;
};

static AcpGlobalState global_state;

// Callback function that Rust calls to execute queries
static const char* ExecuteQueryCallback(const char* sql, void* context) {
    auto db = global_state.db_instance.lock();
    if (!db) {
        return strdup("{\"error\": \"Database not available\"}");
    }

    try {
        Connection con(*db);
        auto result = con.Query(sql);

        if (result->HasError()) {
            std::string error_json = "{\"error\": \"" + result->GetError() + "\"}";
            return strdup(error_json.c_str());
        }

        // Convert result to JSON
        std::stringstream ss;
        ss << "[";
        for (idx_t row = 0; row < result->RowCount(); ++row) {
            if (row > 0) ss << ",";
            ss << "{";
            for (idx_t col = 0; col < result->ColumnCount(); ++col) {
                if (col > 0) ss << ",";
                ss << "\"" << result->ColumnName(col) << "\":";
                Value val = result->GetValue(col, row);
                if (val.IsNull()) {
                    ss << "null";
                } else {
                    // Simple escaping for strings
                    std::string str_val = val.ToString();
                    if (val.type().id() == LogicalTypeId::VARCHAR) {
                        ss << "\"" << str_val << "\"";
                    } else {
                        ss << str_val;
                    }
                }
            }
            ss << "}";
        }
        ss << "]";

        return strdup(ss.str().c_str());
    } catch (std::exception &e) {
        std::string error_json = "{\"error\": \"" + std::string(e.what()) + "\"}";
        return strdup(error_json.c_str());
    }
}

// Get string setting with fallback
static std::string GetStringSetting(const std::string &name, const std::string &fallback) {
    auto db = global_state.db_instance.lock();
    if (!db) {
        return fallback;
    }
    auto &config = DBConfig::GetConfig(*db);
    Value val;
    if (config.TryGetCurrentSetting(name, val)) {
        return val.ToString();
    }
    return fallback;
}

// Get bool setting with fallback
static bool GetBoolSetting(const std::string &name, bool fallback) {
    auto db = global_state.db_instance.lock();
    if (!db) {
        return fallback;
    }
    auto &config = DBConfig::GetConfig(*db);
    Value val;
    if (config.TryGetCurrentSetting(name, val)) {
        return val.GetValue<bool>();
    }
    return fallback;
}

// Get the configured agent command from settings
static std::string GetAgentCommand() {
    return GetStringSetting("acp_agent", "claude-code-acp");
}

// Check if safe mode is enabled (blocks mutations)
static bool IsSafeMode() {
    return GetBoolSetting("acp_safe_mode", true);
}

// Check if debug mode is enabled
static bool IsDebugMode() {
    return GetBoolSetting("acp_debug", false);
}

// Get timeout in seconds (0 = no timeout)
static int32_t GetTimeout() {
    auto db = global_state.db_instance.lock();
    if (!db) {
        return 300;
    }
    auto &config = DBConfig::GetConfig(*db);
    Value val;
    if (config.TryGetCurrentSetting("acp_timeout", val)) {
        return val.GetValue<int32_t>();
    }
    return 300;
}

// Forward declaration
static std::string TransformToSQL(const std::string &nl_query, const std::string &agent_override, bool is_tvf);

// ============================================================================
// Claude Table Function (TVF)
// ============================================================================

struct ClaudeBindData : public TableFunctionData {
    unique_ptr<MaterializedQueryResult> result;
    idx_t current_row;

    ClaudeBindData() : current_row(0) {}
};

static unique_ptr<FunctionData> ClaudeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<ClaudeBindData>();

    // Get the natural language query from the first argument
    if (input.inputs.empty() || input.inputs[0].IsNull()) {
        throw BinderException("claude() requires a query string argument");
    }
    string nl_query = input.inputs[0].GetValue<string>();

    // Generate SQL using the agent (always use claude-code for this TVF)
    string sql_query;
    try {
        sql_query = TransformToSQL(nl_query, "claude-code", true);  // true = TVF mode
    } catch (std::exception &e) {
        throw BinderException("claude() failed to generate SQL: " + string(e.what()));
    }

    // Check if agent returned an error message
    if (sql_query.find("ACP ERROR:") != string::npos) {
        throw BinderException(sql_query);
    }

    // Execute the generated SQL and cache the result
    Connection con(*global_state.db_instance.lock());
    bind_data->result = con.Query(sql_query);

    if (bind_data->result->HasError()) {
        throw BinderException("claude() generated SQL failed: " + bind_data->result->GetError() +
                              "\nGenerated SQL: " + sql_query);
    }

    // Extract schema from result
    for (idx_t i = 0; i < bind_data->result->ColumnCount(); i++) {
        names.push_back(bind_data->result->ColumnName(i));
        return_types.push_back(bind_data->result->types[i]);
    }

    // Handle empty result (no columns)
    if (names.empty()) {
        names.push_back("result");
        return_types.push_back(LogicalType::VARCHAR);
    }

    return std::move(bind_data);
}

struct ClaudeGlobalState : public GlobalTableFunctionState {
    idx_t current_row;
    ClaudeGlobalState() : current_row(0) {}
};

static unique_ptr<GlobalTableFunctionState> ClaudeInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<ClaudeGlobalState>();
}

static void ClaudeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ClaudeBindData>();
    auto &state = data_p.global_state->Cast<ClaudeGlobalState>();

    if (!bind_data.result || state.current_row >= bind_data.result->RowCount()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_row < bind_data.result->RowCount() && count < max_count) {
        for (idx_t col = 0; col < bind_data.result->ColumnCount(); col++) {
            output.SetValue(col, count, bind_data.result->GetValue(col, state.current_row));
        }
        state.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

// ============================================================================
// Safe mode / mutation detection
// ============================================================================

// Check if SQL is a mutation (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, etc.)
static bool IsMutationSQL(const std::string &sql) {
    std::string upper_sql = StringUtil::Upper(sql);
    StringUtil::Trim(upper_sql);

    // Check for mutation keywords at the start
    static const std::vector<std::string> mutation_keywords = {
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
        "TRUNCATE", "REPLACE", "MERGE", "UPSERT", "GRANT", "REVOKE",
        "ATTACH", "DETACH", "COPY", "IMPORT", "EXPORT", "VACUUM",
        "CHECKPOINT", "LOAD", "INSTALL", "UNINSTALL", "FORCE"
    };

    for (const auto &keyword : mutation_keywords) {
        if (StringUtil::StartsWith(upper_sql, keyword)) {
            return true;
        }
    }

    // Also check for WITH ... INSERT/UPDATE/DELETE
    if (StringUtil::StartsWith(upper_sql, "WITH")) {
        for (const auto &keyword : {"INSERT", "UPDATE", "DELETE"}) {
            if (upper_sql.find(keyword) != std::string::npos) {
                return true;
            }
        }
    }

    return false;
}

// Transform natural language to SQL using the Rust/Claude backend
// agent_override: if non-empty, use this agent instead of the setting
// is_tvf: true if called from the claude() table function, false for ACP/CLAUDE statements
static std::string TransformToSQL(const std::string &nl_query, const std::string &agent_override = "", bool is_tvf = false) {
    std::string agent_cmd = agent_override.empty() ? GetAgentCommand() : agent_override;
    bool debug = IsDebugMode();
    int32_t timeout = GetTimeout();
    int32_t mode = is_tvf ? 1 : 0;
    char* sql = acp_generate_sql(nl_query.c_str(), agent_cmd.c_str(), debug, timeout, mode, ExecuteQueryCallback, nullptr);
    if (!sql) {
        throw ParserException("ACP: Failed to generate SQL");
    }
    std::string result(sql);
    acp_free_string(sql);

    // Check for mutations if safe mode is enabled
    if (IsSafeMode() && IsMutationSQL(result)) {
        return "SELECT 'ACP ERROR: Safe mode is enabled. Mutation queries (INSERT, UPDATE, DELETE, DROP, etc.) are blocked. Use SET acp_safe_mode=false to disable.' AS acp_error";
    }

    return result;
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
    global_state.db_instance = instance.shared_from_this();

    auto &config = DBConfig::GetConfig(instance);

    // Register settings
    config.AddExtensionOption(
        "acp_agent",
        "ACP agent to use: 'claude-code' or path to binary",
        LogicalType::VARCHAR,
        Value("claude-code"));

    config.AddExtensionOption(
        "acp_safe_mode",
        "Block mutation queries (INSERT, UPDATE, DELETE, DROP, etc.)",
        LogicalType::BOOLEAN,
        Value(true));

    config.AddExtensionOption(
        "acp_debug",
        "Enable debug output to stderr",
        LogicalType::BOOLEAN,
        Value(false));

    config.AddExtensionOption(
        "acp_timeout",
        "Timeout in seconds for agent session (0 = no timeout)",
        LogicalType::INTEGER,
        Value(300));

    AcpParserExtension acp_parser;
    config.parser_extensions.push_back(acp_parser);
    config.operator_extensions.push_back(make_uniq<AcpOperatorExtension>());

    // Register the claude() table function
    TableFunction claude_func("claude", {LogicalType::VARCHAR}, ClaudeFunction, ClaudeBind, ClaudeInit);
    claude_func.projection_pushdown = false;
    loader.RegisterFunction(claude_func);
}

void AcpExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string AcpExtension::Name() {
    return "acp";
}

std::string AcpExtension::Version() const {
#ifdef EXT_VERSION_ACP
    return EXT_VERSION_ACP;
#else
    return "";
#endif
}

ParserExtensionParseResult acp_parse(ParserExtensionInfo *,
                                       const std::string &query) {
    // Check if query starts with "ACP " or "CLAUDE " (case insensitive)
    std::string upper_query = StringUtil::Upper(query);

    std::string prefix;
    std::string agent_override;
    size_t prefix_len = 0;

    if (StringUtil::StartsWith(upper_query, "ACP ")) {
        prefix = "ACP";
        prefix_len = 4;
        // Use configured agent (no override)
    } else if (StringUtil::StartsWith(upper_query, "CLAUDE ")) {
        prefix = "CLAUDE";
        prefix_len = 7;
        agent_override = "claude-code";
    } else {
        // Not a recognized query, let other parsers handle it
        return ParserExtensionParseResult();
    }

    // Extract the natural language part (after prefix)
    std::string nl_query = query.substr(prefix_len);

    // Remove trailing semicolon if present
    if (!nl_query.empty() && nl_query.back() == ';') {
        nl_query.pop_back();
    }

    // Trim whitespace
    StringUtil::Trim(nl_query);

    if (nl_query.empty()) {
        return ParserExtensionParseResult(prefix + ": Please provide a query after '" + prefix + "'");
    }

    // Transform natural language to SQL
    std::string sql_query;
    try {
        sql_query = TransformToSQL(nl_query, agent_override);
    } catch (std::exception &e) {
        return ParserExtensionParseResult(e.what());
    }

    // Parse the generated SQL
    Parser parser;
    try {
        parser.ParseQuery(sql_query);
    } catch (std::exception &e) {
        return ParserExtensionParseResult(prefix + " generated invalid SQL: " + std::string(e.what()) +
                                          "\nGenerated SQL: " + sql_query);
    }

    auto statements = std::move(parser.statements);
    if (statements.empty()) {
        return ParserExtensionParseResult(prefix + ": No statements generated");
    }

    return ParserExtensionParseResult(
        make_uniq_base<ParserExtensionParseData, AcpParseData>(
            std::move(statements[0])));
}

ParserExtensionPlanResult
acp_plan(ParserExtensionInfo *, ClientContext &context,
           unique_ptr<ParserExtensionParseData> parse_data) {
    // Stash the parse data and throw to trigger binding via acp_bind
    auto acp_state = make_shared_ptr<AcpState>(std::move(parse_data));
    context.registered_state->Remove("acp");
    context.registered_state->Insert("acp", acp_state);
    throw BinderException("Use acp_bind instead");
}

BoundStatement acp_bind(ClientContext &context, Binder &binder,
                          OperatorExtensionInfo *info, SQLStatement &statement) {
    switch (statement.type) {
    case StatementType::EXTENSION_STATEMENT: {
        auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
        if (extension_statement.extension.parse_function == acp_parse) {
            auto lookup = context.registered_state->Get<AcpState>("acp");
            if (lookup) {
                auto acp_state = (AcpState *)lookup.get();
                auto acp_binder = Binder::CreateBinder(context, &binder);
                auto acp_parse_data =
                    dynamic_cast<AcpParseData *>(acp_state->parse_data.get());
                return acp_binder->Bind(*(acp_parse_data->statement));
            }
            throw BinderException("Registered state not found");
        }
    }
    default:
        return {};
    }
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(acp, loader) {
    duckdb::LoadInternal(loader);
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
