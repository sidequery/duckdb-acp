#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class AcpExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

// Forward declarations
BoundStatement acp_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct AcpOperatorExtension : public OperatorExtension {
	AcpOperatorExtension() : OperatorExtension() {
		Bind = acp_bind;
	}

	std::string GetName() override {
		return "acp";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		throw InternalException("acp operator should not be serialized");
	}
};

ParserExtensionParseResult acp_parse(ParserExtensionInfo *, const std::string &query);

ParserExtensionPlanResult acp_plan(ParserExtensionInfo *, ClientContext &, unique_ptr<ParserExtensionParseData>);

struct AcpParserExtension : public ParserExtension {
	AcpParserExtension() : ParserExtension() {
		parse_function = acp_parse;
		plan_function = acp_plan;
	}
};

struct AcpParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, AcpParseData>(statement->Copy());
	}

	virtual string ToString() const override {
		return "AcpParseData";
	}

	AcpParseData(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
	}
};

class AcpState : public ClientContextState {
public:
	explicit AcpState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

} // namespace duckdb
