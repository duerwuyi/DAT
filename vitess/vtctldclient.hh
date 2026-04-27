#ifndef VTCTLDCLIENT_HH
#define VTCTLDCLIENT_HH

#include <string>

#define VITESS_SAVED "./vitess/saved/"
#define VITESS_SCHEMA_FILE "schema.sql"
#define VITESS_SCHEMA_TMP_FILE "tmp_schema.sql"
#define VITESS_VSCHEMA_FILE "vschema.json"
#define VITESS_VSCHEMA_TMP_FILE "tmp_vschema.json"

struct vtctldclient {
    std::string ip;
    int port;

    // vtctldclient  PATH 
    std::string bin = "vtctldclient";

    vtctldclient(std::string ip_, int port_, std::string bin_ = "vtctldclient");

    // 1)  Vitess  vschema string
    std::string GetVSchemaString(const std::string& keyspace) const;

    // 2)  GetVSchemaString  keyspace  tmp_vschema.json
    void DumpVSchemaToTmp(const std::string& keyspace) const;

    void ApplySchemaFromString(const std::string& keyspace, const std::string& schema_sql) const;

    void ApplyVSchemaFromString(const std::string& keyspace, const std::string& vschema_json) const;

    // 5)  keyspace  schema SQL  drop_1_100.sql
    void ApplySchemaFile(const std::string& keyspace, const std::string& sql_file_path) const;

    void Exec_Command(const std::string& command) const;

    struct ExecResult {
        int exit_code;
        std::string output; // stdout + stderr
    };

    std::string Server() const;
    std::string ServerId() const;
    ExecResult Exec(const std::string& cmd) const;

    // For filesystem path components.
    static std::string SanitizePathComponent(const std::string& s);
    static void EnsureDir(const std::string& dir);

    // Saved file layout:
    //   ./vitess/saved/<server_id>/<keyspace>/schema.sql
    //   ./vitess/saved/<server_id>/<keyspace>/vschema.json
    // where server_id is derived from (ip, port).
    std::string KeyspaceDir(const std::string& keyspace) const;
    std::string SchemaTmpPath(const std::string& keyspace) const;
    std::string VSchemaTmpPath(const std::string& keyspace) const;
    std::string SchemaPath(const std::string& keyspace) const;
    std::string VSchemaPath(const std::string& keyspace) const;

    static void WriteFile(const std::string& path, const std::string& content);
    static bool FileExists(const std::string& path);
    static void CopyFileOverwrite(const std::string& from, const std::string& to);

    static std::string ShellQuote(const std::string& s);
};

#endif