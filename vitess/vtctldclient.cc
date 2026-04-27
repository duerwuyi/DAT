#include "vtctldclient.hh"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <cstdio>
#include <sys/wait.h>
#include <iostream>

vtctldclient::vtctldclient(std::string ip_, int port_, std::string bin_)
    : ip(std::move(ip_)), port(port_), bin(std::move(bin_)) {}

std::string vtctldclient::Server() const {
    return ip + ":" + std::to_string(port);
}

std::string vtctldclient::ServerId() const {
    // Make cache paths unique across different vtctld servers.
    return SanitizePathComponent(ip) + "_" + std::to_string(port);
}

vtctldclient::ExecResult vtctldclient::Exec(const std::string& cmd) const {
    std::string full = cmd + " 2>&1";
    std::cerr<<full<<std::endl;
    FILE* pipe = popen(full.c_str(), "r");
    if (!pipe) throw std::runtime_error("popen failed: " + cmd);

    std::string out;
    char buf[4096];
    while (fgets(buf, sizeof(buf), pipe) != nullptr) out += buf;

    int rc = pclose(pipe);
    int exit_code = 1;
    if (WIFEXITED(rc)) exit_code = WEXITSTATUS(rc);

    return {exit_code, out};
}

std::string vtctldclient::SanitizePathComponent(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if ((c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '_' || c == '-' || c == '.') {
            out.push_back(c);
        } else {
            out.push_back('_');
        }
    }
    if (out.empty()) out = "default";
    return out;
}

void vtctldclient::EnsureDir(const std::string& dir) {
    std::filesystem::create_directories(std::filesystem::path(dir));
}

std::string vtctldclient::KeyspaceDir(const std::string& keyspace) const {
    // ./vitess/saved/<server_id>/<keyspace>/
    return std::string(VITESS_SAVED) + ServerId() + "/" + SanitizePathComponent(keyspace) + "/";
}

std::string vtctldclient::SchemaTmpPath(const std::string& keyspace) const {
    return KeyspaceDir(keyspace) + VITESS_SCHEMA_TMP_FILE;
}
std::string vtctldclient::VSchemaTmpPath(const std::string& keyspace) const {
    return KeyspaceDir(keyspace) + VITESS_VSCHEMA_TMP_FILE;
}
std::string vtctldclient::SchemaPath(const std::string& keyspace) const {
    return KeyspaceDir(keyspace) + VITESS_SCHEMA_FILE;
}
std::string vtctldclient::VSchemaPath(const std::string& keyspace) const {
    return KeyspaceDir(keyspace) + VITESS_VSCHEMA_FILE;
}

void vtctldclient::WriteFile(const std::string& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to write file: " + path);
    out << content;
    out.flush();
    if (!out) throw std::runtime_error("Write failed: " + path);
}

bool vtctldclient::FileExists(const std::string& path) {
    return std::filesystem::exists(std::filesystem::path(path));
}

void vtctldclient::CopyFileOverwrite(const std::string& from, const std::string& to) {
    std::filesystem::copy_file(
        std::filesystem::path(from),
        std::filesystem::path(to),
        std::filesystem::copy_options::overwrite_existing
    );
}

std::string vtctldclient::ShellQuote(const std::string& s) {
    // 
    // abc'd => 'abc'"'"'d'
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "'\"'\"'";
        else out += c;
    }
    out += "'";
    return out;
}

// ---- Public APIs ----

std::string vtctldclient::GetVSchemaString(const std::string& keyspace) const {
    // vtctldclient --server ... GetVSchema <keyspace>
    std::string cmd = bin + " --server " + Server() + " GetVSchema " + ShellQuote(keyspace);
    auto res = Exec(cmd);
    if (res.exit_code != 0) {
        throw std::runtime_error("GetVSchema failed.\nCommand: " + cmd + "\nOutput:\n" + res.output);
    }
    return res.output;
}

void vtctldclient::DumpVSchemaToTmp(const std::string& keyspace) const {
    EnsureDir(KeyspaceDir(keyspace));
    std::string vs = GetVSchemaString(keyspace);
    WriteFile(VSchemaTmpPath(keyspace), vs);
}

void vtctldclient::ApplySchemaFromString(const std::string& keyspace, const std::string& schema_sql) const {
    EnsureDir(KeyspaceDir(keyspace));
    const std::string tmp = SchemaTmpPath(keyspace);
    const std::string committed = SchemaPath(keyspace);

    // stage
    WriteFile(tmp, schema_sql);

    std::string cmd = bin + " --server " + Server()
        + " ApplySchema --sql-file " + ShellQuote(tmp) + " " + ShellQuote(keyspace);

    auto res = Exec(cmd);
    if (res.exit_code != 0) {
        // rollback tmp <- committed (if committed exists)
        if (FileExists(committed)) {
            CopyFileOverwrite(committed, tmp);
        }
        throw std::runtime_error("ApplySchema failed.\nCommand: " + cmd + "\nOutput:\n" + res.output);
    }

    // commit: committed <- tmp
    CopyFileOverwrite(tmp, committed);
}

void vtctldclient::ApplyVSchemaFromString(const std::string& keyspace, const std::string& vschema_json) const {
    EnsureDir(KeyspaceDir(keyspace));
    const std::string tmp = VSchemaTmpPath(keyspace);
    const std::string committed = VSchemaPath(keyspace);

    // stage
    WriteFile(tmp, vschema_json);

    std::string cmd = bin + " --server " + Server()
        + " ApplyVSchema --vschema-file " + ShellQuote(tmp) + " " + ShellQuote(keyspace);

    auto res = Exec(cmd);
    if (res.exit_code != 0) {
        // rollback tmp <- committed (if committed exists)
        if (FileExists(committed)) {
            CopyFileOverwrite(committed, tmp);
        }
        throw std::runtime_error("ApplyVSchema failed.\nCommand: " + cmd + "\nOutput:\n" + res.output);
    }

    // commit: committed <- tmp
    CopyFileOverwrite(tmp, committed);
}

void vtctldclient::ApplySchemaFile(const std::string& keyspace, const std::string& sql_file_path) const {
    //  tmp/committed 
    std::string cmd = bin + " --server " + Server()
        + " ApplySchema --sql-file " + ShellQuote(sql_file_path) + " " + ShellQuote(keyspace);

    auto res = Exec(cmd);
    if (res.exit_code != 0) {
        throw std::runtime_error("ApplySchemaFile failed.\nCommand: " + cmd + "\nOutput:\n" + res.output);
    }
}

void vtctldclient::Exec_Command(const std::string& command) const {
    std::string cmd = bin + " --server " + Server() + " "+ command;

    auto res = Exec(cmd);
    if (res.exit_code != 0) {
        throw std::runtime_error("Exec_Command failed.\nCommand: " + cmd + "\nOutput:\n" + res.output);
    }
}