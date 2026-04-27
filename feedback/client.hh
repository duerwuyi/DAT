#ifndef JSON_CLIENT_HH
#define JSON_CLIENT_HH

#include <arpa/inet.h>   // htonl, ntohl
#include <unistd.h>      // send, recv
#include <string>
#include <nlohmann/json.hpp>
#include "extract_feature.hh"

nlohmann::json serialize_usage(const table_usage_in_from_clause& usage);

static nlohmann::json wrap_payload(const std::string& type_name,
    const nlohmann::json& payload) {
    return nlohmann::json{
        {"class_type", type_name},
        {"payload",    payload}
    };
}

static bool send_all(int fd, const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    while (len > 0) {
        ssize_t n = ::send(fd, p, len, 0);
        if (n <= 0) return false;
        p += n; len -= n;
    }
    return true;
}

static bool recv_all(int fd, void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    while (len > 0) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n <= 0) return false;
        p += n; len -= n;
    }
    return true;
}

static bool send_json_with_len(int fd, const nlohmann::json& j) {
    std::string body = j.dump();
    uint32_t len = htonl(static_cast<uint32_t>(body.size()));
    return send_all(fd, &len, sizeof(len)) &&
           send_all(fd, body.data(), body.size());
}

static bool recv_json_with_len(int fd, nlohmann::json& out) {
    uint32_t netlen = 0;
    if (!recv_all(fd, &netlen, sizeof(netlen))) return false;
    uint32_t len = ntohl(netlen);
    if (len == 0 || len > (100u<<20)) return false; // 100MB
    std::string body(len, '\0');
    if (!recv_all(fd, body.data(), body.size())) return false;
    out = nlohmann::json::parse(body, /*cb=*/nullptr, /*allow_exceptions=*/true);
    return true;
}

class JsonClient {
public:
    JsonClient(const std::string& host, int port);
    ~JsonClient();

    bool connect();

    std::unordered_map<std::string, std::string> send_usage(const query_feature& feature);

    void send_action_list(const std::vector<dds>& action_list);

    void send_feedback(feedback& f, bool stop = false);

    action recv_action(shared_ptr<schema> s);

private:
    std::string host_;
    int port_;
    int sock_;
};

#endif