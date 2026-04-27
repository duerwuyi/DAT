#include "client.hh"
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

JsonClient::JsonClient(const std::string& host, int port)
    : host_(host), port_(port), sock_(-1) {}

JsonClient::~JsonClient() {
    if (sock_ != -1) {
        close(sock_);
    }
}

bool JsonClient::connect() {
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
        perror("socket");
        return false;
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, host_.c_str(), &serv_addr.sin_addr) <= 0) {
        perror("inet_pton");
        return false;
    }

    if (::connect(sock_, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        return false;
    }

    return true;
}

std::unordered_map<std::string, std::string> JsonClient::send_usage(const query_feature& feature) {
    nlohmann::json payload = feature;
    nlohmann::json wrapper = wrap_payload("query_feature", payload);

    if (!send_json_with_len(sock_, wrapper)) {
        std::cerr << "Failed to send query_feature\n";
        return {};
    }

    // 3)  + JSON
    nlohmann::json reply;
    if (!recv_json_with_len(sock_, reply)) {
        std::cerr << "Failed to read reply\n";
        return {};
    }

    // 4)  wrapper payload
    const nlohmann::json* obj = &reply;
    if (reply.contains("class_type") && reply.contains("payload") && reply["payload"].is_object()) {
        obj = &reply["payload"];
    }

    // 5)  unordered_map<string, string>
    std::unordered_map<std::string, std::string> out;
    if (obj->is_object()) {
        for (auto it = obj->begin(); it != obj->end(); ++it) {
            // value  JSON 
            out[it.key()] = it->is_string() ? it->get<std::string>() : it->dump();
        }
    } else {
        // 
        out["value"] = obj->dump();
    }
    return out;
}


void JsonClient::send_action_list(const std::vector<dds>& action_list) {
    nlohmann::json payload;
    payload["action_list"] = action_list;     //  json 
    payload["total_groups"]  = total_groups;    // 

    nlohmann::json wrapper = wrap_payload("action_list", payload);

    std::string body = wrapper.dump();
    std::cout << "send_action_list size: " << body.size() << std::endl;

    if (!send_json_with_len(sock_, wrapper)) {
        std::cerr << "Failed to send action_list\n";
        return;
    }

    //  + JSON
    nlohmann::json reply;
    if (!recv_json_with_len(sock_, reply)) {
        std::cerr << "Failed to read reply\n";
        return;
    }
    std::cout << "reply: " << reply.dump() << std::endl;
}

void JsonClient::send_feedback(feedback& f, bool stop) {
    f.stop = stop;
    nlohmann::json payload;
    payload["feedback"] = f;        // to_json  typed
    nlohmann::json wrapper = wrap_payload("feedback", payload);
    if (!send_json_with_len(sock_, wrapper)) {
        std::cerr << "Failed to send feedback\n";
        return;
    }

    nlohmann::json reply;
    if (!recv_json_with_len(sock_, reply)) {
        std::cerr << "Failed to read reply\n";
        return;
    }
    std::cout << "reply: " << reply.dump() << std::endl;
}

action JsonClient::recv_action(shared_ptr<schema> s) {
    nlohmann::json reply;

    //  ack action
    do {
        if (!recv_json_with_len(sock_, reply)) {
            std::cerr << "Failed to read reply\n";
            return {};
        }
    } while (reply.contains("class_type") && reply["class_type"] == "ack");

    if (!(reply.contains("class_type") && reply["class_type"] == "action"
          && reply.contains("payload") && reply["payload"].is_object())) {
        std::cerr << "Unexpected message: " << reply.dump() << "\n";
        return {};
    }

    const auto& p = reply["payload"];
    std::unordered_map<std::string, std::string> params;
    for (auto it = p.begin(); it != p.end(); ++it) {
        // int/bool/array/object dump 
        params[it.key()] = it->is_string() ? it->get<std::string>() : it->dump();
    }

    return get_action_from_server(s, params);
}