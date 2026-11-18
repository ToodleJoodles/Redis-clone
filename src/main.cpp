#include "server.hpp"
#include <iostream>
#include <csignal>
#include <atomic>

static redis::RedisServer* g_server = nullptr;

void signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down gracefully...\n";
    if (g_server) {
        g_server->stop();
    }
}

int main(int argc, char* argv[]) {
    redis::RedisServer::Config config{};

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            config.port = std::stoi(argv[++i]);
        } else if (arg == "--host" && i + 1 < argc) {
            config.host = argv[++i];
        } else if (arg == "--aof") {
            config.aof_enabled = true;
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                config.aof_path = argv[++i];
            }
        } else if (arg == "--rdb" && i + 1 < argc) {
            config.rdb_path = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: redis_server [OPTIONS]\n"
                      << "  --host <ip>       Host to bind (default: 127.0.0.1)\n"
                      << "  --port <port>     Port to bind (default: 6379)\n"
                      << "  --aof [path]      Enable AOF persistence (default: appendonly.aof)\n"
                      << "  --rdb <path>      RDB snapshot path (default: dump.rdb)\n"
                      << "  --help            Show this help message\n";
            return 0;
        }
    }

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "======================================\n";
    std::cout << "  Starting Redis Clone (C++17 RESP2)  \n";
    std::cout << "  Host: " << config.host << " Port: " << config.port << "\n";
    std::cout << "  AOF: " << (config.aof_enabled ? "Enabled" : "Disabled")
              << " (" << config.aof_path << ")\n";
    std::cout << "  RDB: " << config.rdb_path << "\n";
    std::cout << "======================================\n";

    redis::RedisServer server(config);
    g_server = &server;

    if (!server.start()) {
        std::cerr << "Server failed to start.\n";
        return 1;
    }

    std::cout << "Server shutdown complete.\n";
    return 0;
}
