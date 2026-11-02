#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>

int main() {
    // 1. Create a TCP socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }

    // Lets you restart the server immediately without "address in use" errors
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    // 2. Bind to port 6379 on all interfaces
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(6379);
    // TODO: call bind() with listen_fd, (sockaddr*)&addr, sizeof(addr); check for errors
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
}
    // 3. Start listening
    // TODO: call listen() with listen_fd and a backlog of 128; check for errors
    if(listen(listen_fd,128)<0){
        perror("listen");
        return 1;
    }
    std::cout << "listening on 6379\n";

    // 4. Accept one client
    // TODO: call accept() with listen_fd, nullptr, nullptr; store the result in client_fd
    int client_fd=accept(listen_fd, nullptr, nullptr);
    if(client_fd<0){
        perror("accept");
        return 1;
    }
    // 5. Echo loop
    char buf[1024];
    while (true) {
        // TODO: n = read(client_fd, buf, sizeof(buf));
        // TODO: if n <= 0, break (client closed or error)
        // TODO: write(client_fd, buf, n);
        ssize_t n=read(client_fd, buf, sizeof(buf));
        if(n<=0)break;
        write(client_fd, buf, n);
    }
    // TODO: close client_fd and listen_fd
    close(client_fd);
    close(listen_fd);
    return 0;
}