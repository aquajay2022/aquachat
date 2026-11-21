#include <stdio.h>
#include <stdint.h>
#include <sys/socket.h>
#include <errno.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <string.h>

#define MAX_CLIENTS 16
#define MAX_SIZE 1024 // max message size

/*this struct exists to pass info for the accept_loop(), server_init() functions*/
typedef struct info_struct {
    int sockfd; // this is only for the listening socket
    int sockfd_list[MAX_CLIENTS]; //for accepted sockets which can send&recv
    int epollfd; //I may not need this
    int connected_clients;
    struct sockaddr_in sockaddr;
    pthread_t thr_accept;
    pthread_t thr_server;
    pthread_t thr_terminal;
} info_struct;

void ipv4_sockaddr_in_setup(struct sockaddr_in* ptr_to_struct, const char* addr, uint16_t port) {
    ptr_to_struct->sin_family = AF_INET;
    ptr_to_struct->sin_port = htons(port);
    int pton_dbg = inet_pton(AF_INET, addr, &ptr_to_struct->sin_addr.s_addr);
    if (pton_dbg != 1) {
        if (pton_dbg == 0) {
            printf("invalid address\n");
            return;
        }
        printf("(ipv4_sockaddr_setup) errno for inet_pton() %s\n", strerror(errno));
        return;
    }
    return;
}

int ipv4_lsock_setup(void* ptr_to_struct, socklen_t sockaddr_size) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        printf("(lsock_settup) errno for socket() %s\n", strerror(errno));
        return -1;
    }
    if (bind(sock, ptr_to_struct, sockaddr_size) == -1) {
        printf("(lsock_settup) errno for bind() %s\n", strerror(errno));
        return -1;
    }
    if (fcntl(sock, F_SETFD, O_NONBLOCK) == -1) {
        printf("(lsock_settup) fcntl() errno %s\n", strerror(errno));
        return -1;
    }
    if (listen(sock, MAX_CLIENTS) == -1) {
        printf("(lsock_settup) errno for listen() %s\n", strerror(errno));
        return -1;
    }
    return sock;
}

int config(struct sockaddr_in sockaddr, struct info_struct* info, uint16_t port) {
    ipv4_sockaddr_in_setup(&sockaddr, "0.0.0.0", port);
    info->sockfd = ipv4_lsock_setup(&sockaddr, (socklen_t)sizeof(sockaddr));
    if (info->sockfd == -1) {
        return -1;
    }
    info->sockaddr = sockaddr;
    info->connected_clients = 0;
    for (int i = 0; i < MAX_CLIENTS; ++i) {
        info->sockfd_list[i] = -1;
    }
    info->epollfd = epoll_create1(0);
    if (info->epollfd == -1) {
        printf("epoll errno: %s\n", strerror(errno));
        return -1;
    }
    info->thr_server = 0;
    info->thr_accept = 0;
    info->thr_terminal = 0;
    return 0;
}

/*will probaby be ran in a seperate thread?*/
void* accept_loop(void* ptr) {
    printf("accept_loop()\n");
    struct info_struct* info = ptr;
    for (int i = 0; i < MAX_CLIENTS; ++i) {
        struct epoll_event event;
        event.events = EPOLLIN | EPOLLET;
        socklen_t size = sizeof(info->sockaddr);

        event.data.fd = accept(info->sockfd, (struct sockaddr*)&info->sockaddr, &size);
        if (event.data.fd == -1) {
            printf("accept_loop(): accept() errno %s\n", strerror(errno));
            return nullptr;
        }
        if (fcntl(event.data.fd, F_SETFD, O_NONBLOCK) == -1) {
            printf("(lsock_settup) fcntl() errno %s\n", strerror(errno));
            return nullptr;
        }
        info->sockfd_list[i] = event.data.fd;
        info->connected_clients += 1;
        printf("Socket connection added sockfd: %i\n", event.data.fd);

        if (epoll_ctl(info->epollfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1) {
            printf("epoll_ctl errno %s\n", strerror(errno));
            close(info->epollfd);
            return nullptr;
        }
    }

    printf("All slots full!\n");
    return ptr;
}

//close the sockfd and remove from connected list
int remove_sock(struct info_struct* info, int fd) {
    printf("remove_sock(), sockfd: %i\n", fd);
    if (epoll_ctl(info->epollfd, EPOLL_CTL_DEL, fd, NULL) == -1) {
        printf("Couldn't remove fd from epoll, epoll_ctl errno: %s\n", strerror(errno));
        return -1;
    }
    for (int i = 0; i < MAX_CLIENTS; ++i) {
        if (info->sockfd_list[i] != -1 && info->sockfd_list[i] == fd) {
            if (close(fd) == -1) {
                printf("close() errno: %s\n", strerror(errno));
                return -1;
            }
            info->sockfd_list[i] = -1;
            printf("remove_sock(): Done!\n");
            return 0;
        }
    }
    printf("remove_sock(): Done!\n");
    return 0;
}

void exit_cleanup(struct info_struct* info){ //exit cleanly and cleanup
    printf("Exiting and cleaning up!\n");
    int accept_cancel = pthread_cancel(info->thr_accept);
    if (accept_cancel != 0){
        printf("pthread_cancel() error number %i\n", accept_cancel);
    }
    int server_cancel = pthread_cancel(info->thr_server);
    if (server_cancel != 0){
        printf("pthread_cancel() error number %i\n", server_cancel);
    }
    if (close(info->sockfd) == -1){ //close the initial listening sock
        printf("close() errno: %s\n", strerror(errno));
    }
    for (int i = 0; i < MAX_CLIENTS; ++i){
        if (info->sockfd_list[i] != -1){
            close(info->sockfd_list[i]);
        } 
    }
    if (close(info->epollfd) == -1){
        printf("close() errno: %s\n", strerror(errno));
    }
    printf("Done! returning!\n");
    return;
}

int recv_check(struct info_struct* info, struct epoll_event event, int received_bytes) {
    if (received_bytes == -1) {
        printf("recv() errno: %s\n", strerror(errno));
        return -1;
    }
    if (received_bytes == 0) {
        if (remove_sock(info, event.data.fd) == -1) {
            printf("remove_sock() called, sockfd: %i\n", event.data.fd);
            return -1;
        }
        return 0;
    }
    return 0;
}

int message_handler(struct info_struct* info, struct epoll_event event) {
    char message[MAX_SIZE];
    int received_bytes = recv(event.data.fd, message, sizeof(message), 0);
    if (recv_check(info, event, received_bytes) == -1) {
        return -1;
    }
    printf("received message!: %s\n", message);
    for (int i = 0; i < MAX_CLIENTS; ++i) {
        if (info->sockfd_list[i] != event.data.fd && info->sockfd_list[i] != -1) {
            if (send(info->sockfd_list[i], &message, sizeof(message), 0) == -1) {
                printf("send() errno: %s\n", strerror(errno));
                return 0;
            }
        }
    }
    return 0;
}

//the fuck?
void* server_init(void* ptr) {
    struct info_struct* info = ptr;
    struct epoll_event events[MAX_CLIENTS];
    while (1) {
        int ready_fd = epoll_wait(info->epollfd, events, MAX_CLIENTS, -1);
        if (ready_fd == -1) {
            printf("event_handler epoll_wait() errno %s\n", strerror(errno));
            return nullptr;
        }
        printf("epoll_wait() returned %i\n", ready_fd);
        for (int i = 0; i < ready_fd; ++i) {
            message_handler(info, events[i]);
        }
    }
    return info;
}

void* terminal_handler(void* struct_ptr){
    struct info_struct* info = struct_ptr;
    char terminal_input[32]; //I will keep it 32 for now, sounds cool. kek
    while (1){
        fgets((char* restrict)&terminal_input, sizeof(terminal_input), stdin);
        if (strncmp(terminal_input, "/exit", strlen("/exit")) == 0){
            return info;
        }
    }
    return info;
}

int all_init(struct info_struct* info) {
    /*
    accept_loop() - MT seperate thread (thread1)
    server_init() - MT seperate thread (thread2)
    terminal_handler() - MT seperate thread (thread3)
    */
    pthread_t thr_accept, thr_server, thr_terminal;
    int thread1 = pthread_create(&thr_accept, NULL, &accept_loop, info);
    if (thread1 != 0) {
        printf("pthread_create() return value (thread1): %i\n", thread1);
        return -1;
    }
    int thread2 = pthread_create(&thr_server, NULL, &server_init, info);
    if (thread2 != 0) {
        printf("pthread_create() return value (thread2): %i\n", thread2);
        return -1;
    }
    int thread3 = pthread_create(&thr_terminal, NULL, &terminal_handler, info);
    if (thread3 != 0){
        printf("pthread_create() return value (thread3): %i\n", thread3);
        return -1;
    }
    info->thr_accept = thr_accept;
    info->thr_server = thr_server;
    info->thr_terminal = thr_terminal;
    int p_join = pthread_join(thr_terminal, NULL);
    if (p_join != 0) {
        printf("pthread_join() error code %i\n", p_join);
        return -1;
    }
    exit_cleanup(info);
    return 0;
}

int main(void) {
    errno = 0;
    uint16_t port;

    printf("Enter port to host on: ");
    scanf("%hu", &port);

    struct sockaddr_in sockaddr;
    struct info_struct info;
    if (config(sockaddr, &info, port) == -1) {
        printf("struct_config() failed\n");
        return -1;
    }
    if (all_init(&info) == -1) {
        printf("all_init() failed\n");
        return -1;
    }

    return 0;
}
