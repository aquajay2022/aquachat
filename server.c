#include <stdio.h>
#include <stdint.h>
#include <sys/socket.h>
#include <errno.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <fcntl.h>

#define MAX_CLIENTS 16
#define MAX_SIZE 1024 // max message size

/*this struct exists to pass info for the accept_loop(), server_init() functions*/
typedef struct info_struct{ 
    int sockfd; // this is only for the listening socket
    int sockfd_list[MAX_CLIENTS]; //for accepted sockets which can send&recv
    int epollfd; //I may not need this
    int connected_clients;
    struct sockaddr_in sockaddr;
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
        printf("(ipv4_sockaddr_setup) errno for inet_pton() %i\n", errno);
        return;
    }
    return;
}


int ipv4_lsock_setup(void* ptr_to_struct, socklen_t sockaddr_size) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        printf("(lsock_settup) errno for socket() %i\n", errno);
        return -1;
    }
    if (bind(sock, ptr_to_struct, sockaddr_size) == -1) {
        printf("(lsock_settup) errno for bind() %i\n", errno);
        return -1;
    }
    if (fcntl(sock, F_SETFD, O_NONBLOCK) == -1){
        printf("(lsock_settup) fcntl() errno %i\n", errno);
        return -1;
    }
    if (listen(sock, MAX_CLIENTS) == -1) {
        printf("(lsock_settup) errno for listen() %i\n", errno);
        return -1;
    }
    return sock;
}

int config(struct sockaddr_in sockaddr, struct info_struct* info, uint16_t port){
    ipv4_sockaddr_in_setup(&sockaddr, "0.0.0.0", port);
    info->sockfd = ipv4_lsock_setup(&sockaddr, (socklen_t)sizeof(sockaddr));
    if (info->sockfd == -1){
        return -1;
    }
    info->sockaddr = sockaddr;

    info->epollfd = epoll_create1(0);
    if (info->epollfd == -1){
        printf("epoll errno: %i\n", errno);
        return -1;
    }
    return 0;
}

/*will probaby be ran in a seperate thread?*/
void* accept_loop(void* ptr){
    printf("Accept loop\n");
    struct info_struct* struct_ptr = ptr;
    struct_ptr->connected_clients = 0;
    for (int i = 0; i < MAX_CLIENTS; ++i){
        struct epoll_event event;
        event.events = EPOLLIN | EPOLLET;
        socklen_t size = sizeof(struct_ptr->sockaddr);

        event.data.fd = accept(struct_ptr->sockfd, (struct sockaddr *)&struct_ptr->sockaddr, &size);
        if(event.data.fd == -1){
            printf("acl accept errno %i\n", errno);
            return nullptr;
        }
        if (fcntl(event.data.fd, F_SETFD, O_NONBLOCK) == -1){
            printf("(lsock_settup) fcntl() errno %i\n", errno);
            return nullptr;
        }
        struct_ptr->sockfd_list[i] = event.data.fd;
        struct_ptr->connected_clients += 1;
        printf("Socket connection added sockfd: %i\n", event.data.fd);

        if(epoll_ctl(struct_ptr->epollfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1){
            printf("epoll_ctl errno %i\n", errno);
            close(struct_ptr->epollfd);
            return nullptr;
        }
    }

    printf("All slots full!\n");
    return ptr;
}

int msg_handler(struct info_struct* struct_ptr, struct epoll_event event){
    char message_buffer[MAX_SIZE];
    ssize_t received_bytes = recv(event.data.fd, message_buffer, sizeof(message_buffer), 0);
    if(received_bytes == -1){
        printf("recv() (event_handler) errno: %i\n", errno);
        return -1;
    }
    printf("received message!: %s\n", message_buffer);

    for (int i = 0; i < struct_ptr->connected_clients; ++i){
        if (struct_ptr->sockfd_list[i] != event.data.fd){
            ssize_t sent_bytes = send(struct_ptr->sockfd_list[i], &message_buffer, sizeof(message_buffer), 0);
            if (sent_bytes == -1){
                printf("send() (msg_handler) errno: %i\n", errno);
                return -1;
            }
        }
    }
    return 0;
}

int event_handler(struct info_struct* ptr, struct epoll_event event){
    if (msg_handler(ptr, event) == -1){
        return -1;
    }
    return 0;
}
//the fuck?
void* server_init(void* ptr){
    struct info_struct* struct_ptr = ptr;
    struct epoll_event events[MAX_CLIENTS];
    while (1){
        int ready_fd = epoll_wait(struct_ptr->epollfd, events, MAX_CLIENTS, -1);
        if (ready_fd == -1){
            printf("event_handler epoll_wait() errno %i\n", errno);
            return nullptr;
        }
        printf("epoll_wait() returned %i\n", ready_fd);
        for (int i = 0; i < ready_fd; ++i){
            event_handler(struct_ptr, events[i]); // rework here
        }
        
    }
    return nullptr;
}

int all_init(struct info_struct info){
    /*
    accept_loop() - MT seperate thread
    server_init() - MT seperate thread
    */
    pthread_t accept_t, server_t;
    int thread1 = pthread_create(&accept_t, NULL, &accept_loop, &info);
    if(thread1 != 0){
        printf("pthread_create() return value (thread1): %i\n", thread1);
        return -1;
    }
    int thread2 = pthread_create(&server_t, NULL, &server_init, &info);
    if (thread2 != 0){
        printf("pthread_create() return value (thread2): %i\n", thread2);
        return -1;
    }
    int p_join = pthread_join(server_t, NULL);
    if (p_join != 0){
        printf("pthread_join() error code %i\n", p_join);
        return -1;
    }
    return 0;
}

/*void* terminal_input(void* struct_ptr){
    while (1){
        char terminal_buffer[32];

    }
}*/

int main(void){
    errno = 0;
    uint16_t port;

    printf("Enter port to host on: ");
    scanf("%hu", &port);

    struct sockaddr_in sockaddr;
    struct info_struct info;
    if (config(sockaddr, &info, port) == -1){
        printf("struct_config() failed\n");
        return -1;
    }
    if(all_init(info) == -1){
        printf("all_init() failed\n");
        return -1;
    }

    return 0;
}