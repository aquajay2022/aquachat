#include <stdio.h>
#include <stdint.h>
#include <sys/socket.h>
#include <errno.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>

#define MAX_SIZE 1024

typedef struct info_struct{
    int sockfd;
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
        errno = 0;
        return;
    }
    return;
}

int ipv4_sock_setup(const void* ptr_to_struct, socklen_t sockaddr_size) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        printf("errno for socket() %i\n", errno);
        errno = 0;
        return -1;
    }
    int connect_status = connect(sock, ptr_to_struct, sockaddr_size);
    if (connect_status == -1) {
        printf("errno for connect() %i\n", errno);
        errno = 0;
        return -1;
    }
    return sock;
}

void* read_send(void* struct_ptr){
    struct info_struct *info = struct_ptr;
    char buffer[MAX_SIZE];
    while (1){
        fgets((char* restrict)&buffer, sizeof(buffer), stdin);
        ssize_t sent_stuff = send(info->sockfd, buffer, sizeof(buffer), 0);
        if (sent_stuff == -1){
            printf("send() errno %i\n", errno);
            return nullptr;
        }
    }
    return info;
}

void* recv_print(void* struct_ptr){
    struct info_struct *info = struct_ptr;
    char buffer[MAX_SIZE];
    while (1){
        ssize_t received_stuff = recv(info->sockfd, buffer, sizeof(buffer), 0);
        if (received_stuff == -1){
            printf("recv() errno %i\n", errno);
            return nullptr;
        }
        printf("%s", buffer);
    }
    return info;
}

int all_init(struct info_struct info){
    pthread_t read_send_t, recv_print_t;
    int thread1 = pthread_create(&read_send_t, NULL, &read_send, &info);
    if(thread1 != 0){
        printf("pthread_create() return value (thread1): %i\n", thread1);
        return -1;
    }
    int thread2 = pthread_create(&recv_print_t, NULL, &recv_print, &info);
    if (thread2 != 0){
        printf("pthread_create() return value (thread2): %i\n", thread2);
        return -1;
    }
    int p_join = pthread_join(recv_print_t, NULL);
    if (p_join != 0){
        printf("pthread_join() error code %i\n", p_join);
        return -1;
    }
    return 0;
}

int config(struct sockaddr_in sockaddr, struct info_struct* info, char* IP, uint16_t port){
    ipv4_sockaddr_in_setup(&sockaddr, IP, port);
    int sock = ipv4_sock_setup(&sockaddr, (socklen_t)sizeof(sockaddr));
    if (sock == -1){
        return -1;
    }
    info->sockfd = sock;
    info->sockaddr = sockaddr;
    return 0;
}

int main(void){
    errno = 0;
    char IP[13];
    uint16_t port;
    printf("Enter the IP to connect to: ");
    scanf("%s", IP);
    printf("\nEnter the port: ");
    scanf("%hu", &port);

    struct sockaddr_in sockaddr;
    struct info_struct info;
    if (config(sockaddr, &info, IP, port) == -1){
        printf("config() failed\n");
        return 0;
    }

    if(all_init(info) == -1){
        printf("all_init() failed\n");
        return 0;
    }
    return 0;

}