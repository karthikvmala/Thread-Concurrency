#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <semaphore.h>

#define MAX_FILES 10
#define MAX_USERS 100
#define YELLOW "\033[1;33m"
#define PINK "\033[1;35m"
#define WHITE "\033[1;37m"
#define GREEN "\033[1;32m"
#define RED "\033[1;31m"
#define RESET "\033[0m"

// Operation times
int read_time, write_time, delete_time;
int max_concurrent_access;
int patience_time;

// File structure
typedef struct {
    int id;
    int exists;
    pthread_mutex_t lock;
    sem_t access_sem;
    int readers;
    int writers;
} File;

File files[MAX_FILES];
int num_files;

// Request structure
typedef struct {
    int user_id;
    int file_id;
    char operation[10];
    int request_time;
} Request;

Request requests[MAX_USERS];
int num_requests = 0;
time_t start_time;

// Function to process each user request
void *process_request(void *arg) {
    Request *req = (Request *)arg;
    int can_take_request = 0;
    int time_to_wait = req->request_time;

    sleep(time_to_wait);
    printf(YELLOW "User %d has made request for %s on file %d at %d seconds\n" RESET,
               req->user_id, req->operation, req->file_id, req->request_time);
    sleep(1);

    while (1) {
    int elapsed_time = (int)(time(NULL) - start_time);
    // printf("Elapsed time: %d\n", elapsed_time);
    if (elapsed_time - req->request_time >= patience_time) {
        printf(RED "User %d canceled the request due to no response at %d seconds\n" RESET, req->user_id, elapsed_time);
        pthread_exit(NULL);
    }

    File *file = &files[req->file_id - 1];
    pthread_mutex_lock(&file->lock);

    // Check if file exists and conditions for taking up the request
    can_take_request = 1;
    if (!file->exists) {
        printf(WHITE "LAZY has declined the request of User %d at %d seconds because an invalid/deleted file was requested.\n" RESET, req->user_id, elapsed_time);
        pthread_mutex_unlock(&file->lock);
        pthread_exit(NULL);
    } else if (strcmp(req->operation, "WRITE") == 0) {
        if (file->writers > 0 || file->readers > 0) {  
            can_take_request = 0;
        }
    } else if (strcmp(req->operation, "DELETE") == 0) {
        if (file->writers > 0 || file->readers > 0) {  
            can_take_request = 0;
        }
    }
    
    if (can_take_request) {
        printf(PINK "LAZY has taken up the request of User %d at %d seconds\n" RESET, req->user_id, elapsed_time);
        pthread_mutex_unlock(&file->lock);
        break;
    } else {
        pthread_mutex_unlock(&file->lock);
    }

    // Retry after 1 second
    sleep(1);
}

int elapsed_time = (int)(time(NULL) - start_time);

// Process operation
File *file = &files[req->file_id - 1];
pthread_mutex_lock(&file->lock);

if (strcmp(req->operation, "READ") == 0) {
    file->readers++;
    if (file->readers == 1) {
        sem_wait(&file->access_sem); // Lock access for writers when the first reader enters
    }
    pthread_mutex_unlock(&file->lock);

    sleep(read_time); // Simulate read time
    elapsed_time = (int)(time(NULL) - start_time);
    printf(GREEN "The request for User %d was completed at %d seconds\n" RESET, req->user_id, elapsed_time);

    pthread_mutex_lock(&file->lock);
    file->readers--;
    if (file->readers == 0) {
        sem_post(&file->access_sem); // Unlock access for writers when the last reader exits
    }
    pthread_mutex_unlock(&file->lock);
} 
else if (strcmp(req->operation, "WRITE") == 0) {
    sem_wait(&file->access_sem);
    file->writers++;
    pthread_mutex_unlock(&file->lock);

    sleep(write_time); // Simulate write time
    elapsed_time = (int)(time(NULL) - start_time);
    printf(GREEN "The request for User %d was completed at %d seconds\n" RESET, req->user_id, elapsed_time);

    pthread_mutex_lock(&file->lock);
    file->writers--;
    pthread_mutex_unlock(&file->lock);
    sem_post(&file->access_sem);
} 
else if (strcmp(req->operation, "DELETE") == 0) {
    file->exists = 0;
    pthread_mutex_unlock(&file->lock);

    sleep(delete_time); // Simulate delete time
    elapsed_time = (int)(time(NULL) - start_time);
    printf(GREEN "The request for User %d was completed at %d seconds\n" RESET, req->user_id, elapsed_time);
}

pthread_exit(NULL);

}

int main() {
    int i;
    scanf("%d %d %d", &read_time, &write_time, &delete_time);
    scanf("%d %d %d", &num_files, &max_concurrent_access, &patience_time);

    // Initialize files
    for (i = 0; i < num_files; i++) {
        files[i].id = i + 1;
        files[i].exists = 1;
        files[i].readers = 0;
        files[i].writers = 0;
        pthread_mutex_init(&files[i].lock, NULL);
        sem_init(&files[i].access_sem, 0, max_concurrent_access);  // Initialize semaphore for concurrent access
    }

    // Read requests
    while (1) {
        Request req;
        char temp[100];
        scanf("%s", temp);
        if (strcmp(temp, "STOP") == 0) break;
        req.user_id = atoi(temp);
        scanf("%d %s %d", &req.file_id, req.operation, &req.request_time);
        requests[num_requests++] = req;
    }

    // Record start time and print wake-up message
    start_time = time(NULL);
    printf("LAZY has woken up!\n");

    // Create threads to handle each request
    pthread_t threads[MAX_USERS];
    for (i = 0; i < num_requests; i++) {
        pthread_create(&threads[i], NULL, process_request, (void *)&requests[i]);
    }

    // Wait for all threads to complete
    for (i = 0; i < num_requests; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("LAZY has no more pending requests and is going back to sleep!\n");

    // Cleanup
    for (i = 0; i < num_files; i++) {
        pthread_mutex_destroy(&files[i].lock);
        sem_destroy(&files[i].access_sem);  
    }

    return 0;
}
