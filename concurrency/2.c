#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_FILES 100
#define MAX_FILE_NAME_LENGTH 10
#define MAX_FREQUENCY 10
#define MAX_ARRAY_INDEX 1000000 
#define THRESHOLD 42
#define MAX_THREADS 4
#define FIRST_CHARACTERS 2

typedef struct cntsortarr {
    int file_id[MAX_FREQUENCY];
    unsigned long long int value;
} cntsortarr;

typedef struct {
    int id;
    char name[MAX_FILE_NAME_LENGTH];
    char timestamp_str[20]; // Timestamp in the format "YYYY-MM-DDTHH:MM:SS"
} File;

typedef struct {
    int sort_by;
    unsigned long long int* array;
    cntsortarr* cnt_array;
    unsigned long long int start;
    unsigned long long int end;
} ThreadData_cnt;

typedef struct
{
    File *files;
    int start;
    int end;
    int exp;
    int left;
    int right;
    char *sortBy;
} ThreadData_merge;


// pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

// MERGE SORT FUCTIONS
int compare_files(const File *a, const File *b, const char *sortBy)
{
    if (strcmp(sortBy, "Name") == 0)
    {
        return strcmp(a->name, b->name);
    }
    else if (strcmp(sortBy, "ID") == 0)
    {
        return a->id - b->id;
    }
    else if (strcmp(sortBy, "Timestamp") == 0)
    {
        return strcmp(a->timestamp_str, b->timestamp_str);
    }
    return 0; // Should not reach here
}

void merge(File *files, int left, int mid, int right, const char *sortBy)
{
    int n1 = mid - left + 1;
    int n2 = right - mid;

    File *L = (File *)malloc(n1 * sizeof(File));
    File *R = (File *)malloc(n2 * sizeof(File));

    for (int i = 0; i < n1; i++)
        L[i] = files[left + i];
    for (int j = 0; j < n2; j++)
        R[j] = files[mid + 1 + j];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2)
    {
        if (compare_files(&L[i], &R[j], sortBy) <= 0)
        {
            files[k] = L[i];
            i++;
        }
        else
        {
            files[k] = R[j];
            j++;
        }
        k++;
    }

    while (i < n1)
    {
        files[k] = L[i];
        i++;
        k++;
    }

    while (j < n2)
    {
        files[k] = R[j];
        j++;
        k++;
    }

    free(L);
    free(R);
}

// Thread function to perform merge sort
void *thread_merge_sort(void *arg)
{
    ThreadData_merge *data = (ThreadData_merge *)arg;
    if (data->left < data->right)
    {
        int mid = data->left + (data->right - data->left) / 2;

        pthread_t left_thread, right_thread;
        ThreadData_merge left_data;
        left_data.files = data->files;
        left_data.left = data->left;
        left_data.right = mid;
        left_data.sortBy = data->sortBy;
        ThreadData_merge right_data;
        right_data.files = data->files;
        right_data.right = data->right;
        right_data.left = mid + 1;
        right_data.sortBy = data->sortBy;
        // Create threads for left and right subarrays
        pthread_create(&left_thread, NULL, thread_merge_sort, &left_data);
        pthread_create(&right_thread, NULL, thread_merge_sort, &right_data);

        // Wait for the threads to finish
        pthread_join(left_thread, NULL);
        pthread_join(right_thread, NULL);

        // Merge the sorted subarrays
        merge(data->files, data->left, mid, data->right, data->sortBy);
    }
    pthread_exit(NULL);
}

// Function to perform parallel merge sort
void parallel_merge_sort(File *files, int n, char *sortBy)
{
    pthread_t main_thread;
    ThreadData_merge main_data;
    main_data.files = files;
    main_data.left = 0;
    main_data.right = n - 1;
    main_data.sortBy = sortBy;

    // Create the main thread
    pthread_create(&main_thread, NULL, thread_merge_sort, &main_data);

    // Wait for the main thread to finish
    pthread_join(main_thread, NULL);
}

// Merge sort function
void normal_merge_sort(File *files, int left, int right, const char *sortBy)
{
    if (left < right)
    {
        int mid = left + (right - left) / 2;
        normal_merge_sort(files, left, mid, sortBy);
        normal_merge_sort(files, mid + 1, right, sortBy);
        merge(files, left, mid, right, sortBy);
    }
}

// COUNT SORT FUNCTIONS
pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;

unsigned long hashStringToBase26(const char* str) {
    unsigned long hash = 0;
    int count = 0;
    while (*str && count < FIRST_CHARACTERS) {
        hash = hash * 26 + (*str - 'a');
        str++;
        count++;
    }
    return hash;
}

void* threadCountFrequency(void* arg) {
    ThreadData_cnt* data = (ThreadData_cnt*)arg;
    
    // Create a thread-local frequency array
    cntsortarr* local_freq = (cntsortarr*)calloc(MAX_ARRAY_INDEX, sizeof(cntsortarr));
    if (!local_freq) {
        fprintf(stderr, "Memory allocation failed for local frequency array\n");
        pthread_exit(NULL);
    }

    // First pass: Count frequencies locally without any locks
    for (unsigned long long int i = data->start; i < data->end; i++) {
        unsigned long long int index = data->array[i];
        if (index >= MAX_ARRAY_INDEX) continue;
        
        local_freq[index].value++;
        for (int j = 0; j < MAX_FREQUENCY; j++) {
            if (local_freq[index].file_id[j] == 0) {
                local_freq[index].file_id[j] = i + 1;
                break;
            }
        }
    }

    
    // Second pass: Merge local results into global array with proper synchronization
    pthread_mutex_lock(&global_mutex);
    for (int i = 0; i < MAX_ARRAY_INDEX; i++) {
        if (local_freq[i].value > 0) {
            data->cnt_array[i].value += local_freq[i].value;
            
            // Merge file IDs
            int global_idx = 0;
            for (int j = 0; j < MAX_FREQUENCY && local_freq[i].file_id[j] != 0; j++) {
                // Find next empty slot in global array
                while (global_idx < MAX_FREQUENCY && data->cnt_array[i].file_id[global_idx] != 0) {
                    global_idx++;
                }
                if (global_idx < MAX_FREQUENCY) {
                    data->cnt_array[i].file_id[global_idx] = local_freq[i].file_id[j];
                }
            }
        }
    }
    pthread_mutex_unlock(&global_mutex);

    free(local_freq);
    pthread_exit(NULL);
}

unsigned long long int convertTimestampToEpoch(const char* timestamp_str) {
    struct tm tm;
    memset(&tm, 0, sizeof(struct tm));
    sscanf(timestamp_str, "%d-%d-%dT%d:%d:%d", 
           &tm.tm_year, &tm.tm_mon, &tm.tm_mday, 
           &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
    tm.tm_year -= 1900;
    tm.tm_mon -= 1;
    
    time_t epoch_time = mktime(&tm);
    return (unsigned long long int)epoch_time;
}

int main()
{
    int n;
    scanf("%d", &n);
    unsigned long long int* array = (unsigned long long int*)malloc(sizeof(unsigned long long int) * n);
    cntsortarr* freq_array = (cntsortarr*)calloc(MAX_ARRAY_INDEX, sizeof(cntsortarr));
    File* files = (File*)malloc(sizeof(File) * n);
    memset(freq_array, 0, MAX_ARRAY_INDEX * sizeof(cntsortarr));

    for (int i = 0; i < n; i++) {
        scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp_str);
    }

    char sortBy_str[10];
    scanf("%s", sortBy_str);

    int sort_by = (strcmp(sortBy_str, "ID") == 0) ? 1 : 
                  (strcmp(sortBy_str, "Timestamp") == 0) ? 2 : 
                  (strcmp(sortBy_str, "Name") == 0) ? 3 : 0;

    if (n <= THRESHOLD) {
        if (sort_by == 1) {
            for (int i = 0; i < n; i++) {
                array[i] = (unsigned long long int) files[i].id;
            }
        } else if (sort_by == 2) {
            for (int i = 0; i < n; i++) {
                array[i] = convertTimestampToEpoch(files[i].timestamp_str);
            }
            unsigned long long int min_value = array[0];
            for (int i = 1; i < n; i++) {
                if (array[i] < min_value) {
                    min_value = array[i];
                }
            }
            unsigned long long int offset = min_value - 1;
            for (int i = 0; i < n; i++) {
                array[i] -= offset;
            }

        } else if (sort_by == 3) {
            for (int i = 0; i < n; i++) {
                array[i] = (unsigned long long int) hashStringToBase26(files[i].name);
            }
            unsigned long long int min_value = array[0];
            for (int i = 1; i < n; i++) {
                if (array[i] < min_value) {
                    min_value = array[i];
                }
            }
            unsigned long long int offset = min_value - 1;
            for (int i = 0; i < n; i++) {
                array[i] -= offset;
            }
        }
        pthread_t threads[MAX_THREADS];
        ThreadData_cnt thread_data[MAX_THREADS];
        int chunk_size = n / MAX_THREADS;
        int remainder = n % MAX_THREADS;
        unsigned long long int current_start = 0;

        for (int i = 0; i < MAX_THREADS; i++) {
            thread_data[i].sort_by = sort_by;
            thread_data[i].array = array;
            thread_data[i].cnt_array = freq_array;
            thread_data[i].start = current_start;
            thread_data[i].end = current_start + chunk_size + (i < remainder ? 1 : 0);
            current_start = thread_data[i].end;

            if (pthread_create(&threads[i], NULL, threadCountFrequency, &thread_data[i]) != 0) {
                fprintf(stderr, "Failed to create thread %d\n", i);
                // Clean up previously created threads
                for (int j = 0; j < i; j++) {
                    pthread_join(threads[j], NULL);
                }
                free(array);
                free(freq_array);
                free(files);
                return 1;
            }
        }

        for (int i = 0; i < MAX_THREADS; i++) {
            pthread_join(threads[i], NULL);
        }

        for (int i = 0; i < MAX_ARRAY_INDEX; i++) {
            if (freq_array[i].value == 0) continue;
            for (int j = 0; j < MAX_FREQUENCY && freq_array[i].file_id[j] != 0; j++) {
                int file_index = freq_array[i].file_id[j] - 1;
                printf("%s %d %s\n", 
                    files[file_index].name, 
                    files[file_index].id, 
                    files[file_index].timestamp_str);
            }
        }

        pthread_mutex_destroy(&global_mutex);
        free(files);
        free(array);
        free(freq_array);
    }
    else {
        parallel_merge_sort(files, n, sortBy_str);
        for (int i = 0; i < n; i++)
        {
            printf("%s %d %s\n", files[i].name, files[i].id, files[i].timestamp_str);
        }
    }

    return 0;
}

