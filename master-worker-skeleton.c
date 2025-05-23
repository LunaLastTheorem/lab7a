#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <wait.h>
#include <pthread.h>

int item_to_produce, curr_buf_size;
int total_items, max_buf_size, num_workers, num_masters;

int *buffer;

pthread_mutex_t lock;
pthread_cond_t can_produce;
pthread_cond_t can_consume;

void print_produced(int num, int master)
{

  printf("Produced %d by master %d\n", num, master);
}

void print_consumed(int num, int worker)
{

  printf("Consumed %d by worker %d\n", num, worker);
}

// produce items and place in buffer
// modify code below to synchronize correctly
void *generate_requests_loop(void *data)
{
  int thread_id = *((int *)data);

  while (1)
  {
    pthread_mutex_lock(&lock);

    // while we are not done producing but we exceeded the size of buffer, we shall wait
    // short circuits when we exceed buffer
    while (curr_buf_size == max_buf_size && item_to_produce < total_items)
    {
      pthread_cond_wait(&can_produce, &lock);
    }

    if (item_to_produce >= total_items) // done producing all items
    {
      pthread_mutex_unlock(&lock);
      pthread_cond_signal(&can_consume); // signal we are done producing, so if there is remaining consumers not woken, they shall awake
      pthread_cond_signal(&can_produce); //sginal other producers that we are done
      break;
    }

    buffer[curr_buf_size++] = item_to_produce; //!  must be thread safe
    print_produced(item_to_produce, thread_id);
    item_to_produce++; //!  must be thread safe

    pthread_cond_signal(&can_consume); // announce to consumers that there is stuff in the buffer to consume
    pthread_mutex_unlock(&lock);
  }
  return 0;
}

// write function to be run by worker threads
// ensure that the workers call the function print_consumed when they consume an item
void *consume_requests_loop(void *data)
{
  int thread_id = *((int *)data);
  int curr_item;

  while (1)
  {
    pthread_mutex_lock(&lock);

    // while we got nothing to consume and we are not done producing items
    // short circuits when there is something new to consume
    while (curr_buf_size == 0 && item_to_produce < total_items)
    {
      pthread_cond_wait(&can_consume, &lock);
    }

    if (curr_buf_size == 0 && item_to_produce >= total_items) // done consuming everything that needed to be consumed
    {
      pthread_mutex_unlock(&lock);
      pthread_cond_signal(&can_consume); // wake up other consumer threads that there is nothing else to consume
      break;
    }

    curr_buf_size = curr_buf_size - 1;
    curr_item = buffer[curr_buf_size]; //!  must be thread safe
    print_consumed(curr_item, thread_id);

    pthread_cond_signal(&can_produce); // announce to producers that they can produce again
    pthread_mutex_unlock(&lock);
  }
  return 0;
}

int main(int argc, char *argv[])
{
  int *master_thread_id;
  pthread_t *master_thread;

  item_to_produce = 0;
  curr_buf_size = 0;

  int *worker_thread_id;
  pthread_t *worker_thread;

  // initialize locks
  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&can_consume, NULL);
  pthread_cond_init(&can_produce, NULL);

  int i;

  if (argc < 5)
  {
    printf("./master-worker #total_items #max_buf_size #num_workers #masters e.g. ./exe 10000 1000 4 3\n");
    exit(1);
  }
  else
  {
    num_masters = atoi(argv[4]);
    num_workers = atoi(argv[3]);
    total_items = atoi(argv[1]);
    max_buf_size = atoi(argv[2]);
  }

  buffer = (int *)malloc(sizeof(int) * max_buf_size);

  // create master producer threads
  master_thread_id = (int *)malloc(sizeof(int) * num_masters);
  master_thread = (pthread_t *)malloc(sizeof(pthread_t) * num_masters);
  for (i = 0; i < num_masters; i++)
    master_thread_id[i] = i;

  for (i = 0; i < num_masters; i++)
    pthread_create(&master_thread[i], NULL, generate_requests_loop, (void *)&master_thread_id[i]);

  // create worker consumer threads
  worker_thread_id = (int *)malloc(sizeof(int) * num_workers);
  worker_thread = (pthread_t *)malloc(sizeof(pthread_t) * num_workers);

  for (i = 0; i < num_workers; i++)
    worker_thread_id[i] = i;

  for (i = 0; i < num_workers; i++)
    pthread_create(&worker_thread[i], NULL, consume_requests_loop, (void *)&worker_thread_id[i]);

  // wait for all threads to complete
  for (i = 0; i < num_masters; i++)
  {
    pthread_join(master_thread[i], NULL);
    printf("master %d joined\n", i);
  }

  for (i = 0; i < num_workers; i++)
  {
    pthread_join(worker_thread[i], NULL);
    printf("worker %d joined\n", i);
  }

  /*----Deallocating Buffers---------------------*/
  pthread_mutex_destroy(&lock);
  free(buffer);

  free(master_thread);
  free(master_thread_id);

  free(worker_thread);
  free(worker_thread_id);

  return 0;
}
