#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>

LOG_MODULE_REGISTER(demo, LOG_LEVEL_DBG);

#define STACK_SIZE 1024
#define THREAD_PRIORITY 5
#define NUM_ITERATIONS 10000

static volatile int shared_counter;

void thread_a_fn(void *p1, void *p2, void *p3)
{
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        int temp = shared_counter;

        /* Deliberately create a scheduling window */
        k_yield();

        shared_counter = temp + 1;
    }

    LOG_INF("Thread A done");
}

void thread_b_fn(void *p1, void *p2, void *p3)
{
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        int temp = shared_counter;

        /* Deliberately create a scheduling window */
        k_yield();

        shared_counter = temp + 1;
    }

    LOG_INF("Thread B done");
}

K_THREAD_DEFINE(thread_a, STACK_SIZE, thread_a_fn,
                NULL, NULL, NULL, THREAD_PRIORITY, 0, 0);

K_THREAD_DEFINE(thread_b, STACK_SIZE, thread_b_fn,
                NULL, NULL, NULL, THREAD_PRIORITY, 0, 0);

int main(void)
{
    LOG_INF("Starting race-condition experiment");

    /* Give the worker threads time to finish. */
    k_msleep(1000);

    LOG_INF("Expected counter: %d", NUM_ITERATIONS * 2);
    LOG_INF("Actual counter: %d", shared_counter);

    return 0;
}