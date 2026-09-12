#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/task_wdt/task_wdt.h>
#include <stdbool.h>
#include <stdint.h>

LOG_MODULE_REGISTER(l5_task1, LOG_LEVEL_INF);

#define STACK_SIZE                  1024

#define QUEUE_DEPTH                 6
#define PRODUCER_COUNT              30
#define PRODUCER_PERIOD_MS          50

#define CONSUMER_NORMAL_SLEEP_MS    60
#define CONSUMER_STUCK_SLEEP_MS     2000

#define CONSUMER_WDT_TIMEOUT_MS     500

#define HEALTH_PERIOD_MS            100
#define HEALTH_WARN_PERCENT         75

struct sensor_msg {
	uint32_t timestamp_ms;
	int32_t value;
	uint8_t seq;
};

K_MSGQ_DEFINE(sensor_queue,
	      sizeof(struct sensor_msg),
	      QUEUE_DEPTH,
	      4);

/*
 * Thread stacks and control blocks.
 *
 * Threads are created dynamically from main() after the task watchdog
 * has been initialized.
 */
K_THREAD_STACK_DEFINE(producer_stack, STACK_SIZE);
K_THREAD_STACK_DEFINE(consumer_stack, STACK_SIZE);
K_THREAD_STACK_DEFINE(health_stack, STACK_SIZE);

static struct k_thread producer_thread_data;
static struct k_thread consumer_thread_data;
static struct k_thread health_thread_data;

static int consumer_wdt_id = -1;
static volatile bool watchdog_fired;

/*
 * Task watchdog callback.
 *
 * The callback intentionally does not reset the device.
 * The purpose of this assignment is to observe the watchdog firing.
 */
static void consumer_wdt_callback(int channel_id, void *user_data)
{
	ARG_UNUSED(user_data);

	watchdog_fired = true;

	LOG_ERR("[WDT] CALLBACK: consumer watchdog expired, channel=%d",
		channel_id);
}

/*
 * Producer thread.
 *
 * Produces data every 50 ms and places it into the message queue.
 */
static void producer_thread(void *p1, void *p2, void *p3)
{
	ARG_UNUSED(p1);
	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	LOG_INF("[PROD] started");

	for (uint8_t seq = 0; seq < PRODUCER_COUNT; seq++) {
		struct sensor_msg msg = {
			.timestamp_ms = k_uptime_get_32(),
			.value = 100 + seq,
			.seq = seq,
		};

		int ret = k_msgq_put(&sensor_queue,
				     &msg,
				     K_MSEC(200));

		if (ret == 0) {
			uint32_t used = k_msgq_num_used_get(&sensor_queue);

			LOG_INF("[PROD] sent seq=%u val=%d q=%u/%d",
				msg.seq,
				msg.value,
				used,
				QUEUE_DEPTH);
		} else {
			LOG_WRN("[PROD] queue put timeout seq=%u q=%u/%d",
				msg.seq,
				k_msgq_num_used_get(&sensor_queue),
				QUEUE_DEPTH);
		}

		k_msleep(PRODUCER_PERIOD_MS);
	}

	LOG_INF("[PROD] finished");
}

/*
 * Consumer thread.
 *
 * Normally processes messages and feeds its task watchdog.
 *
 * On the first message, the consumer deliberately sleeps for 2 seconds.
 * Since the watchdog timeout is 500 ms, the watchdog expires during
 * this sleep.
 */
static void consumer_thread(void *p1, void *p2, void *p3)
{
	ARG_UNUSED(p1);
	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	struct sensor_msg msg;
	bool stuck_simulated = false;

	LOG_INF("[CONS] started");

	/*
	 * The task watchdog has already been initialized by main().
	 * Therefore it is now safe for this thread to add its watchdog
	 * channel.
	 */
	consumer_wdt_id = task_wdt_add(CONSUMER_WDT_TIMEOUT_MS,
				       consumer_wdt_callback,
				       (void *)k_current_get());

	if (consumer_wdt_id < 0) {
		LOG_ERR("[CONS] task_wdt_add failed: %d",
			consumer_wdt_id);
		return;
	}

	LOG_INF("[CONS] watchdog channel=%d timeout=%d ms",
		consumer_wdt_id,
		CONSUMER_WDT_TIMEOUT_MS);

	while (true) {
		int ret = k_msgq_get(&sensor_queue,
				     &msg,
				     K_MSEC(300));

		if (ret != 0) {
			LOG_INF("[CONS] queue timeout, used=%u/%d",
				k_msgq_num_used_get(&sensor_queue),
				QUEUE_DEPTH);
			break;
		}

		uint32_t now = k_uptime_get_32();
		uint32_t latency = now - msg.timestamp_ms;

		LOG_INF("[CONS] got seq=%u val=%d q=%u/%d latency=%u ms",
			msg.seq,
			msg.value,
			k_msgq_num_used_get(&sensor_queue),
			QUEUE_DEPTH,
			latency);

		/*
		 * Deliberately create one long blocking period.
		 *
		 * The consumer does NOT feed the watchdog during this
		 * 2000 ms sleep. The watchdog timeout is only 500 ms.
		 */
		if (!stuck_simulated) {
			stuck_simulated = true;

			LOG_WRN("[CONS] SIMULATING STUCK CONSUMER: "
				"sleeping %d ms",
				CONSUMER_STUCK_SLEEP_MS);

			k_msleep(CONSUMER_STUCK_SLEEP_MS);

			LOG_INF("[CONS] stuck simulation finished");
		}

		/*
		 * After the simulated failure, the watchdog callback will
		 * have set watchdog_fired.
		 *
		 * If the watchdog has not fired, this is normal operation.
		 */
		if (!watchdog_fired) {
			int ret_feed = task_wdt_feed(consumer_wdt_id);

			if (ret_feed != 0) {
				LOG_ERR("[CONS] watchdog feed failed: %d",
					ret_feed);
			} else {
				LOG_INF("[CONS] watchdog fed");
			}
		} else {
			LOG_WRN("[CONS] watchdog already fired; "
				"stopping watchdog channel");

			int ret_delete = task_wdt_delete(consumer_wdt_id);

			if (ret_delete != 0) {
				LOG_ERR("[CONS] watchdog delete failed: %d",
					ret_delete);
			}

			consumer_wdt_id = -1;
			break;
		}

		k_msleep(CONSUMER_NORMAL_SLEEP_MS);
	}

	LOG_INF("[CONS] finished");
}

/*
 * Health-monitor thread.
 *
 * Warn whenever queue occupancy reaches at least 75%.
 */
static void health_thread(void *p1, void *p2, void *p3)
{
	ARG_UNUSED(p1);
	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	LOG_INF("[HEALTH] started, warning threshold=%d%%",
		HEALTH_WARN_PERCENT);

	while (true) {
		uint32_t used = k_msgq_num_used_get(&sensor_queue);
		uint32_t percent = (used * 100U) / QUEUE_DEPTH;

		if (percent >= HEALTH_WARN_PERCENT) {
			LOG_WRN("[HEALTH] WARNING queue usage=%u/%d (%u%%)",
				used,
				QUEUE_DEPTH,
				percent);
		} else {
			LOG_INF("[HEALTH] queue usage=%u/%d (%u%%)",
				used,
				QUEUE_DEPTH,
				percent);
		}

		k_msleep(HEALTH_PERIOD_MS);
	}
}

int main(void)
{
	int ret;

	LOG_INF("[L5] Task 1: reliability under pressure");

	/*
	 * Initialize the software task watchdog.
	 *
	 * NULL means no hardware watchdog fallback is requested.
	 */
	ret = task_wdt_init(NULL);

	if (ret != 0) {
		LOG_ERR("[L5] task_wdt_init failed: %d", ret);
		return 0;
	}

	LOG_INF("[L5] task watchdog initialized");

	/*
	 * Create all application threads only AFTER the task watchdog
	 * has been initialized.
	 */
	k_tid_t producer_tid = k_thread_create(
		&producer_thread_data,
		producer_stack,
		K_THREAD_STACK_SIZEOF(producer_stack),
		producer_thread,
		NULL,
		NULL,
		NULL,
		5,
		0,
		K_NO_WAIT);

	k_tid_t consumer_tid = k_thread_create(
		&consumer_thread_data,
		consumer_stack,
		K_THREAD_STACK_SIZEOF(consumer_stack),
		consumer_thread,
		NULL,
		NULL,
		NULL,
		5,
		0,
		K_NO_WAIT);

	k_tid_t health_tid = k_thread_create(
		&health_thread_data,
		health_stack,
		K_THREAD_STACK_SIZEOF(health_stack),
		health_thread,
		NULL,
		NULL,
		NULL,
		6,
		0,
		K_NO_WAIT);

	/*
	 * Give the threads useful names for diagnostics and the watchdog
	 * callback.
	 */
	k_thread_name_set(producer_tid, "l5_prod");
	k_thread_name_set(consumer_tid, "l5_cons");
	k_thread_name_set(health_tid, "l5_health");

	LOG_INF("[L5] producer, consumer, and health threads created");

	return 0;
}
