#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/zbus/zbus.h>

LOG_MODULE_REGISTER(demo, LOG_LEVEL_DBG);

#define STACK_SIZE        2048
#define SENSOR_COUNT      10
#define SENSOR_PERIOD_MS  100
#define LOGGER_QUEUE_SIZE 16

/* ================================================================== */
/* Sensor message                                                     */
/* ================================================================== */

struct sensor_data {
    int32_t temperature_mc;
    uint32_t timestamp_ms;
    uint8_t seq;
};

/* Forward declaration required before listener definition. */
static void display_listener_cb(const struct zbus_channel *chan);

/* ================================================================== */
/* zbus observers                                                     */
/* ================================================================== */

/* Fast display update. Runs synchronously in publisher context. */
ZBUS_LISTENER_DEFINE(display_lis, display_listener_cb);

/* Slow logger. Receives notifications of channel updates. */
ZBUS_SUBSCRIBER_DEFINE(logger_sub, LOGGER_QUEUE_SIZE);

/* ================================================================== */
/* zbus channel                                                       */
/* ================================================================== */

ZBUS_CHAN_DEFINE(sensor_chan,
                 struct sensor_data,
                 NULL, NULL,
                 ZBUS_OBSERVERS(display_lis, logger_sub),
                 ZBUS_MSG_INIT(.temperature_mc = 0,
                               .timestamp_ms = 0,
                               .seq = 0));

/* ================================================================== */
/* Display listener                                                   */
/* ================================================================== */

static void display_listener_cb(const struct zbus_channel *chan)
{
    const struct sensor_data *msg =
        (const struct sensor_data *)zbus_chan_const_msg(chan);

    LOG_INF("[DISPLAY] seq=%u temp=%d mC",
            msg->seq,
            msg->temperature_mc);
}

/* ================================================================== */
/* Sensor publisher                                                   */
/* ================================================================== */

static void sensor_thread_fn(void *p1, void *p2, void *p3)
{
    ARG_UNUSED(p1);
    ARG_UNUSED(p2);
    ARG_UNUSED(p3);

    k_thread_name_set(k_current_get(), "sensor");

    for (int i = 0; i < SENSOR_COUNT; i++) {
        struct sensor_data data = {
            .temperature_mc = 24000 + (i * 350),
            .timestamp_ms = k_uptime_get_32(),
            .seq = (uint8_t)i,
        };

        LOG_INF("[SENSOR] publish seq=%u temp=%d mC",
                data.seq,
                data.temperature_mc);

        int ret = zbus_chan_pub(&sensor_chan,
                                &data,
                                K_MSEC(100));

        if (ret != 0) {
            LOG_WRN("[SENSOR] publish failed ret=%d", ret);
        }

        k_msleep(SENSOR_PERIOD_MS);
    }

    LOG_INF("[SENSOR] done");
}

/* ================================================================== */
/* Slow logger subscriber                                             */
/* ================================================================== */

static void logger_thread_fn(void *p1, void *p2, void *p3)
{
    ARG_UNUSED(p1);
    ARG_UNUSED(p2);
    ARG_UNUSED(p3);

    k_thread_name_set(k_current_get(), "logger");

    const struct zbus_channel *chan;
    int received = 0;

    while (received < SENSOR_COUNT) {
		int ret = zbus_sub_wait(&logger_sub,
                        &chan,
                        K_MSEC(1500));

		if (ret != 0) {
			LOG_ERR("[LOGGER] zbus_sub_wait failed: %d", ret);
			break;
		}

		struct sensor_data msg;

		ret = zbus_chan_read(chan, &msg, K_MSEC(100));

		if (ret != 0) {
			LOG_ERR("[LOGGER] zbus_chan_read failed: %d", ret);
			continue;
		}

        received++;

        LOG_INF("[LOGGER] notification=%d seq=%u temp=%d mC latency=%ums",
                received,
				msg.seq,
                msg.temperature_mc,
                k_uptime_get_32() - msg.timestamp_ms);

        /* Deliberately slower than the sensor. */
        k_msleep(300);
    }

    LOG_INF("[LOGGER] done received=%d", received);
}

/* ================================================================== */
/* Threads                                                            */
/* ================================================================== */

K_THREAD_DEFINE(sensor_thread,
                STACK_SIZE,
                sensor_thread_fn,
                NULL, NULL, NULL,
                5, 0, 0);

K_THREAD_DEFINE(logger_thread,
                STACK_SIZE,
                logger_thread_fn,
                NULL, NULL, NULL,
                6, 0, 0);

/* ================================================================== */
/* Main                                                               */
/* ================================================================== */

int main(void)
{
    LOG_INF("=== L4 Task 1: Zbus Pub-Sub ===");
    LOG_INF("sensor publishes every %dms", SENSOR_PERIOD_MS);
    LOG_INF("display uses a zbus listener");
    LOG_INF("logger uses a zbus subscriber");

    return 0;
}