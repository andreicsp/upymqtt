import asyncio
import logging
from upymqtt.client import MQTTClient
from upymqtt.log_handler import MQTTLogPublisher, QueueLogHandler
import sys

logger = logging.getLogger(__file__)


async def task():
    iters = 20
    while iters := iters - 1:
        logger.info(f"Hello, world! {iters} iterations left")
        logger.info(f"Second log message with {iters} iterations left")
        await asyncio.sleep(1)
    logger.info("Goodbye, world!")


async def main():
    handler = QueueLogHandler(maxlen=100)
    args = sys.argv[1:]
    arg_names = [args[i][2:] for i in range(0, len(args)) if i % 2 == 0]
    arg_values = [args[i] for i in range(0, len(args)) if i % 2 == 1]
    kwargs = dict(zip(arg_names, arg_values))

    mqtt_client = MQTTClient("ExampleClient", **kwargs)
    publisher = MQTTLogPublisher(
        client=mqtt_client, topic="telemetry", log_handler=handler
    )

    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return await asyncio.gather(
        publisher.run(), task()
    )

if __name__ == "__main__":
    asyncio.run(main())
