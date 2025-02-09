import asyncio
from logging import Handler
from collections import deque

from upymqtt.client import MQTTClient


class QueueLogHandler(Handler):
    def __init__(self, maxlen=100):
        super().__init__()
        self.queue = deque([], maxlen)
        self.update_event = asyncio.Event()

    def emit(self, record):
        if record.levelno >= self.level:
            self.queue.append(self.format(record))
            self.update_event.set()

    def drain(self):
        while self.queue:
            yield self.queue.popleft()
        self.update_event.clear()


class MQTTLogPublisher:
    def __init__(self, client: MQTTClient, topic: str, log_handler: QueueLogHandler):
        self.client = client
        self.log_handler = log_handler
        self._is_running = False
        self._topic = topic

    async def run(self):
        self._is_running = True
        while self._is_running:
            await self.log_handler.update_event.wait()
            for record in self.log_handler.drain():
                await self.client.publish(
                    topic=self._topic,
                    message=record,
                )
