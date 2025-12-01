import asyncio
from aiokafka import AIOKafkaProducer
from typing import Optional

class Kafka:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        for _ in range(10):
            try:
                self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
                await self._producer.start()
                return
            except Exception:
                await asyncio.sleep(1)
        raise RuntimeError("Kafka not ready")

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def send(self, topic: str, value: bytes):
        assert self._producer, "Kafka producer not started"
        await self._producer.send_and_wait(topic, value)