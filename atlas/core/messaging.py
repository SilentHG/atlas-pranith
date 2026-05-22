import json
import asyncio
from enum import Enum
from redis.asyncio import Redis

class Channel(str, Enum):
    MARKET_DATA = "market_data"
    STRATEGY_SIGNALS = "strategy_signals"
    RISK_ALERTS = "risk_alerts"
    EXECUTION_FILLS = "execution_fills"
    SYSTEM_EVENTS = "system_events"

class MessagingClient:
    def __init__(self, redis_pool: Redis):
        self.redis = redis_pool
        # Shared connection pool: pool_size=10 is assumed to be handled by the redis_pool provided
        
    async def publish(self, channel: Channel, message: dict) -> None:
        try:
            payload = json.dumps(message)
            await self.redis.publish(channel.value, payload)
        except Exception as e:
            logger.error(f"Messaging publish to {channel.value} failed: {e}")

    async def subscribe(self, channel: Channel, callback: callable) -> None:
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel.value)
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    success = False
                    for attempt in range(3):
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(data)
                            else:
                                callback(data)
                            success = True
                            break
                        except Exception as e:
                            logger.warning(f"PubSub callback attempt {attempt + 1}/3 failed: {e}")
                            await asyncio.sleep(0.5 * (attempt + 1))
                            
                    if not success:
                        logger.error(f"PubSub callback dead-lettered after 3 retries for channel {channel.value}")
                        await self.redis.rpush(f"dead_letter:{channel.value}", message["data"])
                except Exception as e:
                    logger.error(f"PubSub message processing error on {channel.value}: {e}")
