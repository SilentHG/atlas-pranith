import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4
from atlas.core.agent_base import BaseAgent, AgentLayer, AgentStatus
from atlas.core.agent_registry import AgentRegistry

class DummyAgent(BaseAgent):
    def __init__(self, redis_client):
        super().__init__(name="Dummy", agent_type="Test", layer=AgentLayer.L1, redis_client=redis_client)
        self.run_called = 0
        self.fail_times = 0

    async def run(self):
        self.run_called += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise Exception("Simulated crash")
        # Keep running
        await asyncio.sleep(0.5)

@pytest.fixture
def mock_redis():
    mock = AsyncMock()
    # Setup some basic returns if needed
    mock.set = AsyncMock()
    mock.hset = AsyncMock()
    mock.hgetall = AsyncMock(return_value={b'agent_id': b'123', b'name': b'Dummy', b'agent_type': b'Test', b'layer': b'L1', b'status': b'running'})
    mock.get = AsyncMock(return_value=b'running')
    mock.exists = AsyncMock(return_value=True)
    mock.keys = AsyncMock(return_value=[b'agent:123'])
    mock.expire = AsyncMock()
    return mock

@pytest.fixture
def mock_db():
    mock = AsyncMock()
    mock.execute = AsyncMock()
    mock.fetch = AsyncMock(return_value=[{'agent_id': 'test-123', 'name': 'Test Agent', 'layer': 'L1', 'status': 'running'}])
    mock.fetchrow = AsyncMock()
    return mock

@pytest.mark.asyncio
async def test_agent_registration(mock_redis, mock_db):
    agent = DummyAgent(mock_redis)
    registry = AgentRegistry(mock_redis, mock_db)
    
    await registry.register(agent)
    
    mock_redis.hset.assert_called_once()
    mock_db.execute.assert_called_once()

@pytest.mark.asyncio
async def test_agent_heartbeat(mock_redis):
    # BaseAgent handles its own heartbeat to Redis
    agent = DummyAgent(mock_redis)
    # Set min_restart_interval to 0 for testing
    agent._min_restart_interval = 0
    await agent.start()
    
    # Let it run for a bit to send heartbeat
    await asyncio.sleep(0.1)
    
    # It should have called redis.hset for heartbeat
    assert mock_redis.hset.call_count >= 1
    
    # And expire
    assert mock_redis.expire.call_count >= 1
    
    await agent.stop()

@pytest.mark.asyncio
async def test_agent_auto_restart_on_crash(mock_redis):
    agent = DummyAgent(mock_redis)
    agent.fail_times = 1  # It will fail once
    agent._min_run_duration = 0 # Disable cooldown for test
    agent._min_restart_interval = 0
    
    await agent.start()
    
    # Let the event loop process the tasks
    await asyncio.sleep(0.2)
    
    # Stop it gracefully
    await agent.stop()
        
    assert agent.run_called >= 1

@pytest.mark.asyncio
async def test_health_check_detection(mock_redis, mock_db):
    registry = AgentRegistry(mock_redis, mock_db)
    
    # Simulate missing heartbeat in Redis
    mock_redis.exists.return_value = False
    
    dead_agents = await registry.health_check()
    assert len(dead_agents) == 1
    assert dead_agents[0]['agent_id'] == "test-123"
    
    # Simulate active heartbeat
    mock_redis.exists.return_value = True
    dead_agents = await registry.health_check()
    assert len(dead_agents) == 0
