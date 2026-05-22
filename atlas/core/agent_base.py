from abc import ABC, abstractmethod
import asyncio
import uuid
from enum import Enum
from loguru import logger
from redis.asyncio import Redis


class AgentLayer(str, Enum):
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"
    L5 = "L5"
    L6 = "L6"
    L7 = "L7"


class AgentStatus(str, Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    INITIALIZING = "initializing"


class GovernanceViolation(RuntimeError):
    """Raised when an advisory-only agent attempts a forbidden action."""
    pass


class BaseAgent(ABC):
    def __init__(
        self,
        name: str,
        agent_type: str,
        layer: str | AgentLayer,
        redis_client: Redis,
        advisory_only: bool = False,
    ):
        self.agent_id: str = str(uuid.uuid4())
        self.name: str = name
        self.agent_type: str = agent_type
        self.layer: str = layer.value if isinstance(layer, AgentLayer) else layer
        self.advisory_only: bool = advisory_only

        self.status: str = AgentStatus.STOPPED.value

        self._redis: Redis = redis_client
        self._retry_count: int = 0
        self.MAX_RETRIES: int = 3

        # Minimum wall-clock duration for a single run() cycle.
        # If the agent's run() completes faster than this, _run_with_retry
        # will add a cooldown before exiting the task, preventing tight
        # restart loops in the supervisor.
        self._min_run_duration: float = 60.0  # seconds
        # Minimum interval between successive starts to avoid restart storms
        self._min_restart_interval: float = 30.0  # seconds
        self._last_start_time: float | None = None

        self._heartbeat_task: asyncio.Task | None = None
        self._main_task: asyncio.Task | None = None

    def _enforce_advisory_guard(self, action: str = "unknown") -> None:
        """Raise GovernanceViolation if this agent is advisory-only.

        Call this at the top of any method that mutates execution state,
        places orders, allocates capital, or overrides governance.
        """
        if self.advisory_only:
            raise GovernanceViolation(
                f"Agent '{self.name}' is advisory-only and cannot perform "
                f"action '{action}'. Advisory agents may only produce "
                f"persisted recommendations — never mutate system state."
            )

    async def start(self):
        if self.status == AgentStatus.RUNNING.value:
            return

        # Enforce a minimum restart interval to prevent supervisor restart storms
        now = asyncio.get_event_loop().time()
        if self._last_start_time is not None:
            elapsed = now - self._last_start_time
            if elapsed < self._min_restart_interval:
                wait = self._min_restart_interval - elapsed
                logger.info(
                    f"Agent {self.name}: delaying start {wait:.0f}s to enforce min_restart_interval"
                )
                await asyncio.sleep(wait)

        self.status = AgentStatus.RUNNING.value

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._main_task = asyncio.create_task(self._run_with_retry())
        self._last_start_time = asyncio.get_event_loop().time()

        logger.info(f"Agent {self.name} started.")

    async def stop(self):
        if self.status != AgentStatus.ERROR.value:
            self.status = AgentStatus.STOPPED.value

        pending_tasks = [
            task
            for task in (self._heartbeat_task, self._main_task)
            if task and not task.done()
        ]

        for task in pending_tasks:
            task.cancel()

        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        self._heartbeat_task = None
        self._main_task = None

        logger.info(f"Agent {self.name} stopped.")

    async def pause(self):
        self.status = AgentStatus.PAUSED.value
        logger.info(f"Agent {self.name} paused.")

    async def resume(self):
        self.status = AgentStatus.RUNNING.value
        logger.info(f"Agent {self.name} resumed.")

    async def _heartbeat_loop(self):
        try:
            while self.status in [
                AgentStatus.RUNNING.value,
                AgentStatus.PAUSED.value
            ]:
                key = f"agent:{self.agent_id}"

                await self._redis.hset(
                    key,
                    mapping={
                        "status": self.status,
                        "layer": self.layer,
                        "name": self.name,
                        "agent_type": self.agent_type,
                        "advisory_only": str(self.advisory_only),
                    },
                )

                await self._redis.expire(key, 30)

                logger.debug(
                    f"Heartbeat sent: {self.name} | {self.layer} | {self.status}"
                )

                await asyncio.sleep(10)

        except asyncio.CancelledError:
            logger.debug(f"Heartbeat loop cancelled for {self.name}")

    async def _run_with_retry(self):
        while (
            self._retry_count <= self.MAX_RETRIES
            and self.status == AgentStatus.RUNNING.value
        ):
            try:
                run_start = asyncio.get_event_loop().time()
                await self.run()
                run_elapsed = asyncio.get_event_loop().time() - run_start

                # Guard against tight restart loops: if the agent's run()
                # completed faster than _min_run_duration, pad with a sleep
                # so the task doesn't exit prematurely.
                if run_elapsed < self._min_run_duration:
                    pad = self._min_run_duration - run_elapsed
                    logger.info(
                        f"Agent {self.name} completed in {run_elapsed:.1f}s "
                        f"(< min {self._min_run_duration:.0f}s) — "
                        f"cooldown {pad:.0f}s before task exit"
                    )
                    await asyncio.sleep(pad)

                # Successful completion — mark agent as stopped (healthy idle)
                self.status = AgentStatus.STOPPED.value
                logger.info(f"Agent {self.name} completed run() successfully — entering stopped state")
                break

            except asyncio.CancelledError:
                break

            except Exception as e:
                self._retry_count += 1

                logger.error(
                    f"Error in agent {self.name}: {e} | Retry {self._retry_count}/{self.MAX_RETRIES}"
                )

                if self._retry_count <= self.MAX_RETRIES:
                    backoff = 2 ** self._retry_count
                    await asyncio.sleep(backoff)

                else:
                    self.status = AgentStatus.ERROR.value

                    logger.critical(
                        f"Agent {self.name} exceeded max retries."
                    )

                    # DO NOT call self.stop() here — that cancels the task
                    # we're running in, causing RecursionError during shutdown.
                    # The parent supervisor loop handles stopping dead agents.
                    break

    @abstractmethod
    async def run(self):
        pass