"""
full_autonomous_cycle.py — Autonomous research-to-execution supervisor.

Starts the existing ATLAS agents that already implement:
  generate -> code -> backtest -> validate -> mutate -> execution -> tracking

This script does not reinvent pipeline logic. It keeps the existing agents alive
under one coordinator so the full cycle can run without a human in the loop.

Usage:
  python scripts/full_autonomous_cycle.py
  python scripts/full_autonomous_cycle.py --duration-minutes 60
"""

from __future__ import annotations

import argparse
import asyncio

from loguru import logger
from redis.asyncio import Redis

from atlas.agents.l2_strategy.coder_agent import CoderAgent
from atlas.agents.l2_strategy.ideator_agent_v2 import IdeatorAgentV2
from atlas.agents.l2_strategy.mutator_agent import MutatorAgent
from atlas.agents.l3_backtest.backtest_runner import BacktestRunner
from atlas.agents.l3_backtest.validator_agent import ValidatorAgent
from atlas.agents.l4_risk.risk_controller import RiskController
from atlas.agents.l5_execution.broker_adapter import SimulatorAdapter
from atlas.agents.l7_meta.mutation_pattern_agent import MutationPatternAgent
from atlas.agents.l5_execution.execution_gateway import ExecutionGateway
from atlas.agents.scouts.regime_scout import RegimeScout
from atlas.agents.scouts.liquidity_scout import LiquidityScout
from atlas.agents.scouts.correlation_scout import CorrelationScout
from atlas.agents.scouts.execution_scout import ExecutionScout
from atlas.agents.l3_validation.walk_forward_analyzer import WalkForwardAnalyzer
from atlas.agents.l3_validation.monte_carlo_simulator import MonteCarloSimulator
from atlas.agents.l3_validation.overfitting_detector import OverfittingDetector
from atlas.agents.l3_validation.regime_validator import RegimeValidator
from atlas.agents.l3_validation.cost_stress_tester import CostStressTester
from atlas.agents.l1_pattern.pattern_recognition_engine import PatternRecognitionEngine
from atlas.agents.l7_meta.feature_importance_engine import FeatureImportanceEngine
from atlas.agents.l7_meta.drift_detection_engine import DriftDetectionEngine
from atlas.agents.l7_meta.strategy_retirement_engine import StrategyRetirementEngine
from atlas.agents.l6_portfolio.portfolio_intelligence_engine import PortfolioIntelligenceEngine
from atlas.agents.l6_portfolio.capital_allocator import CapitalAllocator
from atlas.agents.l6_portfolio.ensemble_execution_engine import EnsembleExecutionEngine
from atlas.agents.l6_portfolio.advanced_portfolio_optimizer import AdvancedPortfolioOptimizer
from atlas.agents.l5_execution.execution_realism_engine import ExecutionRealismEngine
from atlas.agents.l4_risk.systemic_risk_engine import SystemicRiskEngine
from atlas.agents.l4_risk.stress_test_engine import StressTestEngine
from atlas.agents.l4_risk.capital_preservation_engine import CapitalPreservationEngine
from atlas.agents.l7_meta.replay_engine import ReplayEngine
from atlas.agents.l7_meta.system_health_engine import SystemHealthEngine
from atlas.agents.l7_meta.deployment_governor import DeploymentGovernor
from atlas.agents.l7_meta.prompt_evolution_engine import PromptEvolutionEngine
from atlas.agents.l7_meta.mutation_policy_engine import MutationPolicyEngine
from atlas.agents.l7_meta.feature_evolution_engine import FeatureEvolutionEngine
from atlas.agents.l7_meta.agent_performance_governor import AgentPerformanceGovernor
from atlas.agents.scouts.news_intelligence_engine import NewsIntelligenceEngine
from atlas.agents.scouts.source_reliability_engine import SourceReliabilityEngine
from atlas.agents.scouts.hypothesis_validation_engine import HypothesisValidationEngine
from atlas.config.settings import get_settings
from atlas.core.event_lineage import EventLineageClient
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.scripts.soak.phase24_monitor import SoakMonitor


def _build_agents(redis_client: Redis, db_client: TimescaleClient) -> list:
    broker = SimulatorAdapter(default_price=100.0, fill_latency_ms=10)
    risk = RiskController(redis_client, db_client)
    lineage = EventLineageClient(db_client)

    return [
        # Phase 10: Internal Scout Network
        RegimeScout(redis_client, db_client),
        LiquidityScout(redis_client, db_client),
        CorrelationScout(redis_client, db_client),
        ExecutionScout(redis_client, db_client),
        # Phase 11: Advanced Validation & Pattern Intelligence
        WalkForwardAnalyzer(redis_client, db_client),
        MonteCarloSimulator(redis_client, db_client),
        OverfittingDetector(redis_client, db_client),
        RegimeValidator(redis_client, db_client),
        CostStressTester(redis_client, db_client),
        PatternRecognitionEngine(redis_client, db_client),
        FeatureImportanceEngine(redis_client, db_client),
        # Phase 12: Portfolio Intelligence & Capital Realism
        PortfolioIntelligenceEngine(redis_client, db_client),
        CapitalAllocator(redis_client, db_client),
        EnsembleExecutionEngine(redis_client, db_client),
        ExecutionRealismEngine(redis_client, db_client),
        DriftDetectionEngine(redis_client, db_client),
        StrategyRetirementEngine(redis_client, db_client),
        # Phase 13: Production Governance & Reliability
        ReplayEngine(redis_client, db_client),
        SystemHealthEngine(redis_client, db_client),
        DeploymentGovernor(redis_client, db_client),
        # Phase 14: Portfolio Durability & Risk Intelligence
        SystemicRiskEngine(redis_client, db_client),
        StressTestEngine(redis_client, db_client),
        CapitalPreservationEngine(redis_client, db_client),
        AdvancedPortfolioOptimizer(redis_client, db_client),
        # Phase 15: True Meta-Learning
        PromptEvolutionEngine(redis_client, db_client),
        MutationPolicyEngine(redis_client, db_client),
        FeatureEvolutionEngine(redis_client, db_client),
        AgentPerformanceGovernor(redis_client, db_client),
        # Phase 16: External Intelligence Expansion
        NewsIntelligenceEngine(redis_client, db_client),
        SourceReliabilityEngine(redis_client, db_client),
        HypothesisValidationEngine(redis_client, db_client),
        # L2-L5: Core pipeline
        IdeatorAgentV2(0, 0.5, redis_client, db_client, mode="rich"),
        CoderAgent(redis_client, db_client),
        MutatorAgent(redis_client, db_client),
        BacktestRunner(redis_client),
        ValidatorAgent(redis_client, db_client),
        ExecutionGateway(redis_client, db_client, broker, risk, lineage),
        MutationPatternAgent(redis_client, db_client),
    ]


async def _start_agents(agents: list) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []
    for agent in agents:
        await agent.start()
        if getattr(agent, "_main_task", None):
            tasks.append(agent._main_task)
    return tasks


async def _stop_agents(agents: list) -> None:
    stop_results = await asyncio.gather(
        *(agent.stop() for agent in reversed(agents)),
        return_exceptions=True,
    )
    for agent, result in zip(reversed(agents), stop_results):
        if isinstance(result, Exception):
            logger.warning(f"Agent stop failed — {getattr(agent, 'name', agent)}: {result}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the autonomous ATLAS cycle")
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=60,
        help="How long to keep the supervisor running before shutting down",
    )
    args = parser.parse_args()

    settings = get_settings()
    db_client = TimescaleClient(settings.database_url)
    await db_client.connect()
    redis_client = Redis.from_url(settings.redis_url)

    agents = _build_agents(redis_client, db_client)

    # Phase 24: Soak monitoring (non-blocking — failures are logged, not fatal)
    monitor = SoakMonitor(db_client, redis_client, interval_seconds=300)
    try:
        await monitor.start()
        logger.info("SoakMonitor started — capturing metrics every 300s")
    except Exception as e:
        logger.error(f"SoakMonitor failed to start: {e} — continuing without monitoring")
        monitor = None

    logger.info(
        "Starting institutional cycle with Phases 13-18: "
        "portfolio -> risk -> meta-learn -> scout -> execute -> govern -> monitor"
    )
    tasks = await _start_agents(agents)

    runtime_seconds = max(1, args.duration_minutes) * 60

    try:
        start = asyncio.get_event_loop().time()
        _reported: set[int] = set()
        _restart_blocked_until: dict[int, float] = {}
        _restart_counts: dict[int, int] = {}
        while True:
            elapsed = asyncio.get_event_loop().time() - start

            if elapsed >= runtime_seconds:
                logger.info("Autonomous cycle duration reached; shutting down supervisor")
                break

            for i, task in enumerate(tasks):
                if task.done() and i not in _reported:
                    agent = agents[i] if i < len(agents) else None
                    agent_name = (
                        getattr(agent, "name", f"agent_{i}")
                        if agent
                        else f"task_{i}"
                    )

                    exc = task.exception()
                    if task.cancelled():
                        logger.warning(f"Agent task cancelled early — {agent_name}")
                    elif exc is not None:
                        logger.warning(f"Agent task exited early — {agent_name}: {exc}")
                    else:
                        logger.warning(f"Agent task completed early — {agent_name}")

                    # AUTO-RESTART: attempt to revive the dead agent
                    restart_succeeded = False
                    if agent is not None:
                        now = asyncio.get_event_loop().time()
                        blocked_until = _restart_blocked_until.get(i, 0.0)

                        if now < blocked_until:
                            remaining = int(blocked_until - now)
                            logger.warning(
                                f"Restart cooldown active for {agent_name} — "
                                f"{remaining}s remaining"
                            )
                        else:
                            try:
                                logger.info(
                                    f"Auto-restarting agent #{_restart_counts.get(i, 0) + 1} — {agent_name}"
                                )
                                await agent.stop()
                                await agent.start()
                                if agent._main_task and not agent._main_task.done():
                                    tasks[i] = agent._main_task
                                    _reported.discard(i)
                                    restart_succeeded = True
                                    logger.info(f"Agent restarted successfully — {agent_name}")
                                else:
                                    logger.error(f"Agent restart failed (no new task) — {agent_name}")
                            except Exception as restart_exc:
                                logger.error(f"Agent restart raised exception — {agent_name}: {restart_exc}")
                            finally:
                                # ALWAYS apply exponential backoff after any restart attempt
                                # (success or failure) to prevent tight loops from fast-exiting agents.
                                _restart_counts[i] = _restart_counts.get(i, 0) + 1
                                _restart_blocked_until[i] = now + min(
                                    60 * (2 ** min(_restart_counts[i] - 1, 4)), 600
                                )
                                if restart_succeeded:
                                    logger.info(
                                        f"Restart cooldown {min(60 * (2 ** min(_restart_counts[i] - 1, 4)), 600)}s "
                                        f"for {agent_name}"
                                    )

                    if not restart_succeeded:
                        _reported.add(i)

            await asyncio.sleep(5)
    finally:
        if monitor:
            await monitor.stop()
        await _stop_agents(agents)
        try:
            await redis_client.aclose()
        except Exception as exc:
            logger.warning(f"Redis close failed: {exc}")

        try:
            await db_client.close()
        except Exception as exc:
            logger.warning(f"Database close failed: {exc}")


if __name__ == "__main__":
    asyncio.run(main())