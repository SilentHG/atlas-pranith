"""
feature_evolution_engine.py — L7 Meta Agent for automatic feature evolution.

Capabilities:
- Automatic feature synthesis from existing features
- Feature mutation (parameter variation)
- Feature crossover (combine two features)
- Feature retirement (low-importance features)
- Regime-conditioned feature generation
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class FeatureEvolutionEngine(BaseAgent):
    """
    L7 Meta Agent — Evolves feature sets for improved strategy generation.
    """

    name = "FeatureEvolutionEngine"
    agent_type = "feature_evolution"
    layer = "L7"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 7200  # Every 2 hours

    async def run(self):
        logger.info(f"{self.name}: Starting feature evolution engine")

        while self.status == "running":
            try:
                await self._evolve_features()
            except Exception as e:
                logger.error(f"{self.name}: Feature evolution error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _evolve_features(self):
        """Run feature evolution cycle."""
        current_features = await self._load_feature_performance()
        if not current_features:
            logger.info(f"{self.name}: No features to evolve")
            return

        # Retire low-importance features
        retired = 0
        for feat in current_features:
            if feat.get("importance_score", 1.0) < 0.1 and feat.get("n_uses", 0) < 5:
                await self._retire_feature(feat["id"])
                retired += 1

        # Synthesize new features
        if len(current_features) >= 3:
            new_feature = await self._synthesize_feature(current_features)
            if new_feature:
                await self._register_feature(new_feature)

        # Mutate top features
        best = sorted(
            current_features,
            key=lambda f: f.get("importance_score", 0),
            reverse=True,
        )[:3]

        for parent in best:
            mutated = await self._mutate_feature(parent)
            if mutated:
                await self._register_feature(mutated, parent_id=parent["id"])

        logger.info(
            f"{self.name}: Feature evolution — "
            f"{retired} retired, {len(current_features)} active"
        )

    async def _load_feature_performance(self) -> list[dict]:
        """Load features with importance scores."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT id, feature_name, feature_importance_score,
                           n_uses, survival_rate, dominant_archetype
                    FROM feature_importance
                    WHERE feature_name NOT LIKE 'retired_%'
                    ORDER BY feature_importance_score DESC
                """)
            )
            return [
                {
                    "id": str(row[0]),
                    "name": str(row[1]),
                    "importance_score": float(row[2] or 0),
                    "n_uses": int(row[3] or 0),
                    "survival_rate": float(row[4] or 0),
                    "archetype": str(row[5]) if row[5] else "",
                }
                for row in r.fetchall()
            ]

    async def _synthesize_feature(self, existing: list[dict]) -> Optional[str]:
        """Synthesize a new feature by combining existing ones."""
        import random

        if len(existing) < 2:
            return None

        # Pick two features to combine
        candidates = random.sample(existing, min(2, len(existing)))
        operations = ["ratio", "difference", "sum", "product"]
        op = random.choice(operations)

        new_name = f"synthetic_{candidates[0]['name']}_{op}_{candidates[1]['name']}"
        return new_name

    async def _mutate_feature(self, parent: dict) -> Optional[str]:
        """Create a mutated variant of a feature."""
        import random

        mutations = [
            "_smoothed", "_lagged", "_accelerated", "_normalized",
            "_vol_adjusted", "_regime_conditioned",
        ]
        suffix = random.choice(mutations)
        return f"mutated_{parent['name']}{suffix}"

    async def _register_feature(
        self,
        feature_name: str,
        parent_id: Optional[str] = None,
    ):
        """Register a new evolved feature."""
        feature_id = uuid.uuid4()
        await self.db._execute_insert(
            """
            INSERT INTO feature_importance
                (id, feature_name, feature_importance_score,
                 n_uses, survival_rate, dominant_archetype, metadata)
            VALUES
                (:id, :name, 0.5, 1, 0.5, '',
                 CAST(:metadata AS jsonb))
            ON CONFLICT (feature_name) DO NOTHING
            """,
            {
                "id": feature_id,
                "name": feature_name,
                "metadata": json.dumps({
                    "evolved": True,
                    "parent_id": parent_id,
                    "evolved_at": datetime.now(timezone.utc).isoformat(),
                }),
            },
        )
        return feature_id

    async def _retire_feature(self, feature_id: str):
        """Retire a low-importance feature."""
        async with self.db.engine.connect() as conn:
            await conn.execute(
                text("""
                    UPDATE feature_importance
                    SET feature_name = CONCAT('retired_', feature_name),
                        metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:retired_at AS jsonb)
                    WHERE id = CAST(:id AS uuid)
                """),
                {
                    "id": feature_id,
                    "retired_at": json.dumps({"retired_at": datetime.now(timezone.utc).isoformat()}),
                },
            )
