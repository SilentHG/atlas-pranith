"""
pattern_recognition_engine.py — Phase 11: Unsupervised Pattern Discovery.

Implements institutional pattern recognition using:
  - Isolation Forest: anomaly detection on enriched features
  - DBSCAN: clustering of similar market regimes
  - PCA: latent structure discovery and dimensionality reduction
  - Statistical anomaly scoring: z-score / IQR-based outlier detection

Inputs:
  - features_wide (enriched features from TimescaleDB)
  - regime intelligence from RegimeScout
  - volatility state, liquidity state, order flow

Outputs:
  - anomaly clusters with confidence scores
  - structural market patterns (repeating motifs)
  - novel alpha hypotheses for Ideator consumption

Persists into pattern_memory and Redis channel pattern_discoveries.
"""

import asyncio
import json
import math
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.cluster import DBSCAN
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler
    from sklearn.impute import SimpleImputer

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.warning("sklearn not available — pattern_recognition_engine will use statistical methods only")


class PatternRecognitionEngine(BaseAgent):
    """
    Pattern Recognition Engine — discovers latent market structure and novel alpha hypotheses.

    Configuration:
      - run_interval: seconds between analysis cycles (default 3600 = 1 hour)
      - anomaly_contamination: expected proportion of anomalies (default 0.05)
      - dbscan_eps: DBSCAN neighborhood radius (default 0.5)
      - dbscan_min_samples: minimum samples for DBSCAN cluster (default 5)
      - pca_components: number of PCA components (default 5)
    """

    name = "PatternRecognitionEngine"
    agent_type = "pattern_recognition"
    layer = "L1"

    def __init__(
        self,
        redis_client=None,
        db_client=None,
        run_interval: int = 3600,
        anomaly_contamination: float = 0.05,
        dbscan_eps: float = 0.5,
        dbscan_min_samples: int = 5,
        pca_components: int = 5,
    ):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval
        self.contamination = anomaly_contamination
        self.dbscan_eps = dbscan_eps
        self.dbscan_min_samples = dbscan_min_samples
        self.pca_components = pca_components

        # Selected feature columns for pattern analysis
        self.FEATURE_COLS = [
            "returns", "log_returns", "rsi_14", "macd", "macd_signal",
            "price_vs_vwap_pct", "ema_spread_pct", "relative_volume",
            "bollinger_band_position", "rolling_volatility", "trend_strength",
        ]

    async def run(self):
        logger.info(f"{self.name}: starting pattern analysis loop (every {self.run_interval}s)")
        while self.status == "running":
            try:
                results = await self._analyze_patterns()
                if results:
                    await self._persist_discoveries(results)
                    await self._publish(results)
                    logger.info(f"{self.name}: discovered {len(results)} patterns")
            except Exception as e:
                logger.error(f"{self.name}: analysis failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _analyze_patterns(self) -> list[dict]:
        """Run full pattern recognition pipeline."""
        if not self.db:
            return []

        # Fetch features for all symbols
        symbols = await self._fetch_active_symbols()
        if not symbols:
            logger.debug(f"{self.name}: no symbols with feature data")
            return []

        all_discoveries = []

        for symbol in symbols[:10]:  # limit to top 10 symbols
            df = await self._fetch_feature_data(symbol)
            if df is None or len(df) < 50:
                continue

            discoveries = self._analyze_symbol(df, symbol)
            all_discoveries.extend(discoveries)

        return all_discoveries

    async def _fetch_active_symbols(self) -> list[str]:
        """Fetch symbols with available features."""
        try:
            async with self.db.engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT DISTINCT symbol
                        FROM features_wide
                        WHERE time > NOW() - INTERVAL '7 days'
                        LIMIT 20
                    """)
                )
                return [row[0] for row in result.fetchall()]
        except Exception:
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    async def _fetch_feature_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch enriched features from features_wide."""
        try:
            async with self.db.engine.connect() as conn:
                cols = ", ".join(f"\"{c}\"" for c in self.FEATURE_COLS if c != "time")
                result = await conn.execute(
                    text(f"""
                        SELECT time, {cols}
                        FROM features_wide
                        WHERE symbol = :symbol
                        ORDER BY time DESC
                        LIMIT 1000
                    """),
                    {"symbol": symbol},
                )
                rows = result.fetchall()
                if not rows:
                    return None
                col_names = ["time"] + [c for c in self.FEATURE_COLS if c != "time"]
                df = pd.DataFrame(rows, columns=col_names)
                numeric_cols = [c for c in col_names if c != "time"]
                for c in numeric_cols:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                df = df.dropna()
                return df
        except Exception as e:
            logger.warning(f"{self.name}: fetch failed for {symbol}: {e}")
            return None

    def _analyze_symbol(self, df: pd.DataFrame, symbol: str) -> list[dict]:
        """Run pattern detection on a single symbol's feature data."""
        discoveries = []
        feature_cols = [c for c in self.FEATURE_COLS if c in df.columns]

        if len(df) < 30 or len(feature_cols) < 3:
            return discoveries

        X = df[feature_cols].values
        close = df.get("returns", pd.Series(np.zeros(len(df)))).values

        # ---- 1. Isolation Forest Anomaly Detection ----
        anomaly_indices = self._detect_anomalies_isolation_forest(X)
        if anomaly_indices:
            anomaly_rets = close[anomaly_indices]
            avg_anomaly_ret = float(np.mean(anomaly_rets)) if len(anomaly_rets) > 0 else 0.0
            directions = []
            if avg_anomaly_ret > 0.001:
                directions.append("bullish")
            elif avg_anomaly_ret < -0.001:
                directions.append("bearish")

            discoveries.append({
                "pattern_type": "anomaly_cluster",
                "symbol": symbol,
                "confidence": round(min(1.0, len(anomaly_indices) / len(df) * 5), 4),
                "sample_size": int(len(anomaly_indices)),
                "avg_anomaly_return": round(avg_anomaly_ret, 6),
                "direction": directions,
                "method": "isolation_forest",
                "detail": f"Detected {len(anomaly_indices)} anomalies in {symbol} features",
            })

        # ---- 2. DBSCAN Clustering ----
        clusters = self._cluster_dbscan(X)
        if clusters:
            for cluster_id, members in clusters.items():
                if len(members) < 5:
                    continue
                cluster_rets = close[members]
                avg_ret = float(np.mean(cluster_rets))
                cluster_features = np.mean(X[members], axis=0)
                top_features = self._top_features(feature_cols, cluster_features)

                discoveries.append({
                    "pattern_type": "market_cluster",
                    "symbol": symbol,
                    "confidence": round(min(1.0, len(members) / 50), 4),
                    "sample_size": int(len(members)),
                    "avg_cluster_return": round(avg_ret, 6),
                    "cluster_id": int(cluster_id),
                    "top_features": top_features[:3],
                    "method": "dbscan",
                    "detail": f"Cluster {cluster_id}: {len(members)} bars, top features: {', '.join(top_features[:3])}",
                })

        # ---- 3. PCA Latent Structure ----
        pca_result = self._pca_analysis(X, feature_cols)
        if pca_result:
            discoveries.append({
                "pattern_type": "latent_structure",
                "symbol": symbol,
                "confidence": round(pca_result["explained_variance_ratio"], 4),
                "sample_size": len(df),
                "pca_components": self.pca_components,
                "top_loadings": pca_result["top_loadings"],
                "method": "pca",
                "detail": f"PCA: {pca_result['explained_variance_ratio']:.1%} variance in {self.pca_components} components",
            })

        # ---- 4. Statistical Anomaly Scoring ----
        stat_anomalies = self._statistical_anomalies(X, feature_cols)
        for anomaly in stat_anomalies:
            discoveries.append({
                "pattern_type": "statistical_anomaly",
                "symbol": symbol,
                "confidence": round(anomaly["z_score"] / 5.0, 4),  # normalize to [0, 1]
                "sample_size": anomaly["count"],
                "feature": anomaly["feature"],
                "z_score": round(anomaly["z_score"], 2),
                "method": "z_score",
                "detail": f"Z-score {anomaly['z_score']:.1f} on feature {anomaly['feature']}",
            })

        return discoveries

    def _detect_anomalies_isolation_forest(self, X: np.ndarray) -> list[int]:
        """Detect anomalies using Isolation Forest (or simple statistical fallback)."""
        if HAS_SKLEARN and len(X) >= 50:
            try:
                model = IsolationForest(
                    contamination=self.contamination,
                    random_state=42,
                    n_estimators=100,
                )
                preds = model.fit_predict(X)
                return list(np.where(preds == -1)[0])
            except Exception:
                pass

        # Fallback: simple z-score anomaly detection
        z_scores = np.abs((X - np.mean(X, axis=0)) / (np.std(X, axis=0) + 1e-10))
        max_z = np.max(z_scores, axis=1)
        threshold = np.percentile(max_z, 95)
        return list(np.where(max_z > threshold)[0])

    def _cluster_dbscan(self, X: np.ndarray) -> dict[int, list[int]]:
        """Cluster data using DBSCAN."""
        if not HAS_SKLEARN:
            return {}

        try:
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            model = DBSCAN(eps=self.dbscan_eps, min_samples=self.dbscan_min_samples)
            labels = model.fit_predict(X_scaled)

            clusters: dict[int, list[int]] = {}
            for i, label in enumerate(labels):
                if label >= 0:
                    clusters.setdefault(int(label), []).append(i)
            return clusters
        except Exception:
            return {}

    def _pca_analysis(self, X: np.ndarray, feature_cols: list[str]) -> Optional[dict]:
        """Run PCA and extract top loadings."""
        if not HAS_SKLEARN or len(X) < 20 or len(feature_cols) < 3:
            return None

        try:
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            n_comp = min(self.pca_components, len(feature_cols), len(X) - 1)
            if n_comp < 2:
                return None

            pca = PCA(n_components=n_comp)
            pca.fit(X_scaled)

            # Top loadings from first component
            loadings = np.abs(pca.components_[0])
            top_idx = np.argsort(loadings)[::-1][:5]
            top_loadings = [feature_cols[i] for i in top_idx]

            return {
                "explained_variance_ratio": float(np.sum(pca.explained_variance_ratio_)),
                "top_loadings": top_loadings,
                "n_components": n_comp,
            }
        except Exception:
            return None

    def _statistical_anomalies(self, X: np.ndarray, feature_cols: list[str]) -> list[dict]:
        """Detect features with extreme statistical anomalies."""
        anomalies = []
        for j, col in enumerate(feature_cols):
            vals = X[:, j]
            q25, q75 = np.percentile(vals, [25, 75])
            iqr = q75 - q25
            if iqr == 0:
                continue
            z = abs(np.mean(vals)) / (np.std(vals) + 1e-10)
            if z > 2.0:
                n_extreme = int(np.sum(np.abs(vals) > q75 + 1.5 * iqr))
                anomalies.append({
                    "feature": col,
                    "z_score": float(z),
                    "count": n_extreme,
                    "direction": "elevated" if np.mean(vals) > 0 else "depressed",
                })
        return anomalies[:5]

    def _top_features(self, feature_cols: list[str], loadings: np.ndarray) -> list[str]:
        """Return top features by absolute loading weight."""
        idx = np.argsort(np.abs(loadings))[::-1]
        return [feature_cols[i] for i in idx]

    async def _persist_discoveries(self, discoveries: list[dict]) -> None:
        """Save pattern discoveries to pattern_memory."""
        if not self.db:
            return
        for d in discoveries:
            try:
                pattern_type = d["pattern_type"]
                symbol = d.get("symbol", "all")
                confidence = d["confidence"]
                direction = d.get("direction")
                if isinstance(direction, list) and len(direction) > 0:
                    archetype = direction[0]
                elif isinstance(direction, str):
                    archetype = direction
                else:
                    archetype = "unknown"

                await self.db._execute_insert(
                    """
                    INSERT INTO pattern_memory
                        (id, pattern_type, archetype, feature_family, asset_class,
                         timeframe, composite_score_avg, confidence_score, recommendation,
                         motif_details, detected_at, updated_at)
                    VALUES
                        (:id, :pt, :arch, :ff, :ac,
                         :tf, :csa, :cs, :rec,
                         :md, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        updated_at = NOW(),
                        confidence_score = EXCLUDED.confidence_score,
                        recommendation = EXCLUDED.recommendation
                    """,
                    {
                        "id": str(uuid.uuid4()),
                        "pt": pattern_type,
                        "arch": archetype,
                        "ff": [d.get("method", "unknown")] if d.get("method") else ["statistical"],
                        "ac": "crypto",
                        "tf": "1h",
                        "csa": d.get("avg_anomaly_return", d.get("avg_cluster_return", 0)),
                        "cs": confidence,
                        "rec": d.get("detail", ""),
                        "md": safe_json_dumps({k: v for k, v in d.items() if k not in ("pattern_type", "confidence", "detail")}),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist discover failed: {e}")

    async def _publish(self, discoveries: list[dict]) -> None:
        """Publish discoveries to Redis for real-time consumption."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "pattern_discoveries",
                "detected_at": datetime.utcnow().isoformat(),
                "total_discoveries": len(discoveries),
                "pattern_types": list(set(d["pattern_type"] for d in discoveries)),
                "top_discoveries": [
                    {
                        "pattern_type": d["pattern_type"],
                        "method": d.get("method", "unknown"),
                        "confidence": d["confidence"],
                        "detail": d.get("detail", "")[:200],
                    }
                    for d in discoveries[:5]
                ],
            }
            await self._redis.publish("pattern_discoveries", json.dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
