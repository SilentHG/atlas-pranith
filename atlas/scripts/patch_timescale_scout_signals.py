"""
patches data/storage/timescale_client.py to add scout_signals auto-mirror.
Phase 25B — Approach B: modify _execute_insert() to auto-mirror scout inserts.
Handles Windows (\r\n) line endings.
"""
import re
import sys

FILE = "data/storage/timescale_client.py"

with open(FILE, "r", encoding="utf-8") as f:
    content = f.read()

count = 0

# ──────────────────────────────────────────────
# EDIT 1: Add _SCOUT_TABLE_MIRROR_MAP after _extract_table_name_from_insert
# ──────────────────────────────────────────────
# Anchor: return "unknown"\r\n\r\n\r\nclass BarData
MIRROR_MAP = """
# Scout table -> scout_signals mirror configuration.
# Maps scout-specific insert targets to the columns needed by scout_signals.
_SCOUT_TABLE_MIRROR_MAP: dict[str, dict[str, Any]] = {
    "market_regime_memory": {
        "source": "regime_scout",
        "symbol_key": "symbol",
        "signal_type": "regime",
        "confidence_key": "confidence_score",
        "signal_data_keys": [
            "volatility_regime", "trend_regime", "liquidity_regime",
            "correlation_regime", "realized_volatility", "relative_volume",
            "atr_percentile", "compression_detected", "expansion_detected",
            "vwap_deviation_pct",
        ],
    },
    "liquidity_intelligence": {
        "source": "liquidity_scout",
        "symbol_key": "symbol",
        "signal_type": "liquidity",
        "confidence_key": "liquidity_score",
        "signal_data_keys": [
            "avg_spread_bps", "depth_imbalance", "slippage_risk",
            "market_impact_estimate", "liquidity_regime",
        ],
    },
    "correlation_memory": {
        "source": "correlation_scout",
        "symbol_key": None,
        "signal_type": "correlation",
        "confidence_key": "avg_pairwise_corr",
        "signal_data_keys": [
            "cluster_name", "dominant_factor", "risk_state",
            "symbols_analyzed", "correlation_spike_detected",
        ],
    },
    "execution_intelligence": {
        "source": "execution_scout",
        "symbol_key": "symbol",
        "signal_type": "execution",
        "confidence_key": "fill_quality_score",
        "signal_data_keys": [
            "avg_slippage_bps", "fill_latency_ms", "rejection_rate",
            "execution_regime", "sample_size",
        ],
    },
    "external_scout_memory": {
        "source": "source",
        "symbol_key": None,
        "signal_type": "external",
        "confidence_key": "hypothesis_score",
        "signal_data_keys": [
            "source_sub", "sentiment", "source_reliability",
            "signal_direction", "mentioned_tickers",
        ],
    },
}
"""

# Find anchor in both \r\n and \n variants
idx1 = content.find('    return "unknown"\r\n\r\n\r\nclass BarData')
if idx1 >= 0:
    insert_point = idx1 + len('    return "unknown"\r\n')
    content = content[:insert_point] + MIRROR_MAP + content[insert_point:]
    count += 1
    print("EDIT 1: _SCOUT_TABLE_MIRROR_MAP inserted")
else:
    # try \n variant
    idx1 = content.find('    return "unknown"\n\n\nclass BarData')
    if idx1 >= 0:
        insert_point = idx1 + len('    return "unknown"\n')
        content = content[:insert_point] + MIRROR_MAP + content[insert_point:]
        count += 1
        print("EDIT 1: _SCOUT_TABLE_MIRROR_MAP inserted")
    else:
        print("EDIT 1: Anchor not found!")
        idx = content.find('return "unknown"')
        if idx >= 0:
            print(repr(content[idx:idx+200]))
        sys.exit(1)

# ──────────────────────────────────────────────
# EDIT 2: Add _mirror_to_scout_signals method after _quarantine_scout_payload
# ──────────────────────────────────────────────
MIRROR_METHOD = """
    async def _mirror_to_scout_signals(self, table_name: str, params: dict[str, Any]) -> None:
        \"\"\"Auto-mirror scout inserts to scout_signals for pipeline consumption.\"\"\"
        config = _SCOUT_TABLE_MIRROR_MAP.get(table_name)
        if config is None:
            return

        raw_source = params.get(config[\"source\"], \"unknown\")
        symbol = None
        if config.get(\"symbol_key\"):
            symbol = params.get(config[\"symbol_key\"])

        confidence = None
        if config.get(\"confidence_key\"):
            try:
                confidence = float(params.get(config[\"confidence_key\"], 0) or 0)
            except (TypeError, ValueError):
                confidence = 0.0

        signal_data = {}
        for key in config.get(\"signal_data_keys\", []):
            if key in params:
                val = params[key]
                if isinstance(val, (list, dict)):
                    import json
                    signal_data[key] = json.loads(json.dumps(val, default=str))
                else:
                    signal_data[key] = val

        insert_query = \"\"\"
            INSERT INTO scout_signals (source, symbol, signal_type, confidence_score, signal_data)
            VALUES (:source, :symbol, :signal_type, :confidence_score, CAST(:signal_data AS jsonb))
        \"\"\"
        async with self.engine.begin() as conn:
            try:
                await conn.execute(
                    text(insert_query),
                    {
                        \"source\": str(raw_source),
                        \"symbol\": str(symbol) if symbol else None,
                        \"signal_type\": config[\"signal_type\"],
                        \"confidence_score\": confidence,
                        \"signal_data\": safe_json_dumps(signal_data),
                    },
                )
            except Exception:
                # Mirror failures are non-fatal -- do not poison the primary insert
                pass
"""

# Anchor: \n    async def fetchval(self, query...
anchor2 = '\n    async def fetchval(self, query: str, params: Optional[Dict[str, Any]] = None):'
# Need to find the closing of _quarantine_scout_payload
# Look for:\n            )\n\n    async def fetchval
idx2 = content.find('\n            )\n\n    async def fetchval')
if idx2 >= 0:
    insert_point = idx2 + len('\n            )\n')
    content = content[:insert_point] + MIRROR_METHOD + content[insert_point:]
    count += 1
    print("EDIT 2: _mirror_to_scout_signals method inserted")
else:
    # Try \r\n variant
    idx2 = content.find('\r\n            )\r\n\r\n    async def fetchval')
    if idx2 >= 0:
        insert_point = idx2 + len('\r\n            )\r\n')
        content = content[:insert_point] + MIRROR_METHOD.replace('\n', '\r\n') + content[insert_point:]
        count += 1
        print("EDIT 2: _mirror_to_scout_signals method inserted")
    else:
        print("EDIT 2: Anchor not found!")
        idx = content.find('async def fetchval')
        if idx >= 0:
            print("Context around fetchval:", repr(content[idx-100:idx+100]))
        sys.exit(1)

# ──────────────────────────────────────────────
# EDIT 3: Call _mirror_to_scout_signals in _execute_insert after successful insert
# ──────────────────────────────────────────────
# Find the rc == 0 block and add the mirror call after it
MIRROR_CALL = """
                else:
                    # Successful insert -- mirror to scout_signals if applicable
                    table_name = _extract_table_name_from_insert(query)
                    await self._mirror_to_scout_signals(table_name, normalized_params)
"""

anchor3 = '                        "reason": "zero_rowcount",\n                        },\n                    )\n            except Exception as e:'
anchor3_windows = '                        "reason": "zero_rowcount",\r\n                        },\r\n                    )\r\n            except Exception as e:'

idx3 = content.find(anchor3_windows)
if idx3 >= 0:
    replacement = anchor3_windows.replace(
        '                    )\r\n            except Exception as e:',
        '                    )\r\n' + MIRROR_CALL.replace('\n', '\r\n') + '\r\n            except Exception as e:'
    )
    content = content.replace(anchor3_windows, replacement, 1)
    count += 1
    print("EDIT 3: Mirror call added to _execute_insert")
else:
    idx3 = content.find(anchor3)
    if idx3 >= 0:
        replacement = anchor3.replace(
            '                    )\n            except Exception as e:',
            '                    )\n' + MIRROR_CALL + '\n            except Exception as e:'
        )
        content = content.replace(anchor3, replacement, 1)
        count += 1
        print("EDIT 3: Mirror call added to _execute_insert")
    else:
        print("EDIT 3: Anchor not found!")
        idx = content.find('"reason": "zero_rowcount"')
        if idx >= 0:
            print("Context:", repr(content[idx:idx+300]))
        sys.exit(1)

# Write back
with open(FILE, "w", encoding="utf-8") as f:
    f.write(content)

print(f"\nDONE: {count}/3 edits applied successfully!")
