$ErrorActionPreference = "Stop"
$ROOT = "C:\Pranith\Freelancing_Projects\05-11-2026-Amit-ATLAS"

function Write-Step {
    param($msg)
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

function Write-Pass {
    param($msg)
    Write-Host "[PASS] $msg" -ForegroundColor Green
}

function Write-Fail {
    param($msg)
    Write-Host "[FAIL] $msg" -ForegroundColor Red
}


Write-Step "Checking Docker..."

docker version --format '{{.Server.Version}}' > $null 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Fail "Docker Desktop is not running."
    exit 1
}

Write-Pass "Docker OK"

Write-Step "Starting Docker Compose..."
Set-Location $ROOT
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Fail "docker-compose failed."
    exit 1
}
Write-Pass "Docker Compose started"

Write-Step "Waiting for TimescaleDB..."
$retries = 30
$ready = $false

for ($i = 1; $i -le $retries; $i++) {

    $result = docker exec atlas_timescaledb pg_isready -U postgres -d atlas 2>&1

    if ($result -match "accepting connections") {
        $ready = $true
        break
    }

    Start-Sleep -Seconds 3
}

if (-not $ready) {
    Write-Fail "TimescaleDB not ready."
    docker logs atlas_timescaledb --tail 30
    exit 1
}

Write-Pass "TimescaleDB ready"

Write-Step "Checking Redis..."
$redisPing = docker exec atlas_redis redis-cli ping 2>&1

if ($redisPing -notmatch "PONG") {
    Write-Fail "Redis failed."
    docker logs atlas_redis --tail 30
    exit 1
}

Write-Pass "Redis ready"

Write-Step "Installing requirements..."
pip install -r "$ROOT\requirements.txt"
pip install pytest pytest-asyncio asyncpg "sqlalchemy[asyncio]"
pip install -e $ROOT

Write-Step "Testing atlas import..."
python -c "import atlas; print('OK')"

Write-Step "Running tests..."
$env:PYTHONPATH = $ROOT
pytest "$ROOT\atlas\tests\test_db.py" -v
pytest "$ROOT\atlas\tests\test_agent_base.py" -v

Write-Host "`nATLAS LOCAL DEV READY" -ForegroundColor Green