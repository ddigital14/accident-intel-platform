#!/bin/bash
# ============================================================================
# ACCIDENT INTEL PLATFORM - Deployment Script
# MD2020 / Donovan Digital Solutions
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh setup     # First-time setup (DB, Redis, schema, seeds, build)
#   ./deploy.sh start     # Start all services
#   ./deploy.sh stop      # Stop all services
#   ./deploy.sh restart   # Restart everything
#   ./deploy.sh test      # Run test suite
#   ./deploy.sh logs      # View application logs
#   ./deploy.sh status    # Check service status
#   ./deploy.sh reset-db  # Drop and recreate database (DESTRUCTIVE)
# ============================================================================

set -e

APP_NAME="accident-intel-platform"
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$APP_DIR/logs"
PID_DIR="$APP_DIR/.pids"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[AIP]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

# Load env
if [ -f "$APP_DIR/.env" ]; then
  set -a; source "$APP_DIR/.env"; set +a
else
  error ".env file not found. Copy .env.example to .env and configure it."
  exit 1
fi

mkdir -p "$LOG_DIR" "$PID_DIR"

# ============================================================================
# CHECK PREREQUISITES
# ============================================================================

check_prereqs() {
  log "Checking prerequisites..."
  local ok=true

  if ! command -v node &> /dev/null; then error "Node.js not found"; ok=false; fi
  if ! command -v npm &> /dev/null; then error "npm not found"; ok=false; fi
  if ! command -v psql &> /dev/null; then error "psql not found"; ok=false; fi
  if ! command -v redis-cli &> /dev/null; then error "redis-cli not found"; ok=false; fi

  node_ver=$(node -v | cut -d. -f1 | tr -d v)
  if [ "$node_ver" -lt 18 ]; then error "Node.js >= 18 required (found v$node_ver)"; ok=false; fi

  if $ok; then success "All prerequisites met"; fi
  $ok
}

# ============================================================================
# SETUP
# ============================================================================

do_setup() {
  log "═══════════════════════════════════════════════════"
  log " ACCIDENT INTEL PLATFORM - First-Time Setup"
  log "═══════════════════════════════════════════════════"

  check_prereqs

  # Install dependencies
  log "Installing Node.js dependencies..."
  cd "$APP_DIR" && npm ci --production 2>&1 | tail -3
  success "Dependencies installed"

  # Create database
  log "Setting up PostgreSQL database..."
  if ! psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    createdb -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" "$DB_NAME" 2>/dev/null || {
      log "Creating database user and DB..."
      sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';" 2>/dev/null || true
      sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;" 2>/dev/null || true
      sudo -u postgres psql -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" 2>/dev/null || true
      sudo -u postgres psql -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS \"postgis\";" 2>/dev/null || true
      sudo -u postgres psql -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\";" 2>/dev/null || true
    }
  fi

  # Run migrations
  log "Running database migrations..."
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" \
    -f "$APP_DIR/database/migrations/001_initial_schema.sql" 2>&1 | tail -5
  success "Schema created"

  # Run seeds
  log "Seeding metro areas..."
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" \
    -f "$APP_DIR/database/seeds/001_metro_areas.sql" 2>&1 | tail -3
  success "Metro areas seeded"

  log "Seeding test data..."
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" \
    -f "$APP_DIR/database/seeds/002_test_data.sql" 2>&1 | tail -3
  success "Test data seeded"

  # Build frontend
  log "Building React frontend..."
  cd "$APP_DIR/frontend" && npm ci && npm run build 2>&1 | tail -5
  success "Frontend built"

  # Verify Redis
  log "Checking Redis..."
  if redis-cli ping 2>/dev/null | grep -q PONG; then
    success "Redis is running"
  else
    warn "Redis not running. Start it with: redis-server --daemonize yes"
  fi

  log ""
  success "═══════════════════════════════════════════════════"
  success " Setup complete! Run: ./deploy.sh start"
  success "═══════════════════════════════════════════════════"
  log ""
  log "  Admin login: donovan@donovandigitalsolutions.com / Admin2026!"
  log "  Dashboard: http://localhost:${PORT:-3001}"
  log "  API: http://localhost:${PORT:-3001}${API_PREFIX:-/api/v1}"
  log ""
}

# ============================================================================
# START / STOP / RESTART
# ============================================================================

do_start() {
  log "Starting Accident Intel Platform..."

  # Check Redis
  if ! redis-cli ping 2>/dev/null | grep -q PONG; then
    warn "Starting Redis..."
    redis-server --daemonize yes --logfile "$LOG_DIR/redis.log"
  fi
  success "Redis running"

  # Start API server
  if [ -f "$PID_DIR/api.pid" ] && kill -0 $(cat "$PID_DIR/api.pid") 2>/dev/null; then
    warn "API server already running (PID $(cat "$PID_DIR/api.pid"))"
  else
    cd "$APP_DIR"
    NODE_ENV=production nohup node src/server.js >> "$LOG_DIR/api.log" 2>&1 &
    echo $! > "$PID_DIR/api.pid"
    sleep 2

    # Verify
    if curl -sf http://localhost:${PORT:-3001}/health > /dev/null; then
      success "API server running on port ${PORT:-3001} (PID $(cat "$PID_DIR/api.pid"))"
    else
      error "API server failed to start. Check logs: $LOG_DIR/api.log"
    fi
  fi

  # Start background worker
  if [ -f "$PID_DIR/worker.pid" ] && kill -0 $(cat "$PID_DIR/worker.pid") 2>/dev/null; then
    warn "Worker already running (PID $(cat "$PID_DIR/worker.pid"))"
  else
    cd "$APP_DIR"
    NODE_ENV=production nohup node src/ingestion/worker.js >> "$LOG_DIR/worker.log" 2>&1 &
    echo $! > "$PID_DIR/worker.pid"
    success "Worker started (PID $(cat "$PID_DIR/worker.pid"))"
  fi

  log ""
  success "Platform is live!"
  log "  Dashboard: http://localhost:${PORT:-3001}"
  log "  Health: http://localhost:${PORT:-3001}/health"
  log "  Logs: ./deploy.sh logs"
}

do_stop() {
  log "Stopping Accident Intel Platform..."

  for service in api worker; do
    if [ -f "$PID_DIR/$service.pid" ]; then
      pid=$(cat "$PID_DIR/$service.pid")
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        success "Stopped $service (PID $pid)"
      fi
      rm -f "$PID_DIR/$service.pid"
    fi
  done

  success "All services stopped"
}

do_restart() {
  do_stop
  sleep 2
  do_start
}

# ============================================================================
# OTHER COMMANDS
# ============================================================================

do_test() {
  log "Running test suite..."
  cd "$APP_DIR"
  node tests/test-platform.js
}

do_logs() {
  log "Tailing logs (Ctrl+C to stop)..."
  tail -f "$LOG_DIR"/api.log "$LOG_DIR"/worker.log 2>/dev/null || {
    error "No logs found. Has the server been started?"
  }
}

do_status() {
  log "Service Status:"
  for service in api worker; do
    if [ -f "$PID_DIR/$service.pid" ] && kill -0 $(cat "$PID_DIR/$service.pid") 2>/dev/null; then
      success "$service: running (PID $(cat "$PID_DIR/$service.pid"))"
    else
      warn "$service: not running"
    fi
  done

  if redis-cli ping 2>/dev/null | grep -q PONG; then
    success "redis: running"
  else
    warn "redis: not running"
  fi

  if PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1; then
    success "postgresql: running"
    PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -t -c "
      SELECT 'Incidents: ' || COUNT(*) FROM incidents
      UNION ALL SELECT 'Persons: ' || COUNT(*) FROM persons
      UNION ALL SELECT 'Users: ' || COUNT(*) FROM users
      UNION ALL SELECT 'Data Sources: ' || COUNT(*) FROM data_sources;
    "
  else
    warn "postgresql: not accessible"
  fi
}

do_reset_db() {
  warn "This will DROP and RECREATE the database. All data will be lost!"
  read -p "Are you sure? (type YES to confirm): " confirm
  if [ "$confirm" != "YES" ]; then
    log "Cancelled."
    exit 0
  fi

  do_stop
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
  do_setup
}

# ============================================================================
# DOCKER DEPLOYMENT (alternative)
# ============================================================================

do_docker() {
  log "Starting with Docker Compose..."
  cd "$APP_DIR"

  # Build frontend first
  if [ ! -d "frontend/build" ]; then
    log "Building frontend..."
    cd frontend && npm ci && npm run build && cd ..
  fi

  docker-compose up -d --build
  sleep 5

  if curl -sf http://localhost:${PORT:-3001}/health > /dev/null; then
    success "Platform running via Docker"
    log "  Dashboard: http://localhost:${PORT:-3001}"
  else
    error "Docker deployment may have issues. Check: docker-compose logs"
  fi
}

# ============================================================================
# MAIN
# ============================================================================

case "${1:-help}" in
  setup)    do_setup ;;
  start)    do_start ;;
  stop)     do_stop ;;
  restart)  do_restart ;;
  test)     do_test ;;
  logs)     do_logs ;;
  status)   do_status ;;
  reset-db) do_reset_db ;;
  docker)   do_docker ;;
  *)
    echo ""
    echo "  Accident Intel Platform - Deployment Script"
    echo ""
    echo "  Usage: $0 {setup|start|stop|restart|test|logs|status|reset-db|docker}"
    echo ""
    echo "  Commands:"
    echo "    setup     First-time setup (DB + schema + seeds + build)"
    echo "    start     Start API server + worker"
    echo "    stop      Stop all services"
    echo "    restart   Restart everything"
    echo "    test      Run test suite"
    echo "    logs      Tail application logs"
    echo "    status    Check service status + DB counts"
    echo "    reset-db  Drop and recreate database (DESTRUCTIVE)"
    echo "    docker    Deploy with Docker Compose"
    echo ""
    ;;
esac
