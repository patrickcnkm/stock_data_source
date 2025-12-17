#!/bin/bash

# Quant Platform Startup Script
# Starts all services: Docker (Prometheus/Grafana, mandatory) and FastAPI server
# Requires: Python 3.11+, Docker Desktop

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FASTAPI_PORT=8000
GRAFANA_PORT=3000
PROMETHEUS_PORT=9090
VENV_DIR="${SCRIPT_DIR}/.venv"

# Function to print colored messages
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a port is in use
port_in_use() {
    lsof -ti:$1 >/dev/null 2>&1
}

# Function to wait for a service to be ready
wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=30
    local attempt=0

    info "Waiting for $name to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "$url" >/dev/null 2>&1; then
            success "$name is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    warning "$name did not become ready after ${max_attempts} seconds"
    return 1
}

# Function to cleanup on exit
cleanup() {
    info "Shutting down services..."
    
    # Kill FastAPI server if running
    if [ -n "$FASTAPI_PID" ]; then
        kill "$FASTAPI_PID" 2>/dev/null || true
        wait "$FASTAPI_PID" 2>/dev/null || true
    fi
    
    # Stop Docker services
    if command_exists docker-compose || command_exists docker; then
        cd "$SCRIPT_DIR"
        if command_exists docker-compose; then
            docker-compose down 2>/dev/null || true
        elif command_exists docker; then
            docker compose down 2>/dev/null || true
        fi
    fi
    
    success "Cleanup complete"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM EXIT

# Main execution
main() {
    echo "=========================================="
    echo "  Quant Platform Startup Script"
    echo "=========================================="
    echo ""

    # Check prerequisites
    info "Checking prerequisites..."

    # Check Python
    if ! command_exists python3; then
        error "Python 3 is not installed. Please install Python 3.11+ first."
        exit 1
    fi
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    success "Python found: $PYTHON_VERSION"

    # Check Docker (mandatory)
    DOCKER_DESKTOP_APP="/Applications/Docker.app"
    
    if ! command_exists docker; then
        # Check if Docker Desktop app exists but docker command is not in PATH
        if [ -d "$DOCKER_DESKTOP_APP" ]; then
            warning "Docker Desktop is installed but 'docker' command is not in PATH."
            warning "Please ensure Docker Desktop is in your PATH or restart your terminal."
            error "Docker is required for this project."
            exit 1
        else
            error "Docker is not installed. Docker is required for this project."
            echo ""
            error "Please install Docker Desktop:"
            echo "  1. Visit: https://www.docker.com/products/docker-desktop"
            echo "  2. Download and install Docker Desktop for Mac"
            echo "  3. Start Docker Desktop from Applications"
            echo "  4. Run this script again"
            echo ""
            exit 1
        fi
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        warning "Docker daemon is not running."
        
        # Check if Docker Desktop app exists
        if [ -d "$DOCKER_DESKTOP_APP" ]; then
            info "Docker Desktop is installed. Attempting to start it..."
            
            # Try to open Docker Desktop
            if open "$DOCKER_DESKTOP_APP" 2>/dev/null; then
                success "Docker Desktop is starting..."
                info "Waiting for Docker daemon to be ready (this may take 30-60 seconds)..."
                
                # Wait for Docker daemon to start (max 60 seconds)
                local max_wait=60
                local waited=0
                while [ $waited -lt $max_wait ]; do
                    if docker info >/dev/null 2>&1; then
                        success "Docker daemon is ready!"
                        break
                    fi
                    sleep 2
                    waited=$((waited + 2))
                    if [ $((waited % 10)) -eq 0 ]; then
                        info "Still waiting... (${waited}s/${max_wait}s)"
                    fi
                done
                
                # Final check
                if ! docker info >/dev/null 2>&1; then
                    error "Docker daemon did not start after ${max_wait} seconds."
                    error "Please ensure Docker Desktop is running and try again."
                    exit 1
                fi
            else
                error "Failed to start Docker Desktop automatically."
                error "Please start Docker Desktop manually from Applications and try again."
                exit 1
            fi
        else
            error "Docker Desktop is not installed."
            echo ""
            error "Please install Docker Desktop:"
            echo "  1. Visit: https://www.docker.com/products/docker-desktop"
            echo "  2. Download and install Docker Desktop for Mac"
            echo "  3. Start Docker Desktop from Applications"
            echo "  4. Run this script again"
            echo ""
            exit 1
        fi
    fi
    success "Docker is available"

    # Check if virtual environment exists
    if [ ! -d "$VENV_DIR" ]; then
        warning "Virtual environment not found. Creating one..."
        python3 -m venv "$VENV_DIR"
        success "Virtual environment created"
    fi

    # Activate virtual environment
    info "Activating virtual environment..."
    source "${VENV_DIR}/bin/activate"

    # Check and install/upgrade dependencies if needed
    info "Checking dependencies..."
    NEEDS_INSTALL=false
    
    # Check if requirements.txt is newer than installed marker
    if [ ! -f "${VENV_DIR}/.installed" ] || [ "${SCRIPT_DIR}/requirements.txt" -nt "${VENV_DIR}/.installed" ]; then
        NEEDS_INSTALL=true
    else
        # Verify key packages are actually installed
        if ! python3 -c "import fastapi, uvicorn, duckdb, pandas" 2>/dev/null; then
            NEEDS_INSTALL=true
        fi
    fi
    
    if [ "$NEEDS_INSTALL" = true ]; then
        info "Installing/updating dependencies..."
        pip install -q --upgrade pip
        pip install -q -r "${SCRIPT_DIR}/requirements.txt"
        touch "${VENV_DIR}/.installed"
        success "Dependencies installed"
    else
        success "Dependencies are up to date"
    fi

    # Check if database is initialized
    if [ ! -f "${SCRIPT_DIR}/data/quant.duckdb" ]; then
        warning "Database not found. Initializing..."
        python3 "${SCRIPT_DIR}/scripts/init_duckdb.py"
        success "Database initialized"
    fi

    # Check port availability
    if port_in_use $FASTAPI_PORT; then
        error "Port $FASTAPI_PORT is already in use. Please stop the existing service or change the port."
        exit 1
    fi

    # Start Docker services (mandatory)
    info "Starting Docker services (Prometheus/Grafana)..."
    cd "$SCRIPT_DIR"
    
    # Clean up any orphaned containers and stale state
    info "Cleaning up Docker Compose state..."
    if command_exists docker-compose; then
        docker-compose down --remove-orphans 2>/dev/null || true
        docker-compose rm -f 2>/dev/null || true
    elif command_exists docker; then
        docker compose down --remove-orphans 2>/dev/null || true
        docker compose rm -f 2>/dev/null || true
    fi
    
    # Remove any existing containers with our names (in case they exist outside compose)
    docker rm -f quant_prometheus quant_grafana 2>/dev/null || true
    
    # Use docker-compose or docker compose
    info "Starting Docker Compose services..."
    if command_exists docker-compose; then
        if ! docker-compose up -d; then
            warning "Docker Compose failed, trying to start containers manually..."
            # Fallback: start containers manually
            docker run -d --name quant_prometheus -p $PROMETHEUS_PORT:9090 \
                -v "${SCRIPT_DIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
                prom/prometheus:v2.48.0 2>/dev/null || true
            docker run -d --name quant_grafana -p $GRAFANA_PORT:3000 \
                -e GF_SECURITY_ADMIN_USER=admin \
                -e GF_SECURITY_ADMIN_PASSWORD=admin \
                -e GF_USERS_ALLOW_SIGN_UP=false \
                grafana/grafana:10.2.0 2>/dev/null || true
        fi
    elif command_exists docker; then
        if ! docker compose up -d; then
            warning "Docker Compose failed, trying to start containers manually..."
            # Fallback: start containers manually
            docker run -d --name quant_prometheus -p $PROMETHEUS_PORT:9090 \
                -v "${SCRIPT_DIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
                prom/prometheus:v2.48.0 2>/dev/null || true
            docker run -d --name quant_grafana -p $GRAFANA_PORT:3000 \
                -e GF_SECURITY_ADMIN_USER=admin \
                -e GF_SECURITY_ADMIN_PASSWORD=admin \
                -e GF_USERS_ALLOW_SIGN_UP=false \
                grafana/grafana:10.2.0 2>/dev/null || true
        fi
    fi
    
    # Wait a bit for services to start
    sleep 3
    
    # Check Docker services status
    info "Checking Docker services status..."
    if command_exists docker-compose; then
        docker-compose ps 2>/dev/null || docker ps | grep -E "(quant_prometheus|quant_grafana)" || true
    elif command_exists docker; then
        docker compose ps 2>/dev/null || docker ps | grep -E "(quant_prometheus|quant_grafana)" || true
    fi
    
    success "Docker services started"
    info "  - Grafana: http://localhost:$GRAFANA_PORT (admin/admin)"
    info "  - Prometheus: http://localhost:$PROMETHEUS_PORT"

    # Start FastAPI server
    info "Starting FastAPI server..."
    cd "$SCRIPT_DIR"
    
    # Set PYTHONPATH
    export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
    
    # Start uvicorn in background
    uvicorn app.main:app --host 0.0.0.0 --port $FASTAPI_PORT > /tmp/quant_fastapi.log 2>&1 &
    FASTAPI_PID=$!
    
    # Wait for server to be ready
    if wait_for_service "http://localhost:$FASTAPI_PORT/api/health" "FastAPI server"; then
        success "FastAPI server started (PID: $FASTAPI_PID)"
    else
        error "FastAPI server failed to start. Check logs: /tmp/quant_fastapi.log"
        exit 1
    fi

    # Print summary
    echo ""
    echo "=========================================="
    success "All services started successfully!"
    echo "=========================================="
    echo ""
    echo "Services:"
    echo "  - Dashboard: http://localhost:$FASTAPI_PORT/dashboard"
    echo "  - API Docs: http://localhost:$FASTAPI_PORT/docs"
    echo "  - Health: http://localhost:$FASTAPI_PORT/api/health"
    echo "  - Metrics: http://localhost:$FASTAPI_PORT/metrics"
    echo "  - Grafana: http://localhost:$GRAFANA_PORT (admin/admin)"
    echo "  - Prometheus: http://localhost:$PROMETHEUS_PORT"
    echo ""
    echo "Press Ctrl+C to stop all services"
    echo ""

    # Keep script running
    wait $FASTAPI_PID
}

# Run main function
main "$@"
