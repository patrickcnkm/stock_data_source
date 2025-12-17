#!/bin/bash

# Quant Platform Stop Script
# Stops all services: FastAPI server and Docker services

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

# Function to kill process on a port
kill_port() {
    local port=$1
    local service_name=$2
    
    if port_in_use $port; then
        info "Stopping $service_name on port $port..."
        local pids=$(lsof -ti:$port 2>/dev/null || true)
        if [ -n "$pids" ]; then
            for pid in $pids; do
                kill "$pid" 2>/dev/null || true
            done
            # Wait a bit for graceful shutdown
            sleep 2
            # Force kill if still running
            for pid in $pids; do
                if kill -0 "$pid" 2>/dev/null; then
                    warning "Force killing process $pid..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
            done
            success "$service_name stopped"
        fi
    else
        info "$service_name is not running on port $port"
    fi
}

# Main execution
main() {
    echo "=========================================="
    echo "  Quant Platform Stop Script"
    echo "=========================================="
    echo ""

    # Stop FastAPI server
    kill_port $FASTAPI_PORT "FastAPI server"

    # Stop Docker services
    if command_exists docker; then
        if docker info >/dev/null 2>&1; then
            info "Stopping Docker services (Prometheus/Grafana)..."
            cd "$SCRIPT_DIR"
            
            # Use docker-compose or docker compose
            if command_exists docker-compose; then
                docker-compose down 2>/dev/null || warning "Docker Compose down failed or no services running"
            elif command_exists docker; then
                docker compose down 2>/dev/null || warning "Docker Compose down failed or no services running"
            fi
            
            success "Docker services stopped"
            
            # Check if any other containers are running (excluding our project containers)
            info "Checking if Docker Desktop can be shut down..."
            local running_containers=$(docker ps --format '{{.Names}}' 2>/dev/null || echo "")
            local other_containers=$(echo "$running_containers" | grep -v -E '^(quant_prometheus|quant_grafana)$' | grep -v '^$' || true)
            
            if [ -z "$other_containers" ]; then
                # No other containers running, check if Docker Desktop is running
                if pgrep -f "Docker Desktop" >/dev/null 2>&1 || pgrep -f "com.docker.backend" >/dev/null 2>&1; then
                    info "No other Docker containers running. Shutting down Docker Desktop..."
                    
                    # Try graceful quit first (wait a moment for containers to fully stop)
                    sleep 1
                    if osascript -e 'quit app "Docker"' 2>/dev/null; then
                        # Wait a bit for graceful shutdown
                        sleep 2
                        if pgrep -f "Docker Desktop" >/dev/null 2>&1; then
                            warning "Graceful quit taking time, force quitting Docker Desktop..."
                            pkill -9 -f "Docker Desktop" 2>/dev/null || true
                            pkill -9 -f "com.docker.backend" 2>/dev/null || true
                            success "Docker Desktop force quit"
                        else
                            success "Docker Desktop quit successfully"
                        fi
                    else
                        # Force quit if graceful quit fails
                        warning "Graceful quit failed, force quitting Docker Desktop..."
                        pkill -9 -f "Docker Desktop" 2>/dev/null || true
                        pkill -9 -f "com.docker.backend" 2>/dev/null || true
                        success "Docker Desktop force quit"
                    fi
                else
                    info "Docker Desktop is not running"
                fi
            else
                info "Other Docker containers are running, keeping Docker Desktop active:"
                echo "$other_containers" | while read -r container; do
                    if [ -n "$container" ]; then
                        echo "  - $container"
                    fi
                done
            fi
        else
            warning "Docker daemon is not running"
        fi
    else
        info "Docker is not installed, skipping Docker services"
    fi

    # Check if any services are still running
    echo ""
    info "Checking for remaining services..."
    
    local still_running=false
    
    if port_in_use $FASTAPI_PORT; then
        warning "FastAPI server may still be running on port $FASTAPI_PORT"
        still_running=true
    fi
    
    if command_exists docker && docker info >/dev/null 2>&1; then
        cd "$SCRIPT_DIR"
        if command_exists docker-compose; then
            if docker-compose ps 2>/dev/null | grep -q "Up"; then
                warning "Some Docker services may still be running"
                still_running=true
            fi
        elif command_exists docker; then
            if docker compose ps 2>/dev/null | grep -q "Up"; then
                warning "Some Docker services may still be running"
                still_running=true
            fi
        fi
    fi

    # Summary
    echo ""
    echo "=========================================="
    if [ "$still_running" = false ]; then
        success "All services stopped successfully!"
        if command_exists docker && ! docker info >/dev/null 2>&1; then
            info "Docker Desktop has been shut down"
        fi
    else
        warning "Most services stopped. Check above for any remaining processes."
    fi
    echo "=========================================="
    echo ""
    
    # Show how to check for remaining processes
    info "To check for remaining processes:"
    echo "  - FastAPI: lsof -ti:$FASTAPI_PORT"
    echo "  - Docker: docker-compose ps"
    echo ""
}

# Run main function
main "$@"
