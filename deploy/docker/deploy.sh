#!/bin/bash
# =============================================================================
# UDPS One-Command Deployment Script
# =============================================================================
# Deploy UDPS (Unified Data Platform Service) with a single command.
#
# USAGE:
#   ./deploy.sh              # Full deployment (build + setup + start)
#   ./deploy.sh --quick      # Quick start (skip build if images exist)
#   ./deploy.sh --status     # Check deployment status
#   ./deploy.sh --stop       # Stop all services
#   ./deploy.sh --clean      # Stop and remove all data
#   ./deploy.sh --logs       # View live logs
#   ./deploy.sh --test       # Run deployment validation tests
#   ./deploy.sh --help       # Show this help
#
# REQUIREMENTS:
#   - Docker 20.10+ with Compose V2
#   - 8GB RAM minimum (16GB recommended)
#   - 10GB free disk space
#   - Ports 50060, 8443, 9090, 8081 available
#   - sbt (for building from source)
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="${UDPS_VERSION:-0.1.0}"
IMAGE_NAME="${UDPS_IMAGE:-seri-sa/udps}"
HEALTH_URL="http://localhost:8081/health/live"
HEALTH_MAX_ATTEMPTS=30
HEALTH_INTERVAL=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
print_banner() {
    echo ""
    echo -e "${CYAN}+============================================================================+${NC}"
    echo -e "${CYAN}|${NC}                                                                              ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD}██╗   ██╗██████╗ ██████╗ ███████╗${NC}    Unified Data Platform Service        ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD}██║   ██║██╔══██╗██╔══██╗██╔════╝${NC}    Version: ${VERSION}                      ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD}██║   ██║██║  ██║██████╔╝███████╗${NC}                                          ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD}██║   ██║██║  ██║██╔═══╝ ╚════██║${NC}    Data Intelligence at Scale            ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD}╚██████╔╝██████╔╝██║     ███████║${NC}                                          ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}   ${BOLD} ╚═════╝ ╚═════╝ ╚═╝     ╚══════╝${NC}                                          ${CYAN}|${NC}"
    echo -e "${CYAN}|${NC}                                                                              ${CYAN}|${NC}"
    echo -e "${CYAN}+============================================================================+${NC}"
    echo ""
}

print_step() {
    echo ""
    echo -e "${BLUE}------------------------------------------------------------------------------${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}------------------------------------------------------------------------------${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}[OK] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

print_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

print_info() {
    echo -e "${CYAN}[INFO] $1${NC}"
}

show_help() {
    echo "UDPS Deployment Script"
    echo ""
    echo "Usage: ./deploy.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  (no args)     Full deployment: preflight checks, build, setup, start"
    echo "  --quick       Quick start: skip build if images exist"
    echo "  --status      Check deployment status and service health"
    echo "  --stop        Stop all services (preserves data)"
    echo "  --clean       Stop services and remove all data"
    echo "  --logs        View live logs from all services"
    echo "  --test        Run deployment validation tests"
    echo "  --help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh                    # First-time deployment"
    echo "  ./deploy.sh --quick            # Restart after reboot"
    echo "  ./deploy.sh --status           # Check if everything is running"
    echo "  ./deploy.sh --logs             # Troubleshoot issues"
    echo ""
    echo "Environment Variables:"
    echo "  UDPS_VERSION    Version tag (default: 0.1.0)"
    echo "  UDPS_IMAGE      Docker image name (default: seri-sa/udps)"
    echo ""
}

# -----------------------------------------------------------------------------
# Preflight Checks
# -----------------------------------------------------------------------------
check_prerequisites() {
    print_step "Checking Prerequisites"

    local errors=0

    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        echo "       Install Docker: https://docs.docker.com/get-docker/"
        errors=$((errors + 1))
    else
        local docker_version
        docker_version=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "0.0.0")
        local major_version
        major_version=$(echo "$docker_version" | cut -d. -f1)
        if [[ "$major_version" -lt 20 ]]; then
            print_warning "Docker version $docker_version is old (20.10+ recommended)"
        else
            print_success "Docker $docker_version installed"
        fi
    fi

    # Check Docker Compose V2
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose V2 is not available"
        echo "       Docker Compose V2 is included in Docker Desktop"
        echo "       For Linux: https://docs.docker.com/compose/install/"
        errors=$((errors + 1))
    else
        local compose_version
        compose_version=$(docker compose version --short 2>/dev/null || echo "unknown")
        print_success "Docker Compose $compose_version installed"
    fi

    # Check Docker daemon
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        echo "       Start Docker Desktop or run: sudo systemctl start docker"
        errors=$((errors + 1))
    else
        print_success "Docker daemon is running"
    fi

    # Check available memory
    if [[ -f /proc/meminfo ]]; then
        local total_mem_kb
        total_mem_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "0")
        local total_mem_gb=$((total_mem_kb / 1024 / 1024))
        if [[ "$total_mem_gb" -lt 6 ]]; then
            print_warning "Only ${total_mem_gb}GB RAM available (8GB+ recommended)"
        else
            print_success "${total_mem_gb}GB RAM available"
        fi
    fi

    # Check available disk space
    local free_space_kb
    free_space_kb=$(df "$SCRIPT_DIR" 2>/dev/null | tail -1 | awk '{print $4}' || echo "0")
    local free_space_gb=$((free_space_kb / 1024 / 1024))
    if [[ "$free_space_gb" -lt 8 ]]; then
        print_warning "Only ${free_space_gb}GB disk space available (10GB+ recommended)"
    else
        print_success "${free_space_gb}GB disk space available"
    fi

    # Check port availability
    local required_ports=(50060 8443 9090 8081)
    for port in "${required_ports[@]}"; do
        if ss -tuln 2>/dev/null | grep -q ":$port " || netstat -tuln 2>/dev/null | grep -q ":$port "; then
            print_error "Port $port is already in use"
            echo "       Find process: sudo lsof -i :$port"
            errors=$((errors + 1))
        else
            print_success "Port $port is available"
        fi
    done

    # Check OpenSSL (for certificate generation)
    if ! command -v openssl &> /dev/null; then
        print_error "OpenSSL is not installed (required for certificate generation)"
        errors=$((errors + 1))
    else
        print_success "OpenSSL is installed"
    fi

    # Check sbt (optional, needed for source build)
    if ! command -v sbt &> /dev/null; then
        print_warning "sbt is not installed (required for building from source)"
        echo "       Install sbt: https://www.scala-sbt.org/download.html"
        echo "       Build will be skipped if Docker image exists"
    else
        print_success "sbt is installed"
    fi

    if [[ $errors -gt 0 ]]; then
        echo ""
        print_error "Preflight checks failed with $errors error(s)"
        echo ""
        echo "Please fix the issues above and try again."
        exit 1
    fi

    print_success "All preflight checks passed"
}

# -----------------------------------------------------------------------------
# Build Docker Images
# -----------------------------------------------------------------------------
build_images() {
    print_step "Building Docker Images"

    local skip_build=false

    # Check if image already exists
    if docker image inspect "${IMAGE_NAME}:${VERSION}" &>/dev/null; then
        if [[ "$QUICK_MODE" == "true" ]]; then
            print_info "Docker image ${IMAGE_NAME}:${VERSION} already exists, skipping build (--quick mode)"
            skip_build=true
        else
            print_info "Docker image exists but will be rebuilt"
        fi
    fi

    if [[ "$skip_build" == "false" ]]; then
        print_info "Building UDPS (this may take several minutes on first run)..."
        echo ""

        # Step 1: Build the fat JAR with sbt
        if command -v sbt &> /dev/null; then
            print_info "Running sbt assembly..."
            cd "$REPO_ROOT"
            if ! sbt "api/assembly"; then
                print_error "sbt assembly failed"
                echo ""
                echo "Troubleshooting:"
                echo "  1. Check JDK is installed: java -version"
                echo "  2. Check sbt version: sbt sbtVersion"
                echo "  3. Try: sbt clean \"api/assembly\""
                exit 1
            fi
            print_success "sbt assembly completed"
        else
            print_warning "sbt not found, checking for pre-built JAR..."
            local jar_count
            jar_count=$(find "$REPO_ROOT" -path "*/udps-api/target/scala-*/udps-api-assembly-*.jar" 2>/dev/null | wc -l)
            if [[ "$jar_count" -eq 0 ]]; then
                print_error "No pre-built JAR found and sbt is not installed"
                echo "       Install sbt or provide a pre-built JAR"
                exit 1
            fi
            print_info "Using pre-built JAR"
        fi

        # Step 2: Build Docker image
        print_info "Building Docker image..."
        cd "$REPO_ROOT"
        if ! docker build -t "${IMAGE_NAME}:${VERSION}" -t "${IMAGE_NAME}:latest" .; then
            print_error "Docker build failed"
            echo ""
            echo "Troubleshooting:"
            echo "  1. Check Docker is running: docker info"
            echo "  2. Check available disk space: df -h"
            echo "  3. Check Dockerfile exists at repo root"
            exit 1
        fi

        print_success "Docker image ${IMAGE_NAME}:${VERSION} built successfully"
    fi
}

# -----------------------------------------------------------------------------
# Setup Environment
# -----------------------------------------------------------------------------
setup_environment() {
    print_step "Setting Up Environment"

    cd "$SCRIPT_DIR"

    local need_setup=false

    if [[ -f ".env" ]]; then
        print_info "Existing .env file found"

        # Check if certificates exist
        if [[ ! -d "certs" ]] || [[ ! -f "certs/server.crt" ]]; then
            print_warning "Certificates missing - will regenerate"
            need_setup=true
        elif ./validate-config.sh --env-file .env &>/dev/null; then
            print_success "Existing configuration is valid"
            print_success "Certificates exist"
            return 0
        else
            print_warning "Existing configuration has issues"
            if [[ "$QUICK_MODE" == "true" ]]; then
                print_warning "Continuing anyway (--quick mode)"
                return 0
            fi
            echo ""
            read -p "Re-run setup to fix? (Y/n): " rerun
            if [[ "$rerun" =~ ^[Nn]$ ]]; then
                print_info "Using existing configuration"
                return 0
            fi
            need_setup=true
        fi
    else
        need_setup=true
    fi

    if [[ "$need_setup" == "true" ]]; then
        if [[ "$QUICK_MODE" == "true" ]]; then
            print_info "Running quick setup (non-interactive)..."
            QUICK_SETUP=true ./setup.sh
        else
            print_info "Running interactive setup..."
            echo ""
            ./setup.sh
        fi

        if [[ $? -ne 0 ]]; then
            print_error "Setup failed"
            exit 1
        fi
    fi

    print_success "Environment configured successfully"
}

# -----------------------------------------------------------------------------
# Start Services
# -----------------------------------------------------------------------------
start_services() {
    print_step "Starting Services"

    cd "$SCRIPT_DIR"

    print_info "Starting all services..."
    echo ""

    if ! docker compose up -d; then
        print_error "Failed to start services"
        echo ""
        echo "Troubleshooting:"
        echo "  1. View logs: docker compose logs"
        echo "  2. Check status: docker compose ps"
        echo "  3. Try restart: docker compose down && docker compose up -d"
        exit 1
    fi

    print_success "Services started"
}

# -----------------------------------------------------------------------------
# Wait for Health
# -----------------------------------------------------------------------------
wait_for_health() {
    print_step "Waiting for UDPS to Become Healthy"

    print_info "Health endpoint: ${HEALTH_URL}"
    print_info "Max attempts: ${HEALTH_MAX_ATTEMPTS}, interval: ${HEALTH_INTERVAL}s"
    echo ""

    local attempt=1
    while [[ $attempt -le $HEALTH_MAX_ATTEMPTS ]]; do
        local http_code
        http_code=$(curl -sf -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")

        if [[ "$http_code" == "200" ]]; then
            echo ""
            print_success "UDPS is healthy (attempt $attempt/${HEALTH_MAX_ATTEMPTS})"
            return 0
        fi

        printf "\r  Waiting for health... attempt %d/%d (HTTP %s)  " "$attempt" "$HEALTH_MAX_ATTEMPTS" "$http_code"
        sleep "$HEALTH_INTERVAL"
        attempt=$((attempt + 1))
    done

    echo ""
    echo ""
    print_warning "UDPS may not be fully healthy after ${HEALTH_MAX_ATTEMPTS} attempts"
    print_info "Check status with: ./deploy.sh --status"
    print_info "Check logs with:   ./deploy.sh --logs"
}

# -----------------------------------------------------------------------------
# Run Validation Tests
# -----------------------------------------------------------------------------
run_tests() {
    print_step "Running Deployment Validation Tests"

    local errors=0
    local warnings=0

    # Test 1: Health endpoint (liveness)
    echo "Testing health endpoints..."
    if curl -sf "$HEALTH_URL" >/dev/null 2>&1; then
        print_success "Liveness health endpoint responding"
    else
        print_error "Liveness health endpoint not responding ($HEALTH_URL)"
        errors=$((errors + 1))
    fi

    # Test 2: gRPC port reachable
    echo "Testing gRPC port..."
    if nc -z localhost 50060 2>/dev/null || (echo | timeout 3 bash -c "cat < /dev/null > /dev/tcp/localhost/50060" 2>/dev/null); then
        print_success "gRPC port 50060 is accepting connections"
    else
        print_warning "gRPC port 50060 is not reachable"
        warnings=$((warnings + 1))
    fi

    # Test 3: gRPC reflection (if grpcurl is available)
    if command -v grpcurl &>/dev/null; then
        echo "Testing gRPC reflection..."
        if grpcurl -plaintext localhost:50060 list &>/dev/null; then
            print_success "gRPC reflection is available"
        else
            # Try with TLS
            if grpcurl -insecure localhost:50060 list &>/dev/null; then
                print_success "gRPC reflection is available (TLS)"
            else
                print_warning "gRPC reflection not responding"
                warnings=$((warnings + 1))
            fi
        fi
    else
        print_info "grpcurl not installed, skipping gRPC reflection test"
    fi

    # Test 4: HTTPS API port
    echo "Testing HTTPS API port..."
    local https_code
    https_code=$(curl -sf -k -o /dev/null -w "%{http_code}" "https://localhost:8443/health/live" 2>/dev/null || echo "000")
    if [[ "$https_code" == "200" ]]; then
        print_success "HTTPS API endpoint responding on port 8443"
    else
        print_warning "HTTPS API not responding on port 8443 (HTTP $https_code)"
        warnings=$((warnings + 1))
    fi

    # Test 5: Metrics endpoint
    echo "Testing metrics endpoint..."
    local metrics_code
    metrics_code=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9090/metrics" 2>/dev/null || echo "000")
    if [[ "$metrics_code" == "200" ]]; then
        print_success "Prometheus metrics endpoint responding on port 9090"
    else
        print_warning "Metrics endpoint not responding on port 9090 (HTTP $metrics_code)"
        warnings=$((warnings + 1))
    fi

    # Test 6: Container status
    echo ""
    echo "Checking container status..."
    cd "$SCRIPT_DIR"
    local running
    running=$(docker compose ps --format json 2>/dev/null | jq -r 'select(.State=="running") | .Service' 2>/dev/null | wc -l)
    local total
    total=$(docker compose ps --format json 2>/dev/null | jq -r '.Service' 2>/dev/null | wc -l)
    if [[ $running -ge 1 ]]; then
        print_success "$running/$total containers running"
    else
        print_warning "No containers appear to be running"
        warnings=$((warnings + 1))
    fi

    echo ""
    if [[ $errors -gt 0 ]]; then
        print_error "Validation failed with $errors error(s) and $warnings warning(s)"
        return 1
    elif [[ $warnings -gt 0 ]]; then
        print_warning "Validation passed with $warnings warning(s)"
        return 0
    else
        print_success "All validation tests passed!"
        return 0
    fi
}

# -----------------------------------------------------------------------------
# Show Status
# -----------------------------------------------------------------------------
show_status() {
    print_step "Deployment Status"

    cd "$SCRIPT_DIR"

    # Container status
    echo "Container Status:"
    echo ""
    docker compose ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || docker compose ps

    echo ""

    # Service health checks
    echo "Service Health:"
    echo ""

    # UDPS health
    local health_code
    health_code=$(curl -sf -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")
    if [[ "$health_code" == "200" ]]; then
        print_success "UDPS liveness:  OK ($HEALTH_URL)"
    else
        print_error "UDPS liveness:  FAIL (HTTP $health_code)"
    fi

    # HTTPS endpoint
    local https_code
    https_code=$(curl -sf -k -o /dev/null -w "%{http_code}" "https://localhost:8443/health/live" 2>/dev/null || echo "000")
    if [[ "$https_code" == "200" ]]; then
        print_success "UDPS HTTPS:     OK (https://localhost:8443)"
    else
        print_error "UDPS HTTPS:     FAIL (HTTP $https_code)"
    fi

    # Metrics
    local metrics_code
    metrics_code=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9090/metrics" 2>/dev/null || echo "000")
    if [[ "$metrics_code" == "200" ]]; then
        print_success "Metrics:        OK (http://localhost:9090/metrics)"
    else
        print_error "Metrics:        FAIL (HTTP $metrics_code)"
    fi

    echo ""
    echo "Access Points:"
    echo "  gRPC:        localhost:50060"
    echo "  HTTPS API:   https://localhost:8443"
    echo "  Health:      http://localhost:8081/health/live"
    echo "  Metrics:     http://localhost:9090/metrics"
}

# -----------------------------------------------------------------------------
# Show Logs
# -----------------------------------------------------------------------------
show_logs() {
    cd "$SCRIPT_DIR"
    echo "Showing live logs (Ctrl+C to exit)..."
    echo ""
    docker compose logs -f --tail=100
}

# -----------------------------------------------------------------------------
# Stop Services
# -----------------------------------------------------------------------------
stop_services() {
    print_step "Stopping Services"

    cd "$SCRIPT_DIR"
    docker compose down

    print_success "All services stopped"
    print_info "Data volumes preserved. Use --clean to remove all data."
}

# -----------------------------------------------------------------------------
# Clean Everything
# -----------------------------------------------------------------------------
clean_all() {
    print_step "Cleaning Up"

    cd "$SCRIPT_DIR"

    echo -e "${YELLOW}This will remove all containers, volumes, and data!${NC}"
    read -p "Are you sure? (y/N): " confirm

    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    docker compose down -v --rmi local

    print_success "All containers, volumes, and locally-built images removed"
}

# -----------------------------------------------------------------------------
# Print Deployment Success
# -----------------------------------------------------------------------------
print_deployment_success() {
    echo ""
    echo -e "${GREEN}+============================================================================+${NC}"
    echo -e "${GREEN}|                                                                              |${NC}"
    echo -e "${GREEN}|                      DEPLOYMENT SUCCESSFUL!                                  |${NC}"
    echo -e "${GREEN}|                                                                              |${NC}"
    echo -e "${GREEN}+============================================================================+${NC}"
    echo ""
    echo -e "${BOLD}Access your UDPS deployment:${NC}"
    echo ""
    echo -e "  ${CYAN}gRPC:${NC}          localhost:50060"
    echo -e "  ${CYAN}HTTPS API:${NC}     https://localhost:8443"
    echo -e "  ${CYAN}Health:${NC}        http://localhost:8081/health/live"
    echo -e "  ${CYAN}Metrics:${NC}       http://localhost:9090/metrics"
    echo ""
    echo -e "${BOLD}Quick Commands:${NC}"
    echo ""
    echo "  ./deploy.sh --status   Check service health"
    echo "  ./deploy.sh --logs     View live logs"
    echo "  ./deploy.sh --stop     Stop services"
    echo "  ./deploy.sh --test     Run validation tests"
    echo ""
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    QUICK_MODE=false

    case "${1:-}" in
        --help|-h)
            show_help
            exit 0
            ;;
        --status)
            show_status
            exit 0
            ;;
        --logs)
            show_logs
            exit 0
            ;;
        --stop)
            stop_services
            exit 0
            ;;
        --clean)
            clean_all
            exit 0
            ;;
        --test)
            run_tests
            exit $?
            ;;
        --quick)
            QUICK_MODE=true
            ;;
        "")
            # Full deployment
            ;;
        *)
            print_error "Unknown option: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac

    print_banner

    check_prerequisites
    build_images
    setup_environment
    start_services
    wait_for_health

    echo ""
    run_tests

    print_deployment_success
}

main "$@"
