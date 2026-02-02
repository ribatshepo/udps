#!/bin/bash
# =============================================================================
# UDPS Teardown Script
# =============================================================================
# Safely tear down the UDPS deployment with various cleanup levels.
#
# USAGE:
#   ./teardown.sh              # Stop services, preserve data
#   ./teardown.sh --full       # Stop and remove all data volumes
#   ./teardown.sh --purge      # Complete cleanup (volumes, images, certs, env)
#   ./teardown.sh --force      # Skip confirmation prompts
#   ./teardown.sh --help       # Show this help
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="${UDPS_VERSION:-0.1.0}"
IMAGE_NAME="${UDPS_IMAGE:-seri-sa/udps}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Options
FULL_CLEANUP=false
PURGE_ALL=false
FORCE=false

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
print_banner() {
    echo ""
    echo -e "${RED}+============================================================================+${NC}"
    echo -e "${RED}|${NC}                                                                              ${RED}|${NC}"
    echo -e "${RED}|${NC}   ${BOLD}UDPS Teardown${NC}                                                            ${RED}|${NC}"
    echo -e "${RED}|${NC}   Taking down the Unified Data Platform Service                              ${RED}|${NC}"
    echo -e "${RED}|${NC}                                                                              ${RED}|${NC}"
    echo -e "${RED}+============================================================================+${NC}"
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
    echo "UDPS Teardown Script"
    echo ""
    echo "Usage: ./teardown.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  (no args)     Stop all services, preserve data volumes"
    echo "  --full        Stop services AND remove all data volumes"
    echo "  --purge       Complete cleanup: volumes, images, certs, .env"
    echo "  --force, -f   Skip all confirmation prompts"
    echo "  --help, -h    Show this help message"
    echo ""
    echo "Cleanup Levels:"
    echo ""
    echo "  DEFAULT (no flags):"
    echo "    - Stops all running containers"
    echo "    - Removes containers"
    echo "    - Preserves data volumes (postgres, redis, minio, kafka)"
    echo "    - Preserves Docker images"
    echo "    - Preserves certificates and .env"
    echo ""
    echo "  --full:"
    echo "    - Everything in DEFAULT, plus:"
    echo "    - Removes all data volumes (DATABASE WILL BE LOST)"
    echo "    - Removes Docker network"
    echo ""
    echo "  --purge:"
    echo "    - Everything in --full, plus:"
    echo "    - Removes Docker images"
    echo "    - Removes certificates directory"
    echo "    - Removes .env file"
    echo "    - Complete fresh start"
    echo ""
    echo "Examples:"
    echo "  ./teardown.sh                    # Stop services, keep data"
    echo "  ./teardown.sh --full             # Stop and remove all data"
    echo "  ./teardown.sh --purge --force    # Complete cleanup, no prompts"
    echo ""
}

confirm() {
    local message="$1"
    local default="${2:-n}"

    if [[ "$FORCE" == "true" ]]; then
        return 0
    fi

    if [[ "$default" == "y" ]]; then
        read -p "$message (Y/n): " response
        [[ ! "$response" =~ ^[Nn]$ ]]
    else
        read -p "$message (y/N): " response
        [[ "$response" =~ ^[Yy]$ ]]
    fi
}

# -----------------------------------------------------------------------------
# Check Current State
# -----------------------------------------------------------------------------
check_deployment_state() {
    print_step "Checking Deployment State"

    cd "$SCRIPT_DIR"

    local running_containers
    running_containers=$(docker compose ps -q 2>/dev/null | wc -l)

    if [[ $running_containers -gt 0 ]]; then
        print_info "Found $running_containers running container(s)"
        echo ""
        docker compose ps --format "table {{.Service}}\t{{.Status}}" 2>/dev/null || docker compose ps
    else
        print_info "No running containers found"
    fi

    echo ""
    local volumes
    volumes=$(docker volume ls --filter "name=udps" --format "{{.Name}}" 2>/dev/null | wc -l)
    if [[ $volumes -gt 0 ]]; then
        print_info "Found $volumes UDPS data volume(s)"
        docker volume ls --filter "name=udps" --format "  - {{.Name}}" 2>/dev/null
    fi

    echo ""
    local images
    images=$(docker images --filter "reference=*udps*" --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | wc -l)
    if [[ $images -gt 0 ]]; then
        print_info "Found $images UDPS Docker image(s)"
        docker images --filter "reference=*udps*" --format "  - {{.Repository}}:{{.Tag}}" 2>/dev/null
    fi

    echo ""
}

# -----------------------------------------------------------------------------
# Stop Services
# -----------------------------------------------------------------------------
stop_services() {
    print_step "Stopping Services"

    cd "$SCRIPT_DIR"

    if [[ ! -f "docker-compose.yml" ]]; then
        print_warning "docker-compose.yml not found"
        return 0
    fi

    if ! docker compose ps -q 2>/dev/null | grep -q .; then
        print_info "No services currently running"
        return 0
    fi

    print_info "Stopping all services..."

    if docker compose stop; then
        print_success "Services stopped gracefully"
    else
        print_warning "Graceful stop had issues, forcing..."
        docker compose kill 2>/dev/null || true
    fi

    print_info "Removing containers..."
    docker compose down 2>/dev/null || true

    print_success "All containers removed"
}

# -----------------------------------------------------------------------------
# Remove Volumes
# -----------------------------------------------------------------------------
remove_volumes() {
    print_step "Removing Data Volumes"

    echo -e "${YELLOW}WARNING: This will permanently delete all data including:${NC}"
    echo "  - PostgreSQL database"
    echo "  - Redis cache"
    echo "  - MinIO object storage"
    echo "  - Kafka topics and data"
    echo ""

    if ! confirm "Delete all data volumes?"; then
        print_info "Skipping volume removal"
        return 0
    fi

    cd "$SCRIPT_DIR"

    docker compose down -v 2>/dev/null || true

    local volumes=(
        "udps-postgres-data"
        "udps-redis-data"
        "udps-minio-data"
        "udps-kafka-data"
        "udps-zookeeper-data"
        "udps-jaeger-data"
    )

    for vol in "${volumes[@]}"; do
        if docker volume inspect "$vol" &>/dev/null; then
            docker volume rm "$vol" 2>/dev/null && print_success "Removed volume: $vol"
        fi
    done

    if docker network inspect udps-network &>/dev/null; then
        docker network rm udps-network 2>/dev/null && print_success "Removed network: udps-network"
    fi

    print_success "All data volumes removed"
}

# -----------------------------------------------------------------------------
# Remove Images
# -----------------------------------------------------------------------------
remove_images() {
    print_step "Removing Docker Images"

    local images=(
        "${IMAGE_NAME}:${VERSION}"
        "${IMAGE_NAME}:latest"
    )

    echo "The following images will be removed:"
    for img in "${images[@]}"; do
        if docker image inspect "$img" &>/dev/null; then
            echo "  - $img"
        fi
    done
    echo ""

    if ! confirm "Remove these Docker images?"; then
        print_info "Skipping image removal"
        return 0
    fi

    for img in "${images[@]}"; do
        if docker image inspect "$img" &>/dev/null; then
            docker rmi "$img" 2>/dev/null && print_success "Removed image: $img"
        fi
    done

    print_info "Cleaning up dangling images..."
    docker image prune -f --filter "label=org.opencontainers.image.title=UDPS*" 2>/dev/null || true

    print_success "Docker images removed"
}

# -----------------------------------------------------------------------------
# Remove Configuration
# -----------------------------------------------------------------------------
remove_configuration() {
    print_step "Removing Configuration"

    echo -e "${YELLOW}WARNING: This will remove:${NC}"
    echo "  - TLS certificates (certs/)"
    echo "  - Environment file (.env)"
    echo ""
    echo "You will need to run setup.sh again before redeploying."
    echo ""

    if ! confirm "Remove certificates and .env?"; then
        print_info "Skipping configuration removal"
        return 0
    fi

    cd "$SCRIPT_DIR"

    if [[ -d "certs" ]]; then
        rm -rf certs
        print_success "Removed certificates directory"
    fi

    if [[ -f ".env" ]]; then
        rm -f .env
        print_success "Removed .env file"
    fi

    print_success "Configuration files removed"
}

# -----------------------------------------------------------------------------
# Print Summary
# -----------------------------------------------------------------------------
print_summary() {
    echo ""
    echo -e "${GREEN}+============================================================================+${NC}"
    echo -e "${GREEN}|                                                                              |${NC}"
    echo -e "${GREEN}|                         TEARDOWN COMPLETE                                    |${NC}"
    echo -e "${GREEN}|                                                                              |${NC}"
    echo -e "${GREEN}+============================================================================+${NC}"
    echo ""

    if [[ "$PURGE_ALL" == "true" ]]; then
        echo "Complete cleanup performed:"
        echo "  - All containers stopped and removed"
        echo "  - All data volumes deleted"
        echo "  - Docker images removed"
        echo "  - Certificates and .env removed"
        echo ""
        echo "To redeploy, run:"
        echo "  ./deploy.sh"
        echo ""
    elif [[ "$FULL_CLEANUP" == "true" ]]; then
        echo "Full cleanup performed:"
        echo "  - All containers stopped and removed"
        echo "  - All data volumes deleted"
        echo ""
        echo "To redeploy (will use existing images):"
        echo "  ./deploy.sh --quick"
        echo ""
    else
        echo "Services stopped (data preserved):"
        echo "  - All containers stopped and removed"
        echo "  - Data volumes preserved"
        echo ""
        echo "To restart services:"
        echo "  docker compose up -d"
        echo ""
        echo "To remove data as well:"
        echo "  ./teardown.sh --full"
        echo ""
    fi
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --help|-h)
                show_help
                exit 0
                ;;
            --full)
                FULL_CLEANUP=true
                shift
                ;;
            --purge)
                PURGE_ALL=true
                FULL_CLEANUP=true
                shift
                ;;
            --force|-f)
                FORCE=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                echo ""
                show_help
                exit 1
                ;;
        esac
    done

    print_banner

    echo -e "${BOLD}Teardown Level:${NC}"
    if [[ "$PURGE_ALL" == "true" ]]; then
        echo -e "  ${RED}PURGE${NC} - Complete cleanup (containers, volumes, images, config)"
    elif [[ "$FULL_CLEANUP" == "true" ]]; then
        echo -e "  ${YELLOW}FULL${NC} - Remove containers and all data volumes"
    else
        echo -e "  ${GREEN}DEFAULT${NC} - Stop services, preserve data"
    fi
    echo ""

    if [[ "$FORCE" != "true" ]]; then
        if [[ "$PURGE_ALL" == "true" ]]; then
            echo -e "${RED}This will completely remove UDPS and all its data!${NC}"
            if ! confirm "Are you sure you want to continue?"; then
                echo "Cancelled."
                exit 0
            fi
        elif [[ "$FULL_CLEANUP" == "true" ]]; then
            echo -e "${YELLOW}This will remove all data volumes (database, cache, storage)!${NC}"
            if ! confirm "Continue with full cleanup?"; then
                echo "Cancelled."
                exit 0
            fi
        fi
    fi

    check_deployment_state
    stop_services

    if [[ "$FULL_CLEANUP" == "true" ]]; then
        remove_volumes
    fi

    if [[ "$PURGE_ALL" == "true" ]]; then
        remove_images
        remove_configuration
    fi

    print_summary
}

main "$@"
