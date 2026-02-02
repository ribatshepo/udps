#!/bin/bash
# =============================================================================
# UDPS Configuration Validation Script
# =============================================================================
# Validates UDPS configuration before deployment. Checks:
# - Required environment variables are set
# - Minimum lengths for security keys/passwords
# - Certificate files exist and are not expired
# - Port availability
#
# Usage:
#   ./validate-config.sh                    # Validate from environment
#   ./validate-config.sh --env-file .env    # Validate specific .env file
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse arguments
ENV_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env-file)
            shift
            ENV_FILE="$1"
            shift
            ;;
        --env-file=*)
            ENV_FILE="${1#*=}"
            shift
            ;;
        --help|-h)
            echo "UDPS Configuration Validator"
            echo ""
            echo "Usage: ./validate-config.sh [--env-file FILE]"
            echo ""
            echo "Options:"
            echo "  --env-file FILE   Path to .env file to validate"
            echo "  --help            Show this help"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

# Load .env file if specified
if [[ -n "$ENV_FILE" ]]; then
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: .env file not found at $ENV_FILE${NC}"
        exit 1
    fi
    echo -e "${BLUE}Loading configuration from: $ENV_FILE${NC}"
    set -a
    source "$ENV_FILE"
    set +a
    echo
fi

# Counters
ERRORS=0
WARNINGS=0

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
check_required() {
    local var_name=$1
    local var_value="${!var_name}"
    local description=$2

    if [[ -z "$var_value" ]]; then
        echo -e "${RED}ERROR: $var_name is not set${NC}"
        echo -e "       $description"
        echo
        ((ERRORS++))
        return 1
    fi
    return 0
}

check_min_length() {
    local var_name=$1
    local var_value="${!var_name}"
    local min_length=$2
    local description=$3

    if [[ -z "$var_value" ]]; then
        return 1
    fi

    local actual_length=${#var_value}
    if [[ $actual_length -lt $min_length ]]; then
        echo -e "${RED}ERROR: $var_name is too short (${actual_length} chars, minimum ${min_length})${NC}"
        echo -e "       $description"
        echo -e "       Fix: Generate a longer secret: openssl rand -base64 $((min_length * 2))"
        echo
        ((ERRORS++))
        return 1
    fi
    return 0
}

check_port_available() {
    local port=$1
    local service=$2

    if ss -tuln 2>/dev/null | grep -q ":$port " || netstat -tuln 2>/dev/null | grep -q ":$port "; then
        echo -e "${YELLOW}WARNING: Port $port ($service) is already in use${NC}"
        echo -e "         Find process: sudo lsof -i :$port"
        echo
        ((WARNINGS++))
        return 1
    fi
    return 0
}

# =============================================================================
echo -e "${BLUE}"
echo "============================================================================="
echo "  UDPS Configuration Validation"
echo "============================================================================="
echo -e "${NC}"

# =============================================================================
# REQUIRED ENVIRONMENT VARIABLES
# =============================================================================
echo -e "${BLUE}Checking Required Configuration...${NC}"
echo "-----------------------------------------------------------------------------"

# PostgreSQL
check_required "POSTGRES_USER" "PostgreSQL username for UDPS database"
check_required "POSTGRES_PASSWORD" "PostgreSQL password (generate with: openssl rand -base64 24)"
check_required "POSTGRES_DB" "PostgreSQL database name"
check_min_length "POSTGRES_PASSWORD" 16 "Minimum 16 characters recommended for database password"

# Redis
check_required "REDIS_PASSWORD" "Redis password for cache"
check_min_length "REDIS_PASSWORD" 16 "Minimum 16 characters recommended for Redis password"

# MinIO
check_required "MINIO_ROOT_USER" "MinIO root username"
check_required "MINIO_ROOT_PASSWORD" "MinIO root password"
check_min_length "MINIO_ROOT_PASSWORD" 16 "Minimum 16 characters recommended for MinIO password"

# Kafka
check_required "KAFKA_BOOTSTRAP_SERVERS" "Kafka bootstrap servers address"

echo

# =============================================================================
# OPTIONAL WITH WARNINGS
# =============================================================================
echo -e "${BLUE}Checking Optional Configuration...${NC}"
echo "-----------------------------------------------------------------------------"

# Kafka SASL (optional but recommended)
if [[ -n "$KAFKA_SASL_USERNAME" ]]; then
    check_required "KAFKA_SASL_PASSWORD" "Kafka SASL password (required when SASL username is set)"
    check_min_length "KAFKA_SASL_PASSWORD" 16 "Minimum 16 characters recommended for Kafka SASL password"
else
    echo -e "${YELLOW}WARNING: KAFKA_SASL_USERNAME is not set${NC}"
    echo -e "         Kafka SASL authentication is recommended for production"
    echo
    ((WARNINGS++))
fi

# Jaeger (optional)
if [[ -z "$JAEGER_HOST" ]]; then
    echo -e "${YELLOW}WARNING: JAEGER_HOST is not set${NC}"
    echo -e "         Distributed tracing will not be available"
    echo
    ((WARNINGS++))
fi

echo

# =============================================================================
# CERTIFICATE VALIDATION
# =============================================================================
echo -e "${BLUE}Checking TLS Certificates...${NC}"
echo "-----------------------------------------------------------------------------"

CERTS_DIR="$SCRIPT_DIR/certs"

if [[ -d "$CERTS_DIR" ]]; then
    # Check server certificate exists
    if [[ -f "$CERTS_DIR/server.crt" ]]; then
        print_success() { echo -e "${GREEN}[OK] $1${NC}"; }
        print_success "Server certificate exists"

        # Check expiration
        local expiry_date
        expiry_date=$(openssl x509 -enddate -noout -in "$CERTS_DIR/server.crt" 2>/dev/null | cut -d= -f2)
        if [[ -n "$expiry_date" ]]; then
            local expiry_epoch
            expiry_epoch=$(date -d "$expiry_date" +%s 2>/dev/null || echo "0")
            local now_epoch
            now_epoch=$(date +%s)
            local days_left=$(( (expiry_epoch - now_epoch) / 86400 ))

            if [[ $days_left -lt 0 ]]; then
                echo -e "${RED}ERROR: Server certificate has expired ($expiry_date)${NC}"
                echo -e "       Regenerate with: ./setup.sh"
                echo
                ((ERRORS++))
            elif [[ $days_left -lt 30 ]]; then
                echo -e "${YELLOW}WARNING: Server certificate expires in $days_left days ($expiry_date)${NC}"
                echo -e "         Regenerate with: ./setup.sh"
                echo
                ((WARNINGS++))
            else
                echo -e "${GREEN}[OK] Server certificate valid for $days_left days${NC}"
            fi
        fi
    else
        echo -e "${RED}ERROR: Server certificate not found at $CERTS_DIR/server.crt${NC}"
        echo -e "       Run: ./setup.sh to generate certificates"
        echo
        ((ERRORS++))
    fi

    # Check server key exists
    if [[ -f "$CERTS_DIR/server.key" ]]; then
        echo -e "${GREEN}[OK] Server key exists${NC}"

        # Check key permissions
        local key_perms
        key_perms=$(stat -c "%a" "$CERTS_DIR/server.key" 2>/dev/null || echo "unknown")
        if [[ "$key_perms" != "600" ]]; then
            echo -e "${YELLOW}WARNING: Server key has permissions $key_perms (should be 600)${NC}"
            echo -e "         Fix: chmod 600 $CERTS_DIR/server.key"
            echo
            ((WARNINGS++))
        fi
    else
        echo -e "${RED}ERROR: Server key not found at $CERTS_DIR/server.key${NC}"
        echo
        ((ERRORS++))
    fi

    # Check CA certificate
    if [[ -f "$CERTS_DIR/ca.crt" ]]; then
        echo -e "${GREEN}[OK] CA certificate exists${NC}"
    else
        echo -e "${RED}ERROR: CA certificate not found at $CERTS_DIR/ca.crt${NC}"
        echo
        ((ERRORS++))
    fi
else
    echo -e "${RED}ERROR: Certificates directory not found at $CERTS_DIR${NC}"
    echo -e "       Run: ./setup.sh to generate certificates"
    echo
    ((ERRORS++))
fi

echo

# =============================================================================
# PORT AVAILABILITY
# =============================================================================
echo -e "${BLUE}Checking Port Availability...${NC}"
echo "-----------------------------------------------------------------------------"

check_port_available "${UDPS_GRPC_PORT:-50060}" "UDPS gRPC"
check_port_available "${UDPS_HTTPS_PORT:-8443}" "UDPS HTTPS"
check_port_available "${UDPS_METRICS_PORT:-9090}" "UDPS Metrics"
check_port_available "${UDPS_HEALTH_PORT:-8081}" "UDPS Health"

echo

# =============================================================================
# SUMMARY
# =============================================================================
echo "============================================================================="
if [[ $ERRORS -gt 0 ]]; then
    echo -e "${RED}VALIDATION FAILED: ${ERRORS} error(s), ${WARNINGS} warning(s)${NC}"
    echo "============================================================================="
    echo
    echo "Please fix the errors above before deploying."
    echo "Use the setup script to generate secure credentials:"
    echo "  ./setup.sh"
    exit 1
elif [[ $WARNINGS -gt 0 ]]; then
    echo -e "${YELLOW}VALIDATION PASSED WITH WARNINGS: ${WARNINGS} warning(s)${NC}"
    echo "============================================================================="
    echo
    echo "Configuration is valid but review the warnings above."
    exit 0
else
    echo -e "${GREEN}VALIDATION PASSED: All checks passed${NC}"
    echo "============================================================================="
    echo
    echo "Configuration is valid and ready for deployment."
    exit 0
fi
