#!/bin/bash
# =============================================================================
# UDPS Docker Environment Setup Script
# =============================================================================
# Interactive script for setting up UDPS Docker deployments.
#
# USAGE:
#   cd deploy/docker
#   ./setup.sh
#
# This script will:
#   1. Generate secure random secrets (PostgreSQL, Redis, MinIO, Kafka SASL)
#   2. Generate TLS certificates
#   3. Create .env file from .env.template
#   4. Validate the configuration
#
# Non-interactive mode:
#   QUICK_SETUP=true ./setup.sh
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
CERTS_DIR="$SCRIPT_DIR/certs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
print_header() {
    echo ""
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================${NC}"
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
    echo -e "${BLUE}[INFO] $1${NC}"
}

generate_secret() {
    openssl rand -base64 24 | tr -d "=+/" | cut -c1-32
}

generate_long_secret() {
    openssl rand -base64 48
}

# -----------------------------------------------------------------------------
# Certificate Generation
# -----------------------------------------------------------------------------
generate_certificates() {
    print_header "Generating TLS Certificates"

    echo "Creating certificates directory..."
    mkdir -p "$CERTS_DIR"

    local san_config="$CERTS_DIR/openssl-san.cnf"

    cat > "$san_config" << 'SANCFG'
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = v3_req

[dn]
CN = localhost

[v3_req]
subjectAltName = @alt_names

[alt_names]
SANCFG

    local san_index=1
    local default_dns=(
        "localhost"
        "udps"
        "udps-api"
        "postgres"
        "redis"
        "minio"
        "kafka"
        "jaeger"
    )

    for dns in "${default_dns[@]}"; do
        echo "DNS.${san_index} = ${dns}" >> "$san_config"
        ((san_index++))
    done

    echo "IP.1 = 127.0.0.1" >> "$san_config"
    echo "IP.2 = ::1" >> "$san_config"

    echo ""
    echo "Certificate will include these SANs:"
    grep -E "^DNS\.|^IP\." "$san_config" | sed 's/^/  /'
    echo ""

    # Generate CA
    echo "Generating CA key..."
    openssl genrsa -out "$CERTS_DIR/ca.key" 4096 2>/dev/null

    echo "Generating CA certificate..."
    openssl req -new -x509 -days 3650 -key "$CERTS_DIR/ca.key" \
        -out "$CERTS_DIR/ca.crt" \
        -subj "/CN=UDPS Development CA/O=UDPS/OU=Development" 2>/dev/null

    # Generate server certificate
    echo "Generating server key..."
    openssl genrsa -out "$CERTS_DIR/server.key" 2048 2>/dev/null

    echo "Generating server certificate..."
    openssl req -new -key "$CERTS_DIR/server.key" \
        -out "$CERTS_DIR/server.csr" \
        -config "$san_config" 2>/dev/null

    openssl x509 -req -days 365 \
        -in "$CERTS_DIR/server.csr" \
        -CA "$CERTS_DIR/ca.crt" \
        -CAkey "$CERTS_DIR/ca.key" \
        -set_serial 01 \
        -out "$CERTS_DIR/server.crt" \
        -extensions v3_req \
        -extfile "$san_config" 2>/dev/null

    # Generate client certificate
    echo "Generating client certificate..."
    openssl genrsa -out "$CERTS_DIR/client.key" 2048 2>/dev/null

    openssl req -new -key "$CERTS_DIR/client.key" \
        -out "$CERTS_DIR/client.csr" \
        -subj "/CN=udps-client/O=UDPS/OU=Client" 2>/dev/null

    openssl x509 -req -days 365 \
        -in "$CERTS_DIR/client.csr" \
        -CA "$CERTS_DIR/ca.crt" \
        -CAkey "$CERTS_DIR/ca.key" \
        -set_serial 02 \
        -out "$CERTS_DIR/client.crt" 2>/dev/null

    # Cleanup temporary files
    rm -f "$CERTS_DIR/server.csr" "$CERTS_DIR/client.csr" "$san_config"

    # Set permissions
    chmod 600 "$CERTS_DIR"/*.key
    chmod 644 "$CERTS_DIR"/*.crt

    print_success "TLS certificates generated in $CERTS_DIR"

    # Create service-specific certificate directories
    echo ""
    echo "Creating service-specific certificate directories..."

    # Service UIDs:
    # - PostgreSQL: 70 (postgres user)
    # - Redis: 999 (redis user)
    # - MinIO: 1000 (minio user)
    # - API: 1000 (app user in Dockerfile)
    local services=("postgres:70:70" "redis:999:1000" "minio:1000:1000" "api:1000:1000")

    for service_spec in "${services[@]}"; do
        IFS=':' read -r service uid gid <<< "$service_spec"
        local service_dir="$CERTS_DIR/$service"

        mkdir -p "$service_dir"
        cp "$CERTS_DIR/server.crt" "$service_dir/"
        cp "$CERTS_DIR/server.key" "$service_dir/"
        cp "$CERTS_DIR/ca.crt" "$service_dir/"

        chmod 600 "$service_dir/server.key"
        chmod 644 "$service_dir/server.crt" "$service_dir/ca.crt"

        if [[ $(id -u) -eq 0 ]] || command -v sudo &> /dev/null; then
            sudo chown -R "$uid:$gid" "$service_dir" 2>/dev/null || \
                print_warning "Could not set ownership for $service_dir (run with sudo for production)"
        fi

        print_success "Created $service_dir (UID:$uid, GID:$gid)"
    done

    echo ""
    print_success "Service-specific certificates configured"
}

# -----------------------------------------------------------------------------
# Main Script
# -----------------------------------------------------------------------------
print_header "UDPS Docker Environment Setup"

QUICK_SETUP="${QUICK_SETUP:-false}"

# Check for existing .env file
if [[ -f "$ENV_FILE" ]]; then
    if [[ "$QUICK_SETUP" == "true" ]]; then
        print_warning "Existing .env found - keeping it (QUICK_SETUP mode)"
        if [[ -d "$CERTS_DIR" ]] && [[ -f "$CERTS_DIR/server.crt" ]]; then
            print_success "Certificates exist"
            exit 0
        fi
        print_warning "Certificates missing - regenerating"
        generate_certificates
        exit 0
    fi

    echo -e "${YELLOW}An existing .env file was found at:${NC}"
    echo "  $ENV_FILE"
    echo ""
    read -p "Do you want to overwrite it? (y/N): " overwrite
    if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
        echo "Setup cancelled. Existing .env preserved."
        exit 0
    fi
    echo ""
fi

# Generate secrets
print_header "Generating Secrets"

POSTGRES_PASSWORD=$(generate_secret)
REDIS_PASSWORD=$(generate_secret)
MINIO_ROOT_PASSWORD=$(generate_secret)
KAFKA_SASL_PASSWORD=$(generate_secret)

print_success "PostgreSQL password generated"
print_success "Redis password generated"
print_success "MinIO root password generated"
print_success "Kafka SASL password generated"

# Create .env file
print_header "Creating .env File"

cat > "$ENV_FILE" << EOF
# =============================================================================
# UDPS Docker Environment Configuration
# =============================================================================
# Generated by setup.sh on $(date)
# =============================================================================

# =============================================================================
# UDPS APPLICATION
# =============================================================================
UDPS_VERSION=${UDPS_VERSION:-0.1.0}
UDPS_IMAGE=${UDPS_IMAGE:-seri-sa/udps}
UDPS_LOG_LEVEL=INFO

# =============================================================================
# DATABASE (PostgreSQL)
# =============================================================================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=udps
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=udps_db

# =============================================================================
# REDIS CACHE
# =============================================================================
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=${REDIS_PASSWORD}

# =============================================================================
# MINIO OBJECT STORAGE
# =============================================================================
MINIO_HOST=minio
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001
MINIO_ROOT_USER=udps-admin
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}
MINIO_BUCKET=udps-data

# =============================================================================
# KAFKA
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SASL_USERNAME=udps
KAFKA_SASL_PASSWORD=${KAFKA_SASL_PASSWORD}

# =============================================================================
# JAEGER TRACING
# =============================================================================
JAEGER_HOST=jaeger
JAEGER_AGENT_PORT=6831
JAEGER_COLLECTOR_PORT=14268
JAEGER_UI_PORT=16686

# =============================================================================
# PORTS (host-side)
# =============================================================================
UDPS_GRPC_PORT=50060
UDPS_HTTPS_PORT=8443
UDPS_METRICS_PORT=9090
UDPS_HEALTH_PORT=8081

# =============================================================================
# TLS
# =============================================================================
TLS_CERT_PATH=/app/certs/server.crt
TLS_KEY_PATH=/app/certs/server.key
TLS_CA_PATH=/app/certs/ca.crt
EOF

print_success "Created .env file at $ENV_FILE"

# Generate certificates
generate_certificates

# Validate
print_header "Validating Configuration"

if [[ -x "$SCRIPT_DIR/validate-config.sh" ]]; then
    if "$SCRIPT_DIR/validate-config.sh" --env-file "$ENV_FILE"; then
        print_success "Configuration validation passed"
    else
        print_warning "Configuration validation had warnings (see above)"
    fi
else
    print_info "validate-config.sh not executable, skipping validation"
fi

print_header "Setup Complete!"

echo "Configuration Summary:"
echo ""
echo "  PostgreSQL:  udps@postgres:5432/udps_db"
echo "  Redis:       redis:6379"
echo "  MinIO:       minio:9000"
echo "  Kafka:       kafka:9092"
echo "  Jaeger UI:   http://localhost:16686"
echo ""
echo "Files created:"
echo "  - .env"
echo "  - certs/ (TLS certificates)"
echo "    - certs/postgres/  (UID 70 - PostgreSQL)"
echo "    - certs/redis/     (UID 999 - Redis)"
echo "    - certs/minio/     (UID 1000 - MinIO)"
echo "    - certs/api/       (UID 1000 - API)"
echo ""
echo "Next steps:"
echo ""
echo "  1. Review and customize .env if needed"
echo ""
echo "  2. Start UDPS:"
echo "     ./deploy.sh"
echo ""
echo "  3. Or start manually:"
echo "     docker compose up -d"
echo ""
echo "  4. Access UDPS:"
echo "     gRPC:      localhost:50060"
echo "     HTTPS API: https://localhost:8443"
echo "     Health:    http://localhost:8081/health/live"
echo "     Metrics:   http://localhost:9090/metrics"
echo ""
