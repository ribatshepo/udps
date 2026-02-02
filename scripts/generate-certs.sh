#!/bin/bash
# =============================================================================
# Generate SSL certificates for UDPS development and deployment
# =============================================================================
#
# This script generates a CA and server/client certificates for TLS.
# Supports custom domains for cloud deployments.
#
# USAGE:
#   # Local development (default - localhost only)
#   ./scripts/generate-certs.sh
#
#   # Cloud deployment with custom domain(s)
#   ./scripts/generate-certs.sh --domain udps.company.com
#   ./scripts/generate-certs.sh --domain udps.company.com --domain grpc.udps.company.com
#
#   # Specify output directory
#   ./scripts/generate-certs.sh --output /path/to/certs
#
# OPTIONS:
#   --domain FQDN   Add a custom domain to the certificate SANs (repeatable)
#   --output DIR    Output directory for certificates (default: ../deploy/docker/certs)
#   --help          Show this help message
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="$SCRIPT_DIR/../deploy/docker/certs"
CUSTOM_DOMAINS=()

# Parse command line arguments
show_help() {
    head -n 28 "$0" | tail -n 25 | sed 's/^# //' | sed 's/^#//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --domain)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --domain requires a value"
                exit 1
            fi
            CUSTOM_DOMAINS+=("$2")
            shift 2
            ;;
        --output)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --output requires a value"
                exit 1
            fi
            CERTS_DIR="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo "Error: Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

build_san_config() {
    local san_index=1
    local ip_index=1

    cat << 'EOF'
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
EOF

    local default_dns=(
        "localhost"
        "udps-app"
        "udps-api"
        "postgres"
        "redis"
        "kafka"
        "minio"
    )

    for dns in "${default_dns[@]}"; do
        echo "DNS.${san_index} = ${dns}"
        ((san_index++))
    done

    for domain in "${CUSTOM_DOMAINS[@]}"; do
        echo "DNS.${san_index} = ${domain}"
        ((san_index++))
        echo "DNS.${san_index} = *.${domain}"
        ((san_index++))
    done

    echo "IP.${ip_index} = 127.0.0.1"
    ((ip_index++))
    echo "IP.${ip_index} = ::1"
}

echo "=============================================="
echo "UDPS Certificate Generation"
echo "=============================================="
echo ""

echo "Output directory: $CERTS_DIR"
if [[ ${#CUSTOM_DOMAINS[@]} -gt 0 ]]; then
    echo "Custom domains:"
    for domain in "${CUSTOM_DOMAINS[@]}"; do
        echo "  - $domain"
        echo "  - *.$domain (wildcard)"
    done
else
    echo "Custom domains: none (localhost only)"
fi
echo ""

echo "Creating certificates directory..."
mkdir -p "$CERTS_DIR"

echo "Generating CA key..."
openssl genrsa -out "$CERTS_DIR/ca.key" 4096

echo "Generating CA certificate..."
openssl req -new -x509 -days 3650 -key "$CERTS_DIR/ca.key" \
  -out "$CERTS_DIR/ca.crt" \
  -subj "/CN=UDPS Development CA/O=UDPS/OU=Development"

echo "Generating server key..."
openssl genrsa -out "$CERTS_DIR/server.key" 2048

echo "Creating OpenSSL configuration for SAN..."
build_san_config > "$CERTS_DIR/openssl-san.cnf"

echo ""
echo "Certificate will include these SANs:"
grep -E "^DNS\.|^IP\." "$CERTS_DIR/openssl-san.cnf" | sed 's/^/  /'
echo ""

echo "Generating server certificate signing request with SANs..."
openssl req -new -key "$CERTS_DIR/server.key" \
  -out "$CERTS_DIR/server.csr" \
  -config "$CERTS_DIR/openssl-san.cnf"

echo "Signing server certificate with CA and SANs..."
openssl x509 -req -days 365 \
  -in "$CERTS_DIR/server.csr" \
  -CA "$CERTS_DIR/ca.crt" \
  -CAkey "$CERTS_DIR/ca.key" \
  -set_serial 01 \
  -out "$CERTS_DIR/server.crt" \
  -extensions v3_req \
  -extfile "$CERTS_DIR/openssl-san.cnf"

echo "Cleaning up temporary files..."
rm "$CERTS_DIR/server.csr"
rm "$CERTS_DIR/openssl-san.cnf"

echo "Generating client key..."
openssl genrsa -out "$CERTS_DIR/client.key" 2048

echo "Generating client certificate signing request..."
openssl req -new -key "$CERTS_DIR/client.key" \
  -out "$CERTS_DIR/client.csr" \
  -subj "/CN=udps-client/O=UDPS/OU=Client"

echo "Signing client certificate with CA..."
openssl x509 -req -days 365 \
  -in "$CERTS_DIR/client.csr" \
  -CA "$CERTS_DIR/ca.crt" \
  -CAkey "$CERTS_DIR/ca.key" \
  -set_serial 02 \
  -out "$CERTS_DIR/client.crt"

echo "Cleaning up client CSR..."
rm "$CERTS_DIR/client.csr"

chmod 600 "$CERTS_DIR"/*.key
chmod 644 "$CERTS_DIR"/*.crt

echo ""
echo "=============================================="
echo "Certificate generation complete!"
echo "=============================================="
echo ""
echo "Generated files in $CERTS_DIR:"
echo "  - ca.crt      (CA certificate - distribute to clients)"
echo "  - ca.key      (CA private key - keep secure)"
echo "  - server.crt  (Server certificate)"
echo "  - server.key  (Server private key)"
echo "  - client.crt  (Client certificate)"
echo "  - client.key  (Client private key)"
echo ""

if [[ ${#CUSTOM_DOMAINS[@]} -gt 0 ]]; then
    echo "IMPORTANT: For cloud deployments:"
    echo "  1. Import ca.crt into your browser/system trust store"
    echo "  2. Or use a proper certificate from Let's Encrypt / your CA"
    echo ""
fi

echo "These certificates will be mounted into Docker containers via docker-compose.yml"
