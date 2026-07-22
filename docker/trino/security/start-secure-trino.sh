#!/bin/sh
set -eu

require_secret() {
    variable=$1
    value=$2
    if [ -z "$value" ] || [ "$value" = "CHANGE_ME" ]; then
        echo "ERROR: $variable must be configured with a non-placeholder value." >&2
        exit 1
    fi
}

require_secret TRINO_KEYSTORE_PASSWORD "${TRINO_KEYSTORE_PASSWORD:-}"
require_secret TRINO_SHARED_SECRET "${TRINO_SHARED_SECRET:-}"
require_secret TRINO_MARKETING_PASSWORD "${TRINO_MARKETING_PASSWORD:-}"
require_secret TRINO_ENGINEERING_PASSWORD "${TRINO_ENGINEERING_PASSWORD:-}"

runtime_dir=/var/trino/security
keystore="$runtime_dir/trino-keystore.p12"
password_file="$runtime_dir/password.db"
mkdir -p "$runtime_dir"

java_bin=$(command -v java)
keytool_bin="$(dirname "$(readlink -f "$java_bin")")/keytool"

if [ -f "$keystore" ] && ! "$keytool_bin" -list -keystore "$keystore" \
        -storepass "$TRINO_KEYSTORE_PASSWORD" >/dev/null 2>&1; then
    rm -f "$keystore"
fi

if [ ! -f "$keystore" ]; then
    "$keytool_bin" -genkeypair \
        -alias trino \
        -keyalg RSA \
        -keysize 3072 \
        -validity 825 \
        -storetype PKCS12 \
        -keystore "$keystore" \
        -storepass "$TRINO_KEYSTORE_PASSWORD" \
        -keypass "$TRINO_KEYSTORE_PASSWORD" \
        -dname "CN=trino, OU=Lakehouse, O=Retail Banking, L=HCM, C=VN" \
        -ext "SAN=dns:trino,dns:localhost,ip:127.0.0.1" \
        -noprompt
fi

"$java_bin" /opt/trino-security/GeneratePasswordFile.java "$password_file"
chmod 600 "$keystore" "$password_file"

exec /usr/lib/trino/bin/run-trino
