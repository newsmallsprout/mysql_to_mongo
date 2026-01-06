#!/bin/sh
set -e
echo "[init] Checking MySQL SSL setup..."
# Only runs on first container initialization when datadir is empty.
# Generate RSA keys and SSL certs for MySQL server to enable TLS.
if [ -d "/var/lib/mysql" ]; then
  if ls /var/lib/mysql/*.pem >/dev/null 2>&1; then
    echo "[init] SSL certs already present in /var/lib/mysql (*.pem)."
  else
    echo "[init] Generating SSL/RSA for MySQL..."
    if command -v mysql_ssl_rsa_setup >/dev/null 2>&1; then
      mysql_ssl_rsa_setup --datadir=/var/lib/mysql
      echo "[init] mysql_ssl_rsa_setup done."
    else
      echo "[init] mysql_ssl_rsa_setup not found, fallback to openssl self-signed."
      openssl genrsa -out /var/lib/mysql/server-key.pem 2048
      openssl req -new -x509 -key /var/lib/mysql/server-key.pem -days 3650 -subj "/CN=mysql_source" -out /var/lib/mysql/server-cert.pem
      cp /var/lib/mysql/server-cert.pem /var/lib/mysql/ca.pem
      chmod 400 /var/lib/mysql/server-key.pem || true
      chmod 444 /var/lib/mysql/server-cert.pem /var/lib/mysql/ca.pem || true
      echo "[init] openssl self-signed done."
    fi
  fi
fi
