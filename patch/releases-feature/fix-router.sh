#!/bin/bash
# Fix router/index.ts — no python required
set -e
ROOT="${1:-.}"
FILE="$ROOT/frontend/src/router/index.ts"

[ -f "$FILE" ] || { echo "not found: $FILE"; exit 1; }

# Remove consecutive duplicate "      {" lines
awk '!(NR>1 && $0=="      {" && prev=="      {") { print; prev=$0 }' "$FILE" > "${FILE}.tmp"
mv "${FILE}.tmp" "$FILE"

# Insert "      {" before permissions if missing
if grep -B1 "path: 'permissions'" "$FILE" | head -1 | grep -qv '^      {$'; then
  sed -i "/path: 'permissions'/i\\      {" "$FILE"
fi

echo "OK: $FILE fixed"
sed -n '/path: .deploy./,/path: .permissions./p' "$FILE" | head -20
