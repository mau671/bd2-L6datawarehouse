#!/usr/bin/env bash
set -euo pipefail

HOST="${MSSQL_HOST:-mssql_dw}"
PASS="${MSSQL_SA_PASSWORD:-YourStrong@Passw0rd2}"

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"
if [ ! -x "$SQLCMD" ]; then
  SQLCMD="/opt/mssql-tools/bin/sqlcmd"
fi
# último fallback: que esté en el PATH
if [ ! -x "$SQLCMD" ]; then
  SQLCMD="sqlcmd"
fi

run_sql () {
  local f="$1"
  echo ">> Ejecutando $(basename "$f")"
  "$SQLCMD" -b -C -S "$HOST" -U sa -P "$PASS" -i "$f"
}

[ -f /scripts/00_create_database.sql ] && run_sql /scripts/00_create_database.sql
[ -f /scripts/01_schema_dw.sql ]      && run_sql /scripts/01_schema_dw.sql
[ -f /scripts/99_seed.sql ]           && run_sql /scripts/99_seed.sql

echo ">> init_dw OK"
