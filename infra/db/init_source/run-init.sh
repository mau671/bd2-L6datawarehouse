#!/usr/bin/env bash
set -euo pipefail

HOST="${MSSQL_HOST:-mssql_source}"
PASS="${MSSQL_SA_PASSWORD:-YourStrong@Passw0rd1}"

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"
[ -x "$SQLCMD" ] || SQLCMD="/opt/mssql-tools/bin/sqlcmd"

run_sql () {
  local f="$1"
  echo ">> Ejecutando $(basename "$f")"
  # -b hace que sqlcmd devuelva exit code != 0 ante errores
  "$SQLCMD" -b -C -S "$HOST" -U sa -P "$PASS" -i "$f"
}

[ -f /scripts/00_restore_db_sales.sql ] && run_sql /scripts/00_restore_db_sales.sql
[ -f /scripts/01_shift_dates.sql ]      && run_sql /scripts/01_shift_dates.sql

echo ">> init_source OK"
