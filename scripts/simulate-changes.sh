#!/usr/bin/env bash
# Continuous, INSERT-heavy CDC traffic generator for postgres-source.
#
# Every INTERVAL seconds, each round:
#   - INSERTs a new row into customers AND orders (row counts keep growing)
#   - runs one extra op on a random table: UPDATE of a random row touching
#     ALL non-PK columns (60%), DELETE of a random row (25%), nothing (15%)
#     -> inserts are the clear majority of the generated change events
#   - prints source vs sink row counts and warns if the previous round's
#     inserts have not reached the sink yet
# Stop with Ctrl+C (it prints a summary incl. lag warnings).
set -euo pipefail

INTERVAL="${INTERVAL:-5}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# both compose variants share the same project name ("development")
COMPOSE="docker compose -f $ROOT/development/docker-compose.yml"

src_psql() { $COMPOSE exec -T postgres-source psql -U postgres -d sourcedb -v ON_ERROR_STOP=1 "$@"; }
snk_psql() { $COMPOSE exec -T postgres-sink psql -U postgres -d sinkdb -v ON_ERROR_STOP=1 "$@"; }

firsts=(Ada Alan Grace Edsger Barbara Donald Katherine John Mary)
lasts=(Lovelace Turing Hopper Dijkstra Liskel Knuth Johnson Lovelace Hopper)
products=(book mug cable keyboard monitor notebook sticker pen plant lamp)
statuses=(new paid shipped returned)

pick() { local -n arr=$1; echo "${arr[RANDOM % ${#arr[@]}]}"; }

round=0
lagged=0
prev_c_id=''
prev_o_id=''
started=$(date +%s)

stop() {
  echo
  echo "== Stopped after $round round(s) in $(( $(date +%s) - started ))s ($lagged lag warning(s)) =="
  exit 0
}
trap stop INT TERM

counts() {
  local src snk
  src=$($COMPOSE exec -T postgres-source psql -U postgres -d sourcedb -Atc \
    "SELECT 'customers=' || (SELECT count(*) FROM customers) || ', orders=' || (SELECT count(*) FROM orders)" 2>/dev/null || echo '?')
  snk=$($COMPOSE exec -T postgres-sink psql -U postgres -d sinkdb -Atc \
    "SELECT 'customers=' || (SELECT count(*) FROM customers) || ', orders=' || (SELECT count(*) FROM orders)" 2>/dev/null || echo '?')
  echo "  rows: src [$src] | sink [$snk]"
}

echo "== Generating INSERT-heavy CDC traffic every $INTERVAL s (Ctrl+C to stop) =="
counts
while true; do
  round=$((round + 1))
  stamp="$(date +%Y%m%d-%H%M%S)-r$round"

  # warn if the previous round's inserts have not made it to the sink yet
  if [ -n "$prev_c_id" ] && [ -n "$prev_o_id" ]; then
    miss=''
    [ "$(snk_psql -Atc "SELECT count(*) FROM customers WHERE id = $prev_c_id" 2>/dev/null || true)" = "1" ] \
      || miss="customers#$prev_c_id"
    [ "$(snk_psql -Atc "SELECT count(*) FROM orders WHERE id = $prev_o_id" 2>/dev/null || true)" = "1" ] \
      || miss="${miss:+$miss, }orders#$prev_o_id"
    if [ -n "$miss" ]; then
      lagged=$((lagged + 1))
      echo "round $round: WARN - previous round's insert(s) not in the sink yet: $miss" >&2
    fi
  fi

  fi_=$(pick firsts)
  la_=$(pick lasts)
  pr_=$(pick products)
  st_=$(pick statuses)
  qty=$((RANDOM % 5 + 1))
  amount="$(printf '%d.%02d' $((RANDOM % 900 + 10)) $((RANDOM % 100)))"

  # 1) always grow both tables (sed keeps only the RETURNING id line)
  if ! new_c_id=$(src_psql -Atc "INSERT INTO customers (first_name, last_name, email)
        VALUES ('$fi_', '$la_-$stamp', '$fi_.$la_.$stamp@example.com') RETURNING id" 2>/dev/null | sed -n 1p); then
    echo "round $round: WARN - source write failed (is the stack up?)" >&2
    prev_c_id=''; prev_o_id=''
    sleep "$INTERVAL"; continue
  fi
  if ! new_o_id=$(src_psql -Atc "INSERT INTO orders (product, quantity, amount, status)
        VALUES ('$pr_', $qty, $amount, '$st_') RETURNING id" 2>/dev/null | sed -n 1p); then
    echo "round $round: WARN - orders insert failed" >&2
    new_o_id=''
  fi

  # 2) one maintenance op on a random table: UPDATE all fields (60%), DELETE (25%), nothing (15%)
  table=customers; [ $((RANDOM % 2)) -eq 0 ] || table=orders
  roll=$((RANDOM % 100))
  extra=''
  if [ $roll -lt 60 ]; then
    extra="UPDATE random $table row (all fields)"
    if [ "$table" = customers ]; then
      src_psql -c "UPDATE customers SET
            first_name = '$(pick firsts)',
            last_name  = '$(pick lasts)-$stamp',
            email      = '$fi_.$la_.$stamp@updated.example.com',
            updated_at = now()
          WHERE id IN (SELECT id FROM customers ORDER BY random() LIMIT 1)" >/dev/null \
        || extra="UPDATE customers row failed"
    else
      src_psql -c "UPDATE orders SET
            product    = '$(pick products)',
            quantity   = $((RANDOM % 5 + 1)),
            amount     = $(printf '%d.%02d' $((RANDOM % 900 + 10)) $((RANDOM % 100))),
            status     = '$(pick statuses)',
            updated_at = now()
          WHERE id IN (SELECT id FROM orders ORDER BY random() LIMIT 1)" >/dev/null \
        || extra="UPDATE orders row failed"
    fi
  elif [ $roll -lt 85 ]; then
    extra="DELETE random $table row"
    src_psql -c "DELETE FROM $table WHERE id IN (SELECT id FROM $table ORDER BY random() LIMIT 1)" >/dev/null \
      || extra="DELETE $table row failed"
  fi

  echo "round $round: INSERT customers#$new_c_id$( [ -z "$new_o_id" ] || echo ", INSERT orders#$new_o_id" )${extra:+ | $extra}"
  counts

  if [ -n "$new_c_id" ] && [ -n "$new_o_id" ]; then
    prev_c_id=$new_c_id; prev_o_id=$new_o_id
  else
    prev_c_id=''; prev_o_id=''
  fi
  sleep "$INTERVAL" & wait $!
done
