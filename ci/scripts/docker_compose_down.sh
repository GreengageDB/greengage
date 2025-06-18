#!/bin/bash

# For old docker-compose plugin where `ls` command not works
known_locations=(
./greengage/ci/
)

# Trap "emergency_exit" if defined
type emergency_exit &>/dev/null && trap emergency_exit ERR || true

# Docker Compose binary check
set +e ; trap - ERR
docker compose version  >/dev/null 2>&1; exist=$? # Preffered
if [ $exist -eq "0" ]; then
  bin='docker compose'
else
  docker-compose version  >/dev/null 2>&1; exist=$? # Legacy
  if [ $exist -eq "0" ]; then
    bin='docker-compose'
  else
    echo No Docker Compose detected. Exiting # Absent
    exit 1
  fi
fi
[ -n "$DEBUG" ] && echo Docker Compose binary detected: \"$bin\"
set -e ;  type emergency_exit &>/dev/null && trap emergency_exit ERR || true

# Find working Docker Composes
if [ "$bin" = "docker compose" ]; then
  echo -en "Finding working Docker Composes..."
  composes=$($bin ls -q)
  if [ -n "$composes" ]; then
    echo -e "\nFound: $composes"
    for compose in $composes; do
      echo "Stopping '$compose'"
      COMPOSE_HTTP_TIMEOUT=300 $bin -p $compose down
    done
  else
    echo -e " nothing"
  fi
elif [ -n "$known_locations" ]; then
  echo Try to stop Docker Composes from known locations...
  for dir in "${known_locations[@]}"; do
    echo -n Checking $dir...
    [ -d $dir ] && (echo -ne " found\n"; find $dir -type f -name docker-compose.yml -exec docker-compose -f {} down \;) || echo -ne " not found\n"
  done
fi

## May be required
# sudo systemctl restart docker
