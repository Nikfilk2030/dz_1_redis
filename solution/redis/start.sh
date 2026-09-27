set -eu

node=${REDIS_NODE_NAME:?}
case "$node" in
  redis-master|redis-replica-1|redis-replica-2) ;;
  *)
    printf 'Unknown Redis node: %s\n' "$node" >&2
    exit 1
    ;;
esac

config=/data/redis.conf
if [ ! -f "$config" ]; then
  if [ -n "$(ls -A /data)" ]; then
    printf 'Redis configuration missing while persistent data exists\n' >&2
    exit 1
  fi

  tmp=/data/redis.conf.tmp
  cp /usr/local/etc/redis/redis.conf "$tmp"
  printf 'replica-announce-ip %s\n' "$node" >> "$tmp"
  if [ "$node" != redis-master ]; then
    printf 'replicaof redis-master 6379\n' >> "$tmp"
  fi
  mv "$tmp" "$config"
fi

exec docker-entrypoint.sh redis-server "$config"
