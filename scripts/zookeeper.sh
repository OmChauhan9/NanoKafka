#!/usr/bin/env bash
set -euo pipefail

# ZooKeeper's server needs metrics-core and snappy-java, which its pom marks as provided.
# The test classpath already carries both, so reuse it.
root=$(cd "$(dirname "$0")/.." && pwd)
port=${1:-2181}
data_dir=${2:-"${TMPDIR:-/tmp}/nanokafka-zookeeper"}

classpath=$(mvn -q -f "$root/pom.xml" dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=/dev/stdout)
mkdir -p "$data_dir"
echo "ZooKeeper on port $port, data in $data_dir"
exec java -Dzookeeper.admin.enableServer=false -cp "$classpath" \
	org.apache.zookeeper.server.ZooKeeperServerMain "$port" "$data_dir"
