#!/usr/bin/env bash
# Runs a CQL statement against an Instance, connecting with the host, port and
# credentials from its connection Secret, so the published details are tested too.
#
# Usage: cql.sh <namespace> <instance> <statement>
set -euo pipefail

namespace=$1
instance=$2
statement=$3

connection_field() {
	kubectl -n "$namespace" get secret "${instance}-conn" -o "jsonpath={.data.$1}" | base64 -d
}

pod=$(kubectl -n "$namespace" get pod \
	-l "cassandra.datastax.com/cluster=${instance}" \
	--field-selector=status.phase=Running \
	-o jsonpath='{.items[0].metadata.name}')

kubectl -n "$namespace" exec "$pod" -c cassandra -- \
	cqlsh "$(connection_field host)" "$(connection_field port)" \
	-u "$(connection_field username)" -p "$(connection_field password)" \
	-e "$statement"
