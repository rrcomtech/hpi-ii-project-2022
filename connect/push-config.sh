#!/bin/bash

KAFKA_CONNECT_ADDRESS=${1:-localhost}
KAFKA_CONNECT_PORT=${2:-8083}

BASE_CONFIG=${3:-"$(dirname $0)/elastic-sink-bafin_event.json"}
BASE_CONFIG2=${3:-"$(dirname $0)/elastic-sink-bafin_corporate.json"}
BASE_CONFIG3=${3:-"$(dirname $0)/elastic-sink-bafin_person.json"}
BASE_CONFIG4=${3:-"$(dirname $0)/elastic-sink-rb_corporate.json"}
BASE_CONFIG5=${3:-"$(dirname $0)/elastic-sink-rb_person.json"}
BASE_CONFIG6=${3:-"$(dirname $0)/elastic-sink-union.json"}

KAFKA_CONNECT_API="$KAFKA_CONNECT_ADDRESS:$KAFKA_CONNECT_PORT/connectors"

data=$(cat $BASE_CONFIG | jq -s '.[0]')
data2=$(cat $BASE_CONFIG2 | jq -s '.[0]')
data3=$(cat $BASE_CONFIG3 | jq -s '.[0]')
data4=$(cat $BASE_CONFIG4 | jq -s '.[0]')
data5=$(cat $BASE_CONFIG5 | jq -s '.[0]')
data6=$(cat $BASE_CONFIG6 | jq -s '.[0]')

curl -X POST $KAFKA_CONNECT_API --data "$data" -H "content-type:application/json"
curl -X POST $KAFKA_CONNECT_API --data "$data2" -H "content-type:application/json"
curl -X POST $KAFKA_CONNECT_API --data "$data3" -H "content-type:application/json"
curl -X POST $KAFKA_CONNECT_API --data "$data4" -H "content-type:application/json"
curl -X POST $KAFKA_CONNECT_API --data "$data5" -H "content-type:application/json"
curl -X POST $KAFKA_CONNECT_API --data "$data6" -H "content-type:application/json"
