#!/bin/bash

KAFKA_CONNECT_ADDRESS=${1:-localhost}
KAFKA_CONNECT_PORT=${2:-8083}
BASE_CONFIG =${3:-"$(dirname $0)/elastic-sink-bafin_event.json"}
BASE_CONFIG2=${3:-"$(dirname $0)/elastic-sink-bafin_corporate.json"}
BASE_CONFIG3=${3:-"$(dirname $0)/elastic-sink-bafin_person.json"}
BASE_CONFIG4=${3:-"$(dirname $0)/elastic-sink-rb_corporate.json"}
BASE_CONFIG5=${3:-"$(dirname $0)/elastic-sink-rb_person.json"}
BASE_CONFIG6=${3:-"$(dirname $0)/elastic-sink-union.json"}
KAFKA_CONNECT_API="$KAFKA_CONNECT_ADDRESS:$KAFKA_CONNECT_PORT/connectors"

CONNECTOR_NAME =$(jq -r .name $BASE_CONFIG)
CONNECTOR_NAME2=$(jq -r .name $BASE_CONFIG2)
CONNECTOR_NAME3=$(jq -r .name $BASE_CONFIG3)
CONNECTOR_NAME4=$(jq -r .name $BASE_CONFIG4)
CONNECTOR_NAME5=$(jq -r .name $BASE_CONFIG5)
CONNECTOR_NAME6=$(jq -r .name $BASE_CONFIG6)

curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME
curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME2
curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME3
curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME4
curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME5
curl -Is -X DELETE $KAFKA_CONNECT_API/$CONNECTOR_NAME6
