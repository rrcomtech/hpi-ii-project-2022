#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/corporate/bafin/v1/corporate.proto
