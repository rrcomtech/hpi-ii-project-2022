#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/union/v1/union.proto
