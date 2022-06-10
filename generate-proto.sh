#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/bafin/v1/bafin_person.proto
