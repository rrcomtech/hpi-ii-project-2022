# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: bakdata/bafin/v1/bafin.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1c\x62\x61kdata/bafin/v1/bafin.proto\x12\x14\x62\x61kdata.corporate.v1\"\x8f\x01\n\rBafin_general\x12\x11\n\tissuer_id\x18\x01 \x01(\r\x12\x0e\n\x06issuer\x18\x02 \x01(\t\x12\x10\n\x08\x64omicile\x18\x03 \x01(\t\x12\x0f\n\x07\x63ountry\x18\x04 \x01(\t\x12\x38\n\x0c\x62\x61\x66in_detail\x18\x05 \x03(\x0b\x32\".bakdata.corporate.v1.Bafin_detail\"\xc7\x01\n\x0c\x42\x61\x66in_detail\x12\x15\n\rreportable_id\x18\x01 \x01(\r\x12\x12\n\nreportable\x18\x02 \x01(\t\x12\x1b\n\x13reportable_domicile\x18\x03 \x01(\t\x12\x1a\n\x12reportable_country\x18\x04 \x01(\t\x12\x14\n\x0crights_33_34\x18\x05 \x01(\x01\x12\x11\n\trights_38\x18\x06 \x01(\x01\x12\x11\n\trights_39\x18\x07 \x01(\x01\x12\x17\n\x0fpublishing_date\x18\x08 \x01(\tb\x06proto3')



_BAFIN_GENERAL = DESCRIPTOR.message_types_by_name['Bafin_general']
_BAFIN_DETAIL = DESCRIPTOR.message_types_by_name['Bafin_detail']
Bafin_general = _reflection.GeneratedProtocolMessageType('Bafin_general', (_message.Message,), {
  'DESCRIPTOR' : _BAFIN_GENERAL,
  '__module__' : 'bakdata.bafin.v1.bafin_pb2'
  # @@protoc_insertion_point(class_scope:bakdata.corporate.v1.Bafin_general)
  })
_sym_db.RegisterMessage(Bafin_general)

Bafin_detail = _reflection.GeneratedProtocolMessageType('Bafin_detail', (_message.Message,), {
  'DESCRIPTOR' : _BAFIN_DETAIL,
  '__module__' : 'bakdata.bafin.v1.bafin_pb2'
  # @@protoc_insertion_point(class_scope:bakdata.corporate.v1.Bafin_detail)
  })
_sym_db.RegisterMessage(Bafin_detail)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _BAFIN_GENERAL._serialized_start=55
  _BAFIN_GENERAL._serialized_end=198
  _BAFIN_DETAIL._serialized_start=201
  _BAFIN_DETAIL._serialized_end=400
# @@protoc_insertion_point(module_scope)
