# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: bakdata/union/v1/union.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1c\x62\x61kdata/union/v1/union.proto\x12\x10\x62\x61kdata.union.v1\"\xbb\x02\n\x05Union\x12\x15\n\rcorporateName\x18\x01 \x01(\t\x12\r\n\x05rb_id\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61\x66in_id\x18\x03 \x01(\t\x12\x16\n\x0e\x62\x61\x66in_domicile\x18\x04 \x01(\t\x12\x15\n\rbafin_country\x18\x05 \x01(\t\x12\x17\n\x0frb_reference_id\x18\x06 \x01(\t\x12\x15\n\rrb_event_data\x18\x07 \x01(\t\x12\x15\n\rrb_event_type\x18\x08 \x01(\t\x12\x30\n\trb_status\x18\t \x01(\x0e\x32\x1d.bakdata.union.v1.UnionStatus\x12\x16\n\x0erb_information\x18\n \x01(\t\x12:\n\x0c\x62\x61\x66in_detail\x18\x0b \x03(\x0b\x32$.bakdata.union.v1.Union_Bafin_detail\"\xcd\x01\n\x12Union_Bafin_detail\x12\x15\n\rreportable_id\x18\x01 \x01(\r\x12\x12\n\nreportable\x18\x02 \x01(\t\x12\x1b\n\x13reportable_domicile\x18\x03 \x01(\t\x12\x1a\n\x12reportable_country\x18\x04 \x01(\t\x12\x14\n\x0crights_33_34\x18\x05 \x01(\x01\x12\x11\n\trights_38\x18\x06 \x01(\x01\x12\x11\n\trights_39\x18\x07 \x01(\x01\x12\x17\n\x0fpublishing_date\x18\x08 \x01(\t*M\n\x0bUnionStatus\x12\x16\n\x12STATUS_UNSPECIFIED\x10\x00\x12\x13\n\x0fSTATUS_INACTIVE\x10\x01\x12\x11\n\rSTATUS_ACTIVE\x10\x02\x62\x06proto3')

_UNIONSTATUS = DESCRIPTOR.enum_types_by_name['UnionStatus']
UnionStatus = enum_type_wrapper.EnumTypeWrapper(_UNIONSTATUS)
STATUS_UNSPECIFIED = 0
STATUS_INACTIVE = 1
STATUS_ACTIVE = 2


_UNION = DESCRIPTOR.message_types_by_name['Union']
_UNION_BAFIN_DETAIL = DESCRIPTOR.message_types_by_name['Union_Bafin_detail']
Union = _reflection.GeneratedProtocolMessageType('Union', (_message.Message,), {
  'DESCRIPTOR' : _UNION,
  '__module__' : 'bakdata.union.v1.union_pb2'
  # @@protoc_insertion_point(class_scope:bakdata.union.v1.Union)
  })
_sym_db.RegisterMessage(Union)

Union_Bafin_detail = _reflection.GeneratedProtocolMessageType('Union_Bafin_detail', (_message.Message,), {
  'DESCRIPTOR' : _UNION_BAFIN_DETAIL,
  '__module__' : 'bakdata.union.v1.union_pb2'
  # @@protoc_insertion_point(class_scope:bakdata.union.v1.Union_Bafin_detail)
  })
_sym_db.RegisterMessage(Union_Bafin_detail)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _UNIONSTATUS._serialized_start=576
  _UNIONSTATUS._serialized_end=653
  _UNION._serialized_start=51
  _UNION._serialized_end=366
  _UNION_BAFIN_DETAIL._serialized_start=369
  _UNION_BAFIN_DETAIL._serialized_end=574
# @@protoc_insertion_point(module_scope)
