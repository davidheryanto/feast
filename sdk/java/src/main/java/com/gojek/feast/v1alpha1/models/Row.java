package com.gojek.feast.v1alpha1.models;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.types.ValueProto.Value;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class Row {
  private Timestamp entity_timestamp;
  private final Map<String, Value> fields = new HashMap<>();

  public static Row create() {
    return new Row();
  }

  public Map<String, Value> getFields() {
    return fields;
  }

  public Row setEntityTimestamp(Instant timestamp) {
    entity_timestamp = Timestamps.fromMillis(timestamp.toEpochMilli());
    return this;
  }

  public Timestamp getEntityTimestamp() {
    return entity_timestamp;
  }

  public Row setEntityTimestamp(String dateTime) {
    entity_timestamp = Timestamps.fromMillis(Instant.parse(dateTime).toEpochMilli());
    return this;
  }

  public Row set(String fieldName, Object value) {
    String valueType = value.getClass().getCanonicalName();
    switch (valueType) {
      case "java.lang.Integer":
        fields.put(fieldName, Value.newBuilder().setInt32Val((int) value).build());
        break;
      case "java.lang.Long":
        fields.put(fieldName, Value.newBuilder().setInt64Val((long) value).build());
        break;
      case "java.lang.Float":
        fields.put(fieldName, Value.newBuilder().setFloatVal((float) value).build());
        break;
      case "java.lang.Double":
        fields.put(fieldName, Value.newBuilder().setDoubleVal((double) value).build());
        break;
      case "java.lang.String":
        fields.put(fieldName, Value.newBuilder().setStringVal((String) value).build());
        break;
      case "byte[]":
        fields.put(
            fieldName, Value.newBuilder().setBytesVal(ByteString.copyFrom((byte[]) value)).build());
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Type '%s' is unsupported in Feast. Please use one of these value types: Integer, Long, Float, Double, String, byte[].",
                valueType));
    }
    return this;
  }
}
