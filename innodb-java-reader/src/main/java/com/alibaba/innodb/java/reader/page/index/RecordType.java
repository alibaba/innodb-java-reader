/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.google.common.collect.Maps;

import com.alibaba.innodb.java.reader.util.IdAble;

import java.util.Map;

/**
 * RecordType
 *
 * @author xu.zx
 */
public enum RecordType implements IdAble<Integer> {

  /** leaf page conventional record type */
  CONVENTIONAL(0, "conventional"),
  /** non-leaf page node pointer record type */
  NODE_POINTER(1, "node pointer"),
  /** infimum record type */
  INFIMUM(2, "infimum"),
  /** supremum record type */
  SUPREMUM(3, "supremum");

  private int type;

  private String value;

  RecordType(int type, String value) {
    this.type = type;
    this.value = value;
  }

  public int type() {
    return type;
  }

  public String value() {
    return value;
  }

  @Override
  public Integer id() {
    return type;
  }

  // ---------- template method ---------- //

  private static Map<Integer, RecordType> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (RecordType recordType : values()) {
      KVS.put(recordType.type(), recordType);
    }
  }

  public static RecordType parse(int type) {
    return KVS.get(type);
  }

}

