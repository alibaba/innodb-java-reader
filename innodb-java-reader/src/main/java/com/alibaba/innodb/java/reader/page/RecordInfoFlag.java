/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.google.common.collect.Maps;

import com.alibaba.innodb.java.reader.util.IdAble;

import java.util.Map;

/**
 * Record header flag.
 *
 * @author xu.zx
 */
public enum RecordInfoFlag implements IdAble<Integer> {

  /** min record flag */
  MIN_REC(1, "this record is the minimum record in a non-leaf level of the B+Tree"),
  /** record delete flag */
  DELETE_MARKED(2, "will be actually deleted by a purge operation in the future");

  private int type;

  private String value;

  RecordInfoFlag(int type, String value) {
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

  private static Map<Integer, RecordInfoFlag> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (RecordInfoFlag recordType : values()) {
      KVS.put(recordType.type(), recordType);
    }
  }

  public static RecordInfoFlag parse(int type) {
    return KVS.get(type);
  }
}

