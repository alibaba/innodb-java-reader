/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.util.IdAble;

/**
 * Record header flag.
 *
 * @author xu.zx
 */
public enum RecordInfoFlag implements IdAble<Integer> {

  /** min record flag */
  MIN_REC(1, "this record is the minimum record in a non-leaf level of the B+Tree"),
  /** record delete flag */
  DELETE_MARKED(2, "this record will be actually deleted by a purge operation in the future");

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

  public static RecordInfoFlag parse(int type) {
    if (type == MIN_REC.type) {
      return MIN_REC;
    } else if (type == DELETE_MARKED.type) {
      return DELETE_MARKED;
    }
    return null;
  }

}

