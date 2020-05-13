/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.comparator;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Comparison operator.
 *
 * @author xu.zx
 */
public enum ComparisonOperator {

  /* greater than */
  GT(">"),
  /* greater than or equal */
  GTE(">="),
  /* less than */
  LT("<"),
  /* less than or equal */
  LTE("<="),
  /* nop */
  NOP("nop");

  String value;

  ComparisonOperator(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  // ---------- template method ---------- //

  private static Map<String, ComparisonOperator> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (ComparisonOperator operator : values()) {
      KVS.put(operator.value(), operator);
    }
  }

  public static ComparisonOperator parse(String op) {
    return KVS.get(op);
  }

}
