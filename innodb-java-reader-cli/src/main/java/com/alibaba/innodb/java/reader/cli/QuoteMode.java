/*
 * Copyright (C) 2020 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Quote mode.
 *
 * @author Adam Jurcik
 */
public enum QuoteMode {

  /**
   * Quote mode enums
   */

  ALL("all", "quote all fields"),
  NON_NULL("nonnull", "quote all non-null fields"),
  NON_NUMERIC("nonnumeric", "quote all non-numeric fields"),
  NONE("none", "never quote fields");

  QuoteMode(final String mode, final String desc) {
    this.mode = mode;
    this.desc = desc;
  }

  private String mode;

  private String desc;

  public String getMode() {
    return mode;
  }

  public void setMode(String type) {
    this.mode = type;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  // ---------- template method ---------- //

  private static Map<String, QuoteMode> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (QuoteMode mode : values()) {
      KVS.put(mode.getMode(), mode);
    }
  }

  public static QuoteMode parse(String mode) {
    return KVS.get(mode);
  }
}
