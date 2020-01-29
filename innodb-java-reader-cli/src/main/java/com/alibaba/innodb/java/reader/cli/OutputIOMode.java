/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * OutputIOMode
 *
 * @author xu.zx
 */
public enum OutputIOMode {

  /**
   * Output io mode enums
   */

  BUFFER("buffer", "use Java NIO 2.0 buffer io which leverages page cache"),
  MMAP("mmap", "use mmap to write"),
  DIRECT("direct", "use direct io to writer");

  OutputIOMode(final String mode, final String desc) {
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

  private static Map<String, OutputIOMode> KVS = Maps.newHashMapWithExpectedSize(values().length);

  static {
    for (OutputIOMode mode : values()) {
      KVS.put(mode.getMode(), mode);
    }
  }

  public static OutputIOMode parse(String mode) {
    return KVS.get(mode);
  }
}
