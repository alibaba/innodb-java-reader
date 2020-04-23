/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

/**
 * For enum types.
 *
 * @author xu.zx
 */
public interface IdAble<E extends Number> {

  /**
   * Get identifier.
   *
   * @return id
   */
  E id();

}
