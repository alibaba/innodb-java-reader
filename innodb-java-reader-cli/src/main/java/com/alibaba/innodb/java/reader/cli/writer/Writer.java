/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

import java.io.Closeable;

/**
 * Writer
 *
 * @author xu.zx
 */
public interface Writer extends Closeable {

  /**
   * Open
   */
  void open();

  /**
   * Write text
   *
   * @param text text
   */
  void write(String text) throws WriterException;

  /**
   * If new line after writing
   *
   * @return if new line after writing
   */
  boolean ifNewLineAfterWrite();
}
