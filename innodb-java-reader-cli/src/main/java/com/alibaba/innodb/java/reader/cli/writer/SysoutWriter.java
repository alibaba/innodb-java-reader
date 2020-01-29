/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

/**
 * Print out to console.
 *
 * @author xu.zx
 */
public class SysoutWriter implements Writer {

  @Override
  public void open() {

  }

  @Override
  public void write(String text) throws WriterException {
    System.out.println(text);
  }

  @Override
  public void close() throws WriterException {

  }

  @Override
  public boolean ifNewLineAfterWrite() {
    return false;
  }
}
