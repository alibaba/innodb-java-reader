/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

/**
 * WriterException
 *
 * @author xu.zx
 */
public class WriterException extends RuntimeException {

  public WriterException() {
    super();
  }

  public WriterException(String message) {
    super(message);
  }

  public WriterException(Throwable cause) {
    super(cause);
  }

  public WriterException(String message, Throwable cause) {
    super(message, cause);
  }

}

