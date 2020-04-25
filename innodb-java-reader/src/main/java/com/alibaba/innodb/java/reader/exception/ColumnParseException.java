/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.exception;

/**
 * Column parsing exception.
 *
 * @author xu.zx
 */
public class ColumnParseException extends ReaderException {

  public ColumnParseException() {
    super();
  }

  public ColumnParseException(String message) {
    super(message);
  }

  public ColumnParseException(Throwable cause) {
    super(cause);
  }

  public ColumnParseException(String message, Throwable cause) {
    super(message, cause);
  }

}
