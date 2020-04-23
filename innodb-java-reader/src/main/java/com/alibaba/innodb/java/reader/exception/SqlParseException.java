/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.exception;

/**
 * Sql parsing exception.
 *
 * @author xu.zx
 */
public class SqlParseException extends RuntimeException {

  public SqlParseException() {
    super();
  }

  public SqlParseException(String message) {
    super(message);
  }

  public SqlParseException(Throwable cause) {
    super(cause);
  }

  public SqlParseException(String message, Throwable cause) {
    super(message, cause);
  }

}
