/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.exception;

/**
 * Framework root exception.
 * More specific exceptions can extend this class.
 *
 * @author xu.zx
 */
public class ReaderException extends RuntimeException {

  public ReaderException() {
    super();
  }

  public ReaderException(String message) {
    super(message);
  }

  public ReaderException(Throwable cause) {
    super(cause);
  }

  public ReaderException(String message, Throwable cause) {
    super(message, cause);
  }

}
