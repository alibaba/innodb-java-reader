/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.exception;

/**
 * Page loading exception.
 *
 * @author xu.zx
 */
public class PageLoadException extends ReaderException {

  public PageLoadException() {
    super();
  }

  public PageLoadException(String message) {
    super(message);
  }

  public PageLoadException(Throwable cause) {
    super(cause);
  }

  public PageLoadException(String message, Throwable cause) {
    super(message, cause);
  }

}
