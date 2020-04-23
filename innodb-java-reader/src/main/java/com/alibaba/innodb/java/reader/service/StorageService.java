/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.InnerPage;

import java.io.Closeable;
import java.io.IOException;

/**
 * Service for reading tablespace page.
 *
 * @author xu.zx
 */
public interface StorageService extends Closeable {

  /**
   * Open tablespace file.
   *
   * @param ibdFilePath file path
   * @throws IOException throws IOException when open file fails
   */
  void open(String ibdFilePath) throws IOException;

  /**
   * Load page.
   *
   * @param pageNumber page number
   * @return InnerPage
   * @throws ReaderException throws ReaderException when internal errors occurs
   */
  InnerPage loadPage(long pageNumber) throws ReaderException;

  /**
   * Load page header only.
   *
   * @param pageNumber page number
   * @return FilHeader
   * @throws ReaderException throws ReaderException when internal errors occurs
   */
  FilHeader loadPageHeader(long pageNumber) throws ReaderException;

  /**
   * Number of pages in the tablespace.
   *
   * @return Number of pages
   */
  long numOfPages();
}
