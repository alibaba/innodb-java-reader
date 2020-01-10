/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.InnerPage;
import com.alibaba.innodb.java.reader.service.StorageService;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/**
 * DirectIoStorageServiceImpl
 *
 * @author xu.zx
 */
@Slf4j
public class DirectIoStorageServiceImpl implements StorageService {

  @Override
  public void open(String ibdFilePath) throws IOException {

  }

  @Override
  public InnerPage loadPage(long pageNumber) throws ReaderException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FilHeader loadPageHeader(long pageNumber) throws ReaderException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long numOfPages() {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
