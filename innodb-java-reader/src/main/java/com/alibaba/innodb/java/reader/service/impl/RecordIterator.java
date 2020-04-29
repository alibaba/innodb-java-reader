/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;

import java.util.Iterator;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

/**
 * Record iterator.
 *
 * @author xu.zx
 */
@Slf4j
public class RecordIterator implements Iterator<GenericRecord> {

  protected boolean initialized;

  protected List<GenericRecord> curr;

  protected int currIndex;

  protected long currPageNumber;

  protected long endPageNumber;

  protected Index indexPage;

  public RecordIterator() {
  }

  public RecordIterator(List<GenericRecord> curr) {
    this.curr = curr;
  }

  /**
   * Initialization includes:
   * <ul>
   *   <li>1. Looking up starting and ending page number.</li>
   *   <li>2. Load starting page and store records in {@link #curr}</li>
   * </ul>
   */
  public void init() {
    throw new IllegalStateException("Should overwrite init method");
  }

  @Override
  public boolean hasNext() {
    if (!initialized) {
      init();
    }
    return doHasNext();
  }

  public boolean doHasNext() {
    return currIndex != curr.size();
  }

  @Override
  public GenericRecord next() {
    return curr.get(currIndex++);
  }

}
