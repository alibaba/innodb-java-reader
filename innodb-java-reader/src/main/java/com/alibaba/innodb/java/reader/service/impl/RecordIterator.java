/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;

import java.util.Iterator;
import java.util.List;

/**
 * Record iterator.
 *
 * @author xu.zx
 */
public class RecordIterator implements Iterator<GenericRecord> {

  protected List<GenericRecord> curr;

  protected int currIndex;

  protected long currPageNumber;

  protected long endPageNumber;

  protected Index indexPage;

  public RecordIterator(List<GenericRecord> curr) {
    this.curr = curr;
  }

  public RecordIterator(Index indexPage, long endPageNumber, List<GenericRecord> curr) {
    this.indexPage = indexPage;
    this.currPageNumber = indexPage.getPageNumber();
    this.endPageNumber = endPageNumber;
    this.curr = curr;
  }

  @Override
  public boolean hasNext() {
    return currIndex != curr.size();
  }

  @Override
  public GenericRecord next() {
    return curr.get(currIndex++);
  }

}
