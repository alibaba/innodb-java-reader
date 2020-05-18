/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

/**
 * Decorator pattern of record iterator.
 * <p>
 * Sub class can override {@link #next()} to update the returned record.
 * For example, in secondary key querying situation, we should look up record
 * based on the secondary key which contains primary key.
 * <pre>
 *   return new DecoratedRecordIterator(skRecordIterator) {
 *
 *       public GenericRecord next() {
 *         GenericRecord skRecord = super.next();
 *         List&lt;Object&gt; primaryKey = new ArrayList&lt;&gt;(tableDef.getPrimaryKeyColumnNum());
 *         for (String pkName : tableDef.getPrimaryKeyColumnNames()) {
 *           primaryKey.add(skRecord.get(pkName));
 *         }
 *         return queryByPrimaryKey(primaryKey);
 *       }
 *     };
 * </pre>
 *
 * @author xu.zx
 */
@Slf4j
public class DecoratedRecordIterator implements Iterator<GenericRecord> {

  protected Iterator<GenericRecord> recordIterator;

  public DecoratedRecordIterator(Iterator<GenericRecord> recordIterator) {
    this.recordIterator = recordIterator;
  }

  @Override
  public boolean hasNext() {
    return recordIterator.hasNext();
  }

  @Override
  public GenericRecord next() {
    return recordIterator.next();
  }

}
