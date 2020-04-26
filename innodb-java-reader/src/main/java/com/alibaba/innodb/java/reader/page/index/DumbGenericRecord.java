/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.schema.TableDef;

/**
 * For range query, this is the starting and ending pivot record.
 *
 * @author xu.zx
 */
public class DumbGenericRecord extends GenericRecord {

  private GenericRecord delegate;

  public DumbGenericRecord(RecordHeader header, TableDef tableDef, long pageNumber) {
    super(header, tableDef, pageNumber);
  }

  public DumbGenericRecord(GenericRecord record) {
    super(record.getHeader(), record.getTableDef(), record.getPageNumber());
    this.delegate = record;
  }

  public GenericRecord getDelegate() {
    return delegate;
  }
}
