/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.schema.Schema;
import com.alibaba.innodb.java.reader.util.Utils;

import lombok.Data;
import lombok.ToString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Generic record representing one row.
 *
 * @author xu.zx
 */
@Data
public class GenericRecord {

  private long pageNumber;

  private final RecordHeader header;

  /**
   * 记录在物理文件格式中转为{@link com.alibaba.innodb.java.reader.util.SliceInput}，
   * 这个值表示primary key存储位置，不包含header，因为每个record header中的next record offset
   * 是一个相对位置，等于上一个record的primary key的位置加上这个offset 就是下一个record的primary
   * key位置。对于infimum和supremum来说就是literal字符串的起始位置。
   */
  private int primaryKeyPosition;

  /**
   * Table schema.
   */
  @ToString.Exclude
  private Schema schema;

  /**
   * Column values.
   */
  private Object[] values;

  /**
   * If the record is in non-leaf page, then this represents the child page number.
   */
  private long childPageNumber;

  public GenericRecord(RecordHeader header, Schema schema, long pageNumber) {
    this.header = header;
    this.schema = schema;
    this.pageNumber = pageNumber;
    this.values = new Object[schema.getColumnList().size()];
    if (header.getRecordType() == RecordType.INFIMUM) {
      put(schema.getPrimaryKeyColumn().getName(), Utils.MIN);
    }
    if (header.getRecordType() == RecordType.SUPREMUM) {
      put(schema.getPrimaryKeyColumn().getName(), Utils.MAX);
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public void put(String columnName, Object value) {
    checkNotNull(schema);
    checkNotNull(values);
    Schema.Field field = schema.getField(columnName);
    if (field == null) {
      throw new ReaderException("Not a valid schema for column: " + columnName);
    }

    values[field.getOrdinal()] = value;
  }

  public void put(int i, Object v) {
    checkNotNull(values);
    values[i] = v;
  }

  public Object get(String columnName) {
    checkNotNull(schema);
    checkNotNull(values);
    Schema.Field field = schema.getField(columnName);
    if (field == null) {
      return null;
    }
    return values[field.getOrdinal()];
  }

  public Object get(int i) {
    checkNotNull(values);
    return values[i];
  }

  public Object getPrimaryKey() {
    checkNotNull(schema);
    return get(schema.getPrimaryKeyColumn().getName());
  }

  public void setPrimaryKeyPosition(int primaryKeyPosition) {
    this.primaryKeyPosition = primaryKeyPosition;
  }

  public int nextRecordPosition() {
    return primaryKeyPosition + header.getNextRecOffset();
  }

  public boolean isLeafRecord() {
    return childPageNumber == 0;
  }

  public long getChildPageNumber() {
    return childPageNumber;
  }
}
