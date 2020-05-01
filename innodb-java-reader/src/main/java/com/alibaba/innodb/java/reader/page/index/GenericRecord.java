/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.index;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.schema.TableDef;

import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.Data;
import lombok.ToString;

import static com.alibaba.innodb.java.reader.Constants.MAX_VAL;
import static com.alibaba.innodb.java.reader.Constants.MIN_VAL;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Record representing one row.
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
  @JsonIgnore
  @ToString.Exclude
  private TableDef tableDef;

  /**
   * Record fields.
   */
  private Object[] values;

  /**
   * If the record is in non-leaf page, then this represents the child page number.
   */
  private long childPageNumber;

  public GenericRecord(RecordHeader header, TableDef tableDef, long pageNumber) {
    this.header = header;
    this.tableDef = tableDef;
    this.pageNumber = pageNumber;
    this.values = new Object[tableDef.getColumnList().size()];
    if (header.getRecordType() == RecordType.INFIMUM) {
      if (this.tableDef.getPrimaryKeyColumnNum() > 0) {
        this.tableDef.getPrimaryKeyColumnNames().forEach(name -> {
          put(name, MIN_VAL);
        });
      }
    } else if (header.getRecordType() == RecordType.SUPREMUM) {
      if (this.tableDef.getPrimaryKeyColumnNum() > 0) {
        this.tableDef.getPrimaryKeyColumnNames().forEach(name -> {
          put(name, MAX_VAL);
        });
      }
    }
  }

  public TableDef getTableDef() {
    return tableDef;
  }

  public void put(String columnName, Object value) {
    checkNotNull(values);
    TableDef.Field field = tableDef.getField(columnName);
    if (field == null) {
      throw new ReaderException("Not valid for column: " + columnName);
    }

    values[field.getOrdinal()] = value;
  }

  public void put(int i, Object v) {
    checkNotNull(values);
    values[i] = v;
  }

  public Object get(String columnName) {
    checkNotNull(values);
    TableDef.Field field = tableDef.getField(columnName);
    if (field == null) {
      return null;
    }
    return values[field.getOrdinal()];
  }

  public Object get(int i) {
    checkNotNull(values);
    return values[i];
  }

  public List<Object> getPrimaryKey() {
    if (tableDef.getPrimaryKeyColumnNum() > 0) {
      List<Object> pkList = new ArrayList<>(tableDef.getPrimaryKeyColumnNum());
      for (String pkName : tableDef.getPrimaryKeyColumnNames()) {
        pkList.add(get(pkName));
      }
      return Collections.unmodifiableList(pkList);
    }
    return ImmutableList.of();
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
