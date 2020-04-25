/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Innodb index page service, providing read-only query operations.
 *
 * @author xu.zx
 */
public interface IndexService {

  /**
   * Query all records by single page.
   *
   * @param pageNumber page number (int type), can be leaf or non-leaf page
   * @return list of records
   */
  List<GenericRecord> queryByPageNumber(int pageNumber);

  /**
   * Query all records by single page.
   *
   * @param pageNumber page number (long type), can be leaf or non-leaf page
   * @return list of records
   */
  List<GenericRecord> queryByPageNumber(long pageNumber);

  /**
   * Query record by primary key in a tablespace.
   *
   * @param key primary key
   * @return record
   */
  GenericRecord queryByPrimaryKey(Object key);

  /**
   * Query all records in a tablespace.
   * <p>
   * Note this will cause out-of-memory if the table size is too big.
   *
   * @param recordPredicate optional. evaluating record, if true then it will be
   *                        added to result set, else skip it
   * @return all records
   */
  List<GenericRecord> queryAll(Optional<Predicate<GenericRecord>> recordPredicate);

  /**
   * Range query records by primary key in a tablespace.
   * <p>
   * Note this will cause out-of-memory if there are too many records within the range.
   *
   * @param lowerInclusiveKey lower bound, inclusive, if set to null means no limit for lower
   * @param upperExclusiveKey upper bound, exclusive, if set to null means no limit for upper
   * @param recordPredicate   optional. evaluating record, if true then it will be added to
   *                          result set, else skip it
   * @return list of records
   */
  List<GenericRecord> rangeQueryByPrimaryKey(Object lowerInclusiveKey, Object upperExclusiveKey,
                                             Optional<Predicate<GenericRecord>> recordPredicate);

  /**
   * Return an iterator to query all records of a tablespace.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   *
   * @return record iterator
   */
  Iterator<GenericRecord> getQueryAllIterator();

  /**
   * Return an iterator to do range query records by primary key in a tablespace.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   *
   * @param lowerInclusiveKey lower bound, inclusive, if set to null means no limit for lower
   * @param upperExclusiveKey upper bound, exclusive, if set to null means no limit for upper
   * @return record iterator
   */
  Iterator<GenericRecord> getRangeQueryIterator(Object lowerInclusiveKey, Object upperExclusiveKey);

}
