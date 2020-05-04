/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;

import java.util.BitSet;
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
   * Special projection indicating no projection is needed, all fields will be included.
   */
  BitSet NOP_PROJECTION = new BitSet(0);

  /**
   * Query all records by single index page.
   *
   * @param pageNumber page number (int type), can be leaf or non-leaf page
   * @return list of records
   */
  List<GenericRecord> queryByPageNumber(int pageNumber);

  /**
   * Query all records by single index page.
   *
   * @param pageNumber page number (long type), can be leaf or non-leaf page
   * @return list of records
   */
  List<GenericRecord> queryByPageNumber(long pageNumber);

  /**
   * Query all records in a tablespace.
   * <p>
   * Note this will cause out-of-memory if the table size is too big.
   * Make sure fields are included in projection for predicate to use.
   *
   * @param recordPredicate  optional filtering, if predicate returns true upon
   *                         record, then it will be added to result set
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return all records
   */
  List<GenericRecord> queryAll(Optional<Predicate<GenericRecord>> recordPredicate,
                               Optional<List<String>> recordProjection);

  /**
   * Query record by primary key in a tablespace with projection list.
   * <p>
   * For single key, the the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param key              primary key, single key or a composite key
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return record
   */
  GenericRecord queryByPrimaryKey(List<Object> key, Optional<List<String>> recordProjection);

  /**
   * Range query records by primary key in a tablespace with a filter and projection.
   * <p>
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param lower            if rangeQuery is true, then this is the lower bound
   * @param lowerOperator    if rangeQuery is true, then this is the comparison operator for lower
   * @param upper            if rangeQuery is true, then this is the upper bound
   * @param upperOperator    if rangeQuery is true, then this is the comparison operator for upper
   * @param recordPredicate  optional filtering, if predicate returns true upon
   *                         record, then it will be added to result set
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return list of records
   */
  List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lower, ComparisonOperator lowerOperator,
                                             List<Object> upper, ComparisonOperator upperOperator,
                                             Optional<Predicate<GenericRecord>> recordPredicate,
                                             Optional<List<String>> recordProjection);

  /**
   * Return an iterator to query all records of a tablespace.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   *
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return record iterator
   */
  Iterator<GenericRecord> getQueryAllIterator(Optional<List<String>> recordProjection);

  /**
   * Return an iterator to do range query by primary key in a tablespace.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   * <p>
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param lower            if rangeQuery is true, then this is the lower bound
   * @param lowerOperator    if rangeQuery is true, then this is the comparison operator for lower
   * @param upper            if rangeQuery is true, then this is the upper bound
   * @param upperOperator    if rangeQuery is true, then this is the comparison operator for upper
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return record iterator
   */
  Iterator<GenericRecord> getRangeQueryIterator(List<Object> lower, ComparisonOperator lowerOperator,
                                                List<Object> upper, ComparisonOperator upperOperator,
                                                Optional<List<String>> recordProjection);

  /**
   * Load index page by page number.
   *
   * @param pageNumber page number
   * @return index page
   */
  Index loadIndexPage(long pageNumber);

}
