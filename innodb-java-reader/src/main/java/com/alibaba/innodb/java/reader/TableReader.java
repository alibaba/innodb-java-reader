/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.TableDef;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Reader to query upon an Innodb file with suffix of <tt>*.ibd</tt>.
 * All operations are thread-safe.
 *
 * @author xu.zx
 */
public interface TableReader extends Closeable {

  /**
   * Open table.
   */
  void open();

  /**
   * Return total page count.
   *
   * @return number of pages.
   */
  long getNumOfPages();

  /**
   * Read all pages into memory which may cause OutOfMemory when tablespace file size is too big.
   * Use {@link #getPageIterator()} if file size is too big.
   *
   * @return list of AbstractPage
   */
  List<AbstractPage> readAllPages();

  /**
   * Get page iterator.
   *
   * @return Iterator of AbstractPage
   */
  Iterator<AbstractPage> getPageIterator();

  /**
   * Read all page headers into memory.
   *
   * @return list of FilHeader
   */
  List<FilHeader> readAllPageHeaders();

  /**
   * Read one page as {@link AbstractPage}.
   *
   * @param pageNumber page number
   * @return AbstractPage
   */
  AbstractPage readPage(long pageNumber);

  /**
   * Get all index page filling rate, use iterator pattern to avoid OutOfMemory.
   *
   * @return filling rate
   */
  double getAllIndexPageFillingRate();

  /**
   * Get single index page filling rate.
   *
   * @param pageNumber page number
   * @return filling rate
   */
  double getIndexPageFillingRate(int pageNumber);

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
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param key primary key, single key or a composite key
   * @return record
   */
  GenericRecord queryByPrimaryKey(List<Object> key);

  /**
   * Query all records in a tablespace.
   * <p>
   * Note this will cause out-of-memory if the table size is too big.
   *
   * @return all records
   */
  List<GenericRecord> queryAll();

  /**
   * Query all records in a tablespace with a filter.
   * <p>
   * Note this will cause out-of-memory if the table size is too big.
   *
   * @param recordPredicate optional. evaluating record, if true then it will be
   *                        added to result set, else skip it
   * @return all records
   */
  List<GenericRecord> queryAll(Predicate<GenericRecord> recordPredicate);

  /**
   * Range query records by primary key in a tablespace.
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param lower         if rangeQuery is true, then this is the lower bound
   * @param lowerOperator if rangeQuery is true, then this is the comparison operator for lower
   * @param upper         if rangeQuery is true, then this is the upper bound
   * @param upperOperator if rangeQuery is true, then this is the comparison operator for upper
   * @return list of records
   */
  List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lower, ComparisonOperator lowerOperator,
                                             List<Object> upper, ComparisonOperator upperOperator);

  /**
   * Range query records by primary key in a tablespace with a filter.
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param lower           if rangeQuery is true, then this is the lower bound
   * @param lowerOperator   if rangeQuery is true, then this is the comparison operator for lower
   * @param upper           if rangeQuery is true, then this is the upper bound
   * @param upperOperator   if rangeQuery is true, then this is the comparison operator for upper
   * @param recordPredicate optional. evaluating record, if true then it will be added to
   *                        result set, else skip it
   * @return list of records
   */
  List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lower, ComparisonOperator lowerOperator,
                                             List<Object> upper, ComparisonOperator upperOperator,
                                             Predicate<GenericRecord> recordPredicate);

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
   * <p>
   * For single key the list size should be one, for composite key the size
   * will be more than one.
   *
   * @param lower         if rangeQuery is true, then this is the lower bound
   * @param lowerOperator if rangeQuery is true, then this is the comparison operator for lower
   * @param upper         if rangeQuery is true, then this is the upper bound
   * @param upperOperator if rangeQuery is true, then this is the comparison operator for upper
   * @return record iterator
   */
  Iterator<GenericRecord> getRangeQueryIterator(List<Object> lower, ComparisonOperator lowerOperator,
                                                List<Object> upper, ComparisonOperator upperOperator);

  /**
   * Return table definition.
   *
   * @return table definition
   */
  TableDef getTableDef();

  /**
   * Close.
   */
  @Override
  void close();
}
