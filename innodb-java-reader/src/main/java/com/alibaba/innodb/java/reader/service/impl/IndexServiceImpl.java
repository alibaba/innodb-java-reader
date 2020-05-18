/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.column.ColumnFactory;
import com.alibaba.innodb.java.reader.comparator.ComparisonOperator;
import com.alibaba.innodb.java.reader.comparator.KeyComparator;
import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.InnerPage;
import com.alibaba.innodb.java.reader.page.PageType;
import com.alibaba.innodb.java.reader.page.blob.Blob;
import com.alibaba.innodb.java.reader.page.index.DumbGenericRecord;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.index.OverflowPagePointer;
import com.alibaba.innodb.java.reader.page.index.RecordHeader;
import com.alibaba.innodb.java.reader.page.index.RecordType;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.Workaround;
import com.alibaba.innodb.java.reader.service.IndexService;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.Pair;
import com.alibaba.innodb.java.reader.util.SliceInput;
import com.alibaba.innodb.java.reader.util.ThreadContext;
import com.alibaba.innodb.java.reader.util.Utils;

import org.apache.commons.collections.CollectionUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.ROOT_PAGE_NUMBER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_BODY;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_REC_HEADER;
import static com.alibaba.innodb.java.reader.column.ColumnType.BLOB_TEXT_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.BLOB_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.TEXT_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.VARBINARY;
import static com.alibaba.innodb.java.reader.column.ColumnType.VARCHAR;
import static com.alibaba.innodb.java.reader.comparator.ComparisonOperator.GT;
import static com.alibaba.innodb.java.reader.comparator.ComparisonOperator.GTE;
import static com.alibaba.innodb.java.reader.comparator.ComparisonOperator.LT;
import static com.alibaba.innodb.java.reader.comparator.ComparisonOperator.LTE;
import static com.alibaba.innodb.java.reader.comparator.ComparisonOperator.NOP;
import static com.alibaba.innodb.java.reader.config.ReaderSystemProperty.ENABLE_THROW_EXCEPTION_FOR_UNSUPPORTED_MYSQL80_LOB;
import static com.alibaba.innodb.java.reader.util.Utils.anyElementEmpty;
import static com.alibaba.innodb.java.reader.util.Utils.constructMaxRecord;
import static com.alibaba.innodb.java.reader.util.Utils.constructMinRecord;
import static com.alibaba.innodb.java.reader.util.Utils.expandRecord;
import static com.alibaba.innodb.java.reader.util.Utils.isOptionalPresent;
import static com.alibaba.innodb.java.reader.util.Utils.makeNotNull;
import static com.alibaba.innodb.java.reader.util.Utils.noneEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

/**
 * Innodb index page service, providing read-only query operations.
 *
 * @author xu.zx
 */
@Slf4j
public class IndexServiceImpl implements IndexService {

  private TableDef tableDef;

  private StorageService storageService;

  private KeyComparator keyComparator;

  public IndexServiceImpl(StorageService storageService, TableDef tableDef,
                          KeyComparator keyComparator) {
    this.storageService = storageService;
    this.tableDef = tableDef;
    this.keyComparator = keyComparator;
  }

  /**
   * Query all records by single index page.
   *
   * @param pageNumber page number (int type), can be leaf or non-leaf page
   * @return list of records
   */
  @Override
  public List<GenericRecord> queryByPageNumber(int pageNumber) {
    return queryByPageNumber((long) pageNumber);
  }

  /**
   * Query all records by single index page.
   *
   * @param pageNumber page number (long type), can be leaf or non-leaf page
   * @return list of records
   */
  @Override
  public List<GenericRecord> queryByPageNumber(long pageNumber) {
    return queryWithinIndexPage(tableDef, loadIndexPage(pageNumber));
  }

  /**
   * Query all records within one index page with all fields included
   * (nop projection, {@link #NOP_PROJECTION} is used).
   *
   * @param tableDef table definition
   * @param index    index page
   * @return list of records
   */
  private List<GenericRecord> queryWithinIndexPage(TableDef tableDef, Index index) {
    return queryWithinIndexPage(tableDef, index, false, ImmutableList.of(),
        NOP, ImmutableList.of(), NOP, NOP_PROJECTION);
  }

  /**
   * Query all records within one index page with projection.
   *
   * @param tableDef   table definition
   * @param index      index page
   * @param projection projection of selected column ordinal in bitmap
   * @return list of records
   */
  private List<GenericRecord> queryWithinIndexPage(TableDef tableDef, Index index, BitSet projection) {
    return queryWithinIndexPage(tableDef, index, false, ImmutableList.of(),
        NOP, ImmutableList.of(), NOP, projection);
  }

  /**
   * Query within one index page, range query is supported.
   *
   * @param tableDef      table definition
   * @param index         index page
   * @param rangeQuery    if the query is range enabled
   * @param lower         if rangeQuery is true, then this is the lower bound
   * @param lowerOperator if rangeQuery is true, then this is the comparison operator for lower
   * @param upper         if rangeQuery is true, then this is the upper bound
   * @param upperOperator if rangeQuery is true, then this is the comparison operator for upper
   * @param projection    projection of selected column ordinal in bitmap
   * @return list of records
   */
  private List<GenericRecord> queryWithinIndexPage(TableDef tableDef, Index index, boolean rangeQuery,
                                                   List<Object> lower, ComparisonOperator lowerOperator,
                                                   List<Object> upper, ComparisonOperator upperOperator,
                                                   BitSet projection) {
    checkKey(lower, lowerOperator, upper, upperOperator);

    // num of heap records - system records
    List<GenericRecord> result = new ArrayList<>(index.getIndexHeader().getNumOfRecs());
    SliceInput sliceInput = index.getSliceInput();

    if (log.isDebugEnabled()) {
      log.debug("{}, {}", index.getIndexHeader(), index.getFsegHeader());
    }
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    int recCounter = 0;
    sliceInput.setPosition(nextRecPos);

    // only range query needed
    boolean noneEmpty = false;
    boolean lowerNotEmpty = false;
    boolean upperNotEmpty = false;

    if (rangeQuery) {
      noneEmpty = noneEmpty(lower, upper);
      lowerNotEmpty = isNotEmpty(lower);
      upperNotEmpty = isNotEmpty(upper);
    }

    while (nextRecPos != supremum.getPrimaryKeyPosition()) {
      GenericRecord record = readRecord(tableDef, index.getPageNumber(), sliceInput,
          index.isLeafPage(), projection);
      if (rangeQuery) {
        if (noneEmpty) {
          if (qualified(tableDef.getPrimaryKeyColumns(), record.getPrimaryKey(),
              lower, lowerOperator, upper, upperOperator)) {
            result.add(record);
          }
        } else if (lowerNotEmpty) {
          if (lowerQualified(tableDef.getPrimaryKeyColumns(), record.getPrimaryKey(),
              lower, lowerOperator)) {
            result.add(record);
          }
        } else if (upperNotEmpty) {
          if (upperQualified(tableDef.getPrimaryKeyColumns(), record.getPrimaryKey(),
              upper, upperOperator)) {
            result.add(record);
            break;
          }
        } else {
          throw new ReaderException("Lower and upper should not be both empty");
        }
      } else {
        result.add(record);
      }
      nextRecPos = record.nextRecordPosition();
      recCounter++;
    }

    // double-check
    if (recCounter != index.getIndexHeader().getNumOfRecs()) {
      log.error("Records read and numOfRecs in index header not match!");
    }

    return result;
  }

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
  @Override
  public List<GenericRecord> queryAll(Optional<Predicate<GenericRecord>> recordPredicate,
                                      Optional<List<String>> recordProjection) {
    List<GenericRecord> recordList = new ArrayList<>();
    traverseBPlusTree(tableDef, ROOT_PAGE_NUMBER, recordList, recordPredicate, transformProjection(recordProjection));
    return recordList;
  }

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
  @Override
  public GenericRecord queryByPrimaryKey(List<Object> key, Optional<List<String>> recordProjection) {
    checkArgument(isNotEmpty(key), "Key should not be empty");
    checkArgument(!anyElementEmpty(key), "Key should not contain null elements");
    checkArgument(key.size() == tableDef.getPrimaryKeyColumnNum(), "Search key count not match");

    BitSet projection = transformProjection(recordProjection);
    Index index = loadIndexPage(ROOT_PAGE_NUMBER);
    checkState(index.isRootPage(), "Root page is wrong which should not happen");
    GenericRecord record = binarySearchByDirectory(tableDef, ROOT_PAGE_NUMBER, index, key, projection);
    if (record == null || DumbGenericRecord.class.equals(record.getClass())) {
      return null;
    }
    return record;
  }

  /**
   * Return an iterator to query all records of a tablespace.
   * <p>
   * Leverage {@link #getRangeQueryIterator(List, ComparisonOperator, List, ComparisonOperator, Optional, boolean)}.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   *
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @param ascOrder         if set result records in ascending order
   * @return all records
   */
  @Override
  public Iterator<GenericRecord> getQueryAllIterator(Optional<List<String>> recordProjection,
                                                     boolean ascOrder) {
    return getRangeQueryIterator(ImmutableList.of(), NOP, ImmutableList.of(), NOP, recordProjection, ascOrder);
  }

  /**
   * The implementation is different from the way {@link #queryAll(Optional, Optional)} works.
   * <p>
   * This method will do point query to search the nearest lower and upper bound record, then visit the leaf
   * page, go through all the level 0 pages by the double-linked pages.
   * While {@link #queryAll(Optional, Optional)} traverses b+ tree in a depth-first way.
   *
   * @param lower            lower bound
   * @param lowerOperator    comparison operator for lower
   * @param upper            upper bound
   * @param upperOperator    comparison operator for upper
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @param ascOrder         if set result records in ascending order
   * @return record iterator
   */
  @Override
  public Iterator<GenericRecord> getRangeQueryIterator(List<Object> lower, ComparisonOperator lowerOperator,
                                                       List<Object> upper, ComparisonOperator upperOperator,
                                                       Optional<List<String>> recordProjection,
                                                       final boolean ascOrder) {
    return getRangeQueryIterator(tableDef, ROOT_PAGE_NUMBER, lower, lowerOperator, upper, upperOperator,
        recordProjection, ascOrder);
  }

  /**
   * Return record iterator by secondary key (SK) in a tablespace. This is first go through all
   * secondary keys and look up record back to clustered index.
   * <p>
   * This is friendly to memory since only one page is loaded per batch.
   * <p>
   * For single key the lower or upper list size should be one, for composite key the size
   * will be more than one.
   *
   * @param skName        secondary key name in <code>SHOW CREATE TABLE</code> command
   * @param lower         lower bound
   * @param lowerOperator comparison operator for lower
   * @param upper         upper bound
   * @param upperOperator comparison operator for upper
   * @param ascOrder      if set result records in ascending order
   * @return record iterator, record is composed by secondary key columns and primary key columns
   */
  @Override
  public Iterator<GenericRecord> getQueryIteratorBySk(String skName,
                                                      List<Object> lower, ComparisonOperator lowerOperator,
                                                      List<Object> upper, ComparisonOperator upperOperator,
                                                      Optional<List<String>> recordProjection,
                                                      boolean ascOrder) {
    try {
      Optional<Integer> skOrdinal = ThreadContext.getSkOrdinal() != null
          ? Optional.of(ThreadContext.getSkOrdinal()) : Optional.empty();
      TableDef skTableDef = tableDef.buildSkTableDef(skName, skOrdinal);
      long skRootPageNumber =
          Workaround.getSkRootPageNumber(tableDef, skName, skOrdinal, rootPageNumber -> loadIndexPage(rootPageNumber));
      log.debug("Build secondary key table {}", skTableDef);
      Iterator<GenericRecord> skRecordIterator =
          getSkRangeQueryIterator(skTableDef, skRootPageNumber, makeNotNull(lower), lowerOperator,
              makeNotNull(upper), upperOperator, ascOrder);
      if (satisfyCoveringIndex(skTableDef, recordProjection)) {
        log.debug("Covering index is satisfied skTableDef={}, projection={}", skTableDef, recordProjection.get());
        return skRecordIterator;
      }
      return new DecoratedRecordIterator(skRecordIterator) {

        @Override
        public GenericRecord next() {
          GenericRecord skRecord = super.next();
          // create primary key
          List<Object> primaryKey = new ArrayList<>(tableDef.getPrimaryKeyColumnNum());
          for (String pkName : tableDef.getPrimaryKeyColumnNames()) {
            primaryKey.add(skRecord.get(pkName));
          }
          // look up by clustered index
          return queryByPrimaryKey(primaryKey, recordProjection);
        }
      };
    } finally {
      // we assume the caller would call clean(), but here we remove one key aggressively
      ThreadContext.remove(ThreadContext.SK_ORDINAL_KEY);
    }
  }

  /**
   * If covering index is satisfied, this will help to determine whether to look up back to
   * clustered index.
   *
   * @param skTableDef       secondary key virtual table definition
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @return true if covering index is met
   */
  private boolean satisfyCoveringIndex(TableDef skTableDef, Optional<List<String>> recordProjection) {
    if (recordProjection.isPresent()) {
      List<String> projection = recordProjection.get();
      return projection.stream()
          .allMatch(p -> skTableDef.getColumnNames().contains(p));
    }
    return false;
  }

  /**
   * Return secondary key (SK) record iterator in a tablespace.
   *
   * @param skTableDef       secondary key virtual table definition
   * @param skRootPageNumber secondary key root page number
   * @param lower            lower bound
   * @param lowerOperator    comparison operator for lower
   * @param upper            upper bound
   * @param upperOperator    comparison operator for upper
   * @param ascOrder         if set result records in ascending order
   * @return record iterator, record is composed by secondary key columns and primary key columns
   */
  public Iterator<GenericRecord> getSkRangeQueryIterator(TableDef skTableDef, long skRootPageNumber,
                                                         List<Object> lower, ComparisonOperator lowerOperator,
                                                         List<Object> upper, ComparisonOperator upperOperator,
                                                         boolean ascOrder) {
    return getRangeQueryIterator(skTableDef, skRootPageNumber,
        lower, lowerOperator, upper, upperOperator, Optional.empty(), ascOrder);
  }

  /**
   * The implementation is different from the way {@link #queryAll(Optional, Optional)} works.
   * <p>
   * This method will do point query to search the nearest lower and upper bound record, then visit the leaf
   * page, go through all the level 0 pages by the double-linked pages.
   * While {@link #queryAll(Optional, Optional)} traverses b+ tree in a depth-first way.
   *
   * @param tableDef         table definition
   * @param lower            lower bound
   * @param lowerOperator    comparison operator for lower
   * @param upper            upper bound
   * @param upperOperator    comparison operator for upper
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   * @param ascOrder         if set result records in ascending order
   * @return record iterator
   */
  private Iterator<GenericRecord> getRangeQueryIterator(final TableDef tableDef, final long rootPageNumber,
                                                        List<Object> lower, ComparisonOperator lowerOperator,
                                                        List<Object> upper, ComparisonOperator upperOperator,
                                                        Optional<List<String>> recordProjection,
                                                        final boolean ascOrder) {
    checkKey(lower, lowerOperator, upper, upperOperator);

    final boolean isNoPk = tableDef.isNoPrimaryKey();
    final int keyColumnNum = tableDef.getPrimaryKeyColumnNum();
    if (!isNoPk) {
      if (isEmpty(lower)) {
        lower = constructMinRecord(keyColumnNum);
        lowerOperator = GTE;
      } else if (lower.size() < keyColumnNum) {
        lower = expandRecord(lower, lowerOperator, keyColumnNum);
      }
      if (isEmpty(upper)) {
        upper = constructMaxRecord(tableDef.getPrimaryKeyColumnNum());
        upperOperator = LTE;
      } else if (upper.size() < keyColumnNum) {
        upper = expandRecord(upper, upperOperator, keyColumnNum);
      }
    }

    if (noneEmpty(lower, upper)) {
      if (keyComparator.compare(lower, upper, tableDef.getPrimaryKeyColumns()) > 0) {
        return new RecordIterator(ImmutableList.of());
      }
    }

    final BitSet projection = transformProjection(recordProjection);
    final List<Object> finalLower = lower;
    final List<Object> finalUpper = upper;
    final ComparisonOperator finalLowerOperator = lowerOperator;
    final ComparisonOperator finalUpperOperator = upperOperator;

    // load first page lazily
    return new RecordIterator() {
      @Override
      public void init() {
        // for initialization, we only need to search by pk projection
        BitSet pkProjection = tableDef.createBitmapWithPkIncluded();
        this.asc = ascOrder;
        if (!isNoPk) {
          Pair<Long, Long> startAndEndPageNumber = queryStartAndEndPageNumber(tableDef, rootPageNumber,
              finalLower, finalLowerOperator, finalUpper, finalUpperOperator, pkProjection);
          // read from start page
          if (asc) {
            currPageNumber = startAndEndPageNumber.getFirst();
            endPageNumber = startAndEndPageNumber.getSecond();
          } else {
            currPageNumber = startAndEndPageNumber.getSecond();
            endPageNumber = startAndEndPageNumber.getFirst();
          }
          indexPage = loadIndexPage(currPageNumber);
          curr = queryWithinIndexPage(tableDef, indexPage, true,
              finalLower, finalLowerOperator, finalUpper, finalUpperOperator, projection);
        } else {
          if (asc) {
            currPageNumber = queryStartPage(tableDef, rootPageNumber, pkProjection);
            endPageNumber = queryEndPage(tableDef, rootPageNumber, pkProjection);
          } else {
            currPageNumber = queryEndPage(tableDef, rootPageNumber, pkProjection);
            endPageNumber = queryStartPage(tableDef, rootPageNumber, pkProjection);
          }
          indexPage = loadIndexPage(currPageNumber);
          curr = queryWithinIndexPage(tableDef, indexPage, projection);
        }
        if (!asc) {
          Collections.reverse(curr);
        }

        if (log.isDebugEnabled()) {
          log.debug("RangeQuery, start page {} records, {}", curr.size(), indexPage.getIndexHeader());
        }
        initialized = true;
      }

      @Override
      public boolean doHasNext() {
        if (currIndex == curr.size()) {
          if (currPageNumber != endPageNumber) {
            if (asc) {
              currPageNumber = indexPage.getInnerPage().getFilHeader().getNextPage();
            } else {
              currPageNumber = indexPage.getInnerPage().getFilHeader().getPrevPage();
            }
            Index nextIndexPage = loadIndexPage(currPageNumber);
            if (log.isDebugEnabled()) {
              log.debug("RangeQuery, load page {} records, {}", nextIndexPage.getIndexHeader());
            }
            this.indexPage = nextIndexPage;
            if (currPageNumber != endPageNumber || isNoPk) {
              this.curr = queryWithinIndexPage(tableDef, nextIndexPage, projection);
            } else {
              this.curr = queryWithinIndexPage(tableDef, nextIndexPage, true,
                  finalLower, finalLowerOperator, finalUpper, finalUpperOperator, projection);
            }
            if (!asc) {
              Collections.reverse(curr);
            }
            this.currIndex = 0;
            return true;
          } else {
            return false;
          }
        }
        return true;
      }
    };
  }

  private Pair<Long, Long> queryStartAndEndPageNumber(final TableDef tableDef, final long rootPageNumber,
                                                      List<Object> lower, ComparisonOperator lowerOperator,
                                                      List<Object> upper, ComparisonOperator upperOperator,
                                                      BitSet projection) {
    checkKey(lower, lowerOperator, upper, upperOperator);
    Index index = loadIndexPage(rootPageNumber);
    GenericRecord startRecord = binarySearchByDirectory(tableDef, rootPageNumber, index, lower, projection);
    GenericRecord endRecord = binarySearchByDirectory(tableDef, rootPageNumber, index, upper, projection);
    if (log.isDebugEnabled()) {
      log.debug("RangeQuery, start record(inc) is {}, end record(exc) is {}", startRecord, endRecord);
    }
    return Pair.of(startRecord.getPageNumber(), endRecord.getPageNumber());
  }

  /**
   * Query B+ tree left-most page number
   *
   * @param pageNumber root page number
   * @param projection projetion
   * @return page number
   */
  private long queryStartPage(TableDef tableDef, long pageNumber, BitSet projection) {
    Index index = loadIndexPage(pageNumber);
    SliceInput sliceInput = index.getSliceInput();
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    sliceInput.setPosition(nextRecPos);

    if (nextRecPos != supremum.getPrimaryKeyPosition()) {
      GenericRecord record = readRecord(tableDef, index.getPageNumber(), sliceInput,
          index.isLeafPage(), projection);
      if (record.isLeafRecord()) {
        return record.getPageNumber();
      } else {
        return queryStartPage(tableDef, record.getChildPageNumber(), projection);
      }
    }
    return pageNumber;
  }

  /**
   * Query B+ tree right-most page number
   *
   * @param tableDef   table definition
   * @param pageNumber root page number
   * @param projection projetion
   * @return page number
   */
  private long queryEndPage(TableDef tableDef, long pageNumber, BitSet projection) {
    Index index = loadIndexPage(pageNumber);
    SliceInput sliceInput = index.getSliceInput();
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    sliceInput.setPosition(nextRecPos);

    GenericRecord lastRecord = null;
    while (nextRecPos != supremum.getPrimaryKeyPosition()) {
      lastRecord = readRecord(tableDef, index.getPageNumber(), sliceInput,
          index.isLeafPage(), projection);
      nextRecPos = lastRecord.nextRecordPosition();
    }

    if (lastRecord == null) {
      return pageNumber;
    }

    if (lastRecord.isLeafRecord()) {
      return lastRecord.getPageNumber();
    }
    return queryEndPage(tableDef, lastRecord.getChildPageNumber(), projection);
  }

  /**
   * Range query records by primary key in a tablespace.
   * <p>
   * For single key the lower or upper list size should be one, for composite key the size
   * will be more than one.
   * <p>
   * Leverage {@link #getRangeQueryIterator(List, ComparisonOperator, List, ComparisonOperator, Optional, boolean)}
   * if range is specified, or else fallback to {@link #queryAll(Optional, Optional)}.
   *
   * @param lower            if rangeQuery is true, then this is the lower bound
   * @param lowerOperator    if rangeQuery is true, then this is the comparison operator for lower
   * @param upper            if rangeQuery is true, then this is the upper bound
   * @param upperOperator    if rangeQuery is true, then this is the comparison operator for upper
   * @param recordPredicate  optional. evaluating record, if true then it will be added to
   *                         result set, else skip it
   * @param recordProjection optional projection of selected column names, if no present, all
   *                         fields will be included
   */
  @Override
  public List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lower, ComparisonOperator lowerOperator,
                                                    List<Object> upper, ComparisonOperator upperOperator,
                                                    Optional<Predicate<GenericRecord>> recordPredicate,
                                                    Optional<List<String>> recordProjection) {
    checkKey(lower, lowerOperator, upper, upperOperator);

    // disable the shortcut to query all because queryAll is not memory friendly
    // if (allEmpty(lower, upper)) {
    //  return queryAll(recordPredicate, recordProjection);
    // }

    Iterator<GenericRecord> iterator = getRangeQueryIterator(lower, lowerOperator,
        upper, upperOperator, recordProjection, true);
    List<GenericRecord> recordList = new ArrayList<>();
    if (recordPredicate != null && recordPredicate.isPresent()) {
      Predicate<GenericRecord> predicate = recordPredicate.get();
      // duplicate some code to avoid break branch prediction
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        if (predicate.test(record)) {
          recordList.add(record);
        }
      }
    } else {
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        recordList.add(record);
      }
    }
    return recordList;
  }

  /**
   * Traverse b+ tree from root page recursively in depth-first way.
   *
   * @param tableDef        table definition
   * @param pageNumber      page number
   * @param recordList      where record will be add to
   * @param recordPredicate optional filtering record, if true then it will be added to result set,
   *                        else skip it
   * @param projection      projection of selected column ordinal in bitmap
   */
  private void traverseBPlusTree(TableDef tableDef, long pageNumber, List<GenericRecord> recordList,
                                 Optional<Predicate<GenericRecord>> recordPredicate,
                                 BitSet projection) {
    Index index = loadIndexPage(pageNumber);
    SliceInput sliceInput = index.getSliceInput();

    if (log.isTraceEnabled()) {
      log.trace("{}", index.getIndexHeader());
    }
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    int recCounter = 0;
    sliceInput.setPosition(nextRecPos);

    if (isOptionalPresent(recordPredicate)) {
      // duplicate some code to avoid break branch prediction
      Predicate<GenericRecord> predicate = recordPredicate.get();
      while (nextRecPos != supremum.getPrimaryKeyPosition()) {
        GenericRecord record = readRecord(tableDef, index.getPageNumber(), sliceInput,
            index.isLeafPage(), projection);
        if (record.isLeafRecord()) {
          if (predicate.test(record)) {
            recordList.add(record);
          }
        } else {
          traverseBPlusTree(tableDef, record.getChildPageNumber(), recordList, recordPredicate, projection);
        }
        nextRecPos = record.nextRecordPosition();
        recCounter++;
      }
    } else {
      while (nextRecPos != supremum.getPrimaryKeyPosition()) {
        GenericRecord record = readRecord(tableDef, index.getPageNumber(), sliceInput,
            index.isLeafPage(), projection);
        if (record.isLeafRecord()) {
          recordList.add(record);
        } else {
          traverseBPlusTree(tableDef, record.getChildPageNumber(), recordList, recordPredicate, projection);
        }
        nextRecPos = record.nextRecordPosition();
        recCounter++;
      }
    }

    // double-check
    if (recCounter != index.getIndexHeader().getNumOfRecs()) {
      log.error("Records read and numOfRecs in index header not match!");
    }
  }

  /**
   * Linear search a record in one page.
   * <p>
   * Algorithm looks like <code>page_cur_search_with_match</code> function
   * in <code>page0cur.cc</code>.
   *
   * @param tableDef   table definition
   * @param pageNumber page number
   * @param index      page index
   * @param position   record starting position, usually it is the primary
   *                   key position
   * @param targetKey  search target key
   * @param projection projection of selected column ordinal in bitmap
   * @return GenericRecord if found, or else DumbGenericRecord representing a closest record
   */
  private GenericRecord linearSearch(TableDef tableDef, long pageNumber,
                                     Index index, int position, List<Object> targetKey,
                                     BitSet projection) {
    SliceInput sliceInput = index.getSliceInput();
    sliceInput.setPosition(position);
    GenericRecord record = readRecord(tableDef, index.getPageNumber(), sliceInput, index.isLeafPage(), projection);
    checkNotNull(record, "Record should not be null");
    log.debug("LinearSearch: page={}, level={}, key={}, header={}",
        pageNumber, index.getIndexHeader().getPageLevel(), record.getPrimaryKey(), record.getHeader());
    GenericRecord preRecord = record;
    boolean isLeafPage = index.isLeafPage();
    while (!record.equals(index.getSupremum())) {
      int compare = keyComparator.compare(record.getPrimaryKey(), targetKey, tableDef.getPrimaryKeyColumns());
      // if compare < 0 then continue to check next
      if (compare > 0) {
        if (isLeafPage) {
          return new DumbGenericRecord(record);
        } else {
          // corner case,对于比smallest还小的需要判断infimum
          long childPageNumber = preRecord.getChildPageNumber();
          if (preRecord.getHeader().getRecordType() == RecordType.INFIMUM) {
            childPageNumber = record.getChildPageNumber();
          }
          return binarySearchByDirectory(tableDef, childPageNumber,
              loadIndexPage(childPageNumber), targetKey, projection);
        }
      } else if (compare == 0) {
        if (isLeafPage) {
          return record;
        } else {
          long childPageNumber = Utils.cast(record.getChildPageNumber());
          return binarySearchByDirectory(tableDef, childPageNumber,
              loadIndexPage(childPageNumber), targetKey, projection);
        }
      }

      sliceInput.setPosition(record.nextRecordPosition());
      GenericRecord nextRecord = readRecord(tableDef, index.getPageNumber(),
          sliceInput, index.isLeafPage(), projection);
      preRecord = record;
      record = nextRecord;
    }
    if (isLeafPage) {
      return new DumbGenericRecord(record);
    } else {
      long childPageNumber = Utils.cast(preRecord.getChildPageNumber());
      return binarySearchByDirectory(tableDef, childPageNumber,
          loadIndexPage(childPageNumber), targetKey, projection);
    }
  }

  /**
   * Search from directory slots in binary search way, and then call
   * {@link #linearSearch(TableDef, long, Index, int, List, BitSet)}
   *
   * to search the specific record.
   *
   * @param tableDef   table definition
   * @param pageNumber page number
   * @param index      index page
   * @param targetKey  search target key
   * @param projection projection of selected column ordinal in bitmap
   * @return GenericRecord
   * @see <a href="https://leetcode-cn.com/problems/search-insert-position">search-insert-position
   * on leetcode</a>
   */
  private GenericRecord binarySearchByDirectory(TableDef tableDef, long pageNumber, Index index,
                                                List<Object> targetKey, BitSet projection) {
    checkNotNull(index);
    checkNotNull(targetKey);
    int[] dirSlots = index.getDirSlots();
    SliceInput sliceInput = index.getSliceInput();
    if (log.isTraceEnabled()) {
      log.trace("DirSlots is {}", Arrays.toString(dirSlots));
    }

    int start = 0;
    int end = dirSlots.length - 1;
    GenericRecord record;
    while (start <= end) {
      int mid = (start + end) / 2;
      int recPos = dirSlots[mid];
      sliceInput.setPosition(recPos);
      record = readRecord(tableDef, index.getPageNumber(), sliceInput, index.isLeafPage(), projection);
      checkNotNull(record, "record should not be null");
      if (log.isTraceEnabled()) {
        log.trace("SearchByDir: page={}, level={}, recordKey={}, targetKey={}, dirSlotSize={}, "
                + "start={}, end={}, mid={}",
            pageNumber, index.getIndexHeader().getPageLevel(), record.getPrimaryKey(),
            targetKey, dirSlots.length, start, end, mid);
      }

      List<Object> midVal = record.getPrimaryKey();
      int compare = keyComparator.compare(midVal, targetKey, tableDef.getPrimaryKeyColumns());
      if (compare > 0) {
        end = mid - 1;
      } else if (compare < 0) {
        start = mid + 1;
      } else {
        return linearSearch(tableDef, pageNumber, index, recPos, targetKey, projection);
      }
    }
    log.debug("SearchByDir, start={}", start);
    return linearSearch(tableDef, pageNumber, index, dirSlots[start - 1], targetKey, projection);
  }

  @Override
  public Index loadIndexPage(long pageNumber) {
    InnerPage page = storageService.loadPage(pageNumber);
    int sdiPageNum = 0;
    while (sdiPageNum++ < ROOT_PAGE_NUMBER + 1
        && page.pageType() != null
        && PageType.SDI.equals(page.pageType())) {
      log.debug("Skip SDI (Serialized Dictionary Information) page "
          + page.getPageNumber() + " since version is >= Mysql8");
      page = storageService.loadPage(++pageNumber);
    }
    Index index = new Index(page, tableDef);
    if (log.isDebugEnabled()) {
      checkState(page.getFilHeader().getPageType() == PageType.INDEX,
          "Page " + pageNumber + " is not index page, actual page type is "
              + page.getFilHeader().getPageType());
      log.debug("Load {} page {}, {} records", index.isLeafPage()
          ? "leaf" : "non-leaf", pageNumber, index.getIndexHeader().getNumOfRecs());
    }
    return index;
  }

  private Blob loadBlobPage(final long pageNumber, long offset) {
    InnerPage page = storageService.loadPage(pageNumber);
    if (page.pageType() != null && PageType.LOB_FIRST.equals(page.pageType())) {
      // TODO support mysql8.0 lob page
      if (ENABLE_THROW_EXCEPTION_FOR_UNSUPPORTED_MYSQL80_LOB.value()) {
        throw new IllegalStateException("New format of LOB page type not supported currently");
      } else {
        return null;
      }
    }
    checkState(page.getFilHeader().getPageType() == PageType.BLOB,
        "Page " + pageNumber + " is not blob page, actual page type is "
            + page.getFilHeader().getPageType());
    Blob blob = new Blob(page, offset);
    if (log.isDebugEnabled()) {
      log.debug("Load page {}, {}", pageNumber, blob);
    }
    return blob;
  }

  /**
   * Read fields from one row and construct them into a record.
   *
   * @param tableDef   table definition
   * @param pageNumber page number
   * @param bodyInput  bytes input
   * @param isLeafPage is B+ tree leaf page
   * @param projection projection of selected column ordinal in bitmap
   * @return record
   */
  private GenericRecord readRecord(TableDef tableDef, long pageNumber, SliceInput bodyInput,
                                   boolean isLeafPage, BitSet projection) {
    int primaryKeyPos = bodyInput.position();

    bodyInput.decrPosition(SIZE_OF_REC_HEADER);
    RecordHeader header = RecordHeader.fromSlice(bodyInput);
    bodyInput.decrPosition(SIZE_OF_REC_HEADER);

    if (header.getRecordType() == RecordType.INFIMUM || header.getRecordType() == RecordType.SUPREMUM) {
      GenericRecord mum = new GenericRecord(header, tableDef, pageNumber);
      mum.setPrimaryKeyPosition(primaryKeyPos);
      return mum;
    }

    // nullByteSize is an array indicating which fields(nullable) are null.
    // only works on leaf pages because non-leaf page does not allow nulls.
    List<String> nullColumnNames = null;
    int nullableColumnNum = tableDef.getNullableColumnNum();
    int nullByteSize = (nullableColumnNum + 7) / 8;
    if (isLeafPage && tableDef.containsNullColumn()) {
      int[] nullBitmap = Utils.getBitArray(bodyInput, nullableColumnNum);
      nullColumnNames = Utils.getFromBitArray(tableDef.getNullableColumnList(), nullBitmap, Column::getName);
    }

    // For each non-NULL variable-length field, the record header contains the length
    // in one or two bytes.
    List<Integer> varLenArray = null;
    List<Boolean> overflowPageArray = null;
    if (tableDef.containsVariableLengthColumn()) {
      int varColNum;
      List<Column> valCols;
      if (isLeafPage || tableDef.isDerivedFromSk()) {
        varColNum = tableDef.getVariableLengthColumnNum();
        valCols = tableDef.getVariableLengthColumnList();
      } else {
        // for non-leaf page, only pk columns are included
        varColNum = tableDef.getPrimaryKeyVarLenColumnNum();
        valCols = tableDef.getPrimaryKeyVarLenColumns();
      }
      // array list creation with maximum capacity
      varLenArray = varColNum == 0 ? Collections.emptyList() : new ArrayList<>(varColNum);
      overflowPageArray = varColNum == 0 ? Collections.emptyList() : new ArrayList<>(varColNum);
      bodyInput.decrPosition(nullByteSize);
      for (int i = 0; i < varColNum; i++) {
        Column varColumn = valCols.get(i);
        if (nullColumnNames != null && nullColumnNames.contains(varColumn.getName())) {
          continue;
        }
        bodyInput.decrPosition(1);
        int len = bodyInput.readUnsignedByte();
        boolean overflowPageFlag = false;
        if (isTwoBytesLen(varColumn, len)) {
          bodyInput.decrPosition(2);
          // This means there is off-page
          overflowPageFlag = ((0x40 & len) != 0);
          len = ((len & 0x3f) << 8) + bodyInput.readUnsignedByte();
          bodyInput.decrPosition(1);
        } else {
          bodyInput.decrPosition(1);
        }
        varLenArray.add(len);
        overflowPageArray.add(overflowPageFlag);
      }
    }

    // read primary key
    bodyInput.setPosition(primaryKeyPos);
    GenericRecord record = new GenericRecord(header, tableDef, pageNumber);
    int varLenIdx = 0;
    if (tableDef.getPrimaryKeyColumnNum() > 0) {
      // set primary key, single key or composite key
      for (Column pkColumn : tableDef.getPrimaryKeyColumns()) {
        // columns that is not in projection bitmap will be included as well
        // because in we rely on pk to compare when querying by pk and
        // range querying to locating record in head and tail pages.
        if (columnValueIsNull(nullColumnNames, pkColumn)) {
          record.put(pkColumn.getName(), null);
        } else {
          putColumnValueToRecord(bodyInput, varLenArray, overflowPageArray,
              record, varLenIdx, pkColumn);
          if (pkColumn.isVariableLength()) {
            varLenIdx++;
          }
        }
      }
    } else {
      // default 6 bytes ROW ID if no primary key is defined
      bodyInput.readByteArray(6);
    }
    if (log.isTraceEnabled()) {
      log.trace("Read record, pkPos={}, key={}, recordHeader={}, nullColumnNames={}, varLenArray={}, overflow={}",
          primaryKeyPos, Arrays.toString(record.getValues()), header, nullColumnNames,
          varLenArray, overflowPageArray);
    }
    record.setPrimaryKeyPosition(primaryKeyPos);

    // read all other columns
    if (isLeafPage) {
      if (!tableDef.isDerivedFromSk()) {
        // skip 13 bytes, 6-byte transaction ID field and a 7-byte roll pointer field.
        bodyInput.skipBytes(13);
      }

      for (Column column : tableDef.getColumnList()) {
        // skip primary key since it is set
        if (tableDef.isColumnPrimaryKey(column)) {
          continue;
        }

        // skip column that is not in projection bitmap
        if (projection != NOP_PROJECTION && !projection.get(column.getOrdinal())) {
          if (!columnValueIsNull(nullColumnNames, column)) {
            skipColumn(bodyInput, varLenArray, overflowPageArray,
                record, varLenIdx, column);
            if (column.isVariableLength()) {
              varLenIdx++;
            }
          }
          // jump to next field
          continue;
        }

        if (columnValueIsNull(nullColumnNames, column)) {
          record.put(column.getName(), null);
        } else {
          putColumnValueToRecord(bodyInput, varLenArray, overflowPageArray,
              record, varLenIdx, column);
          if (column.isVariableLength()) {
            varLenIdx++;
          }
        }
      }
    } else {
      if (tableDef.isDerivedFromSk()) {
        for (Column column : tableDef.getColumnList()) {
          // skip primary key since it is set
          if (tableDef.isColumnPrimaryKey(column)) {
            continue;
          }
          if (columnValueIsNull(nullColumnNames, column)) {
            record.put(column.getName(), null);
          } else {
            putColumnValueToRecord(bodyInput, varLenArray, overflowPageArray,
                record, varLenIdx, column);
            if (column.isVariableLength()) {
              varLenIdx++;
            }
          }
        }
      }
      long childPageNumber = bodyInput.readUnsignedInt();
      if (log.isTraceEnabled()) {
        log.trace("Read record, pkPos={}, key={}, childPage={}", primaryKeyPos,
            Arrays.toString(record.getValues()), childPageNumber);
      }
      record.setChildPageNumber(childPageNumber);
    }

    // set to next record position
    checkElementIndex(record.nextRecordPosition(), SIZE_OF_BODY, "Next record position is out of bound");
    bodyInput.setPosition(record.nextRecordPosition());

    return record;
  }

  /**
   * @see {@link #putColumnValueToRecord(SliceInput, List, List, GenericRecord, int, Column)}
   */
  private void skipColumn(SliceInput bodyInput, List<Integer> varLenArray, List<Boolean> overflowPageArray,
                          GenericRecord record, int varLenIdx, Column column) {
    if (column.isVariableLength()) {
      checkState(varLenArray != null && overflowPageArray != null);
      checkElementIndex(varLenIdx, varLenArray.size());
      if (!overflowPageArray.get(varLenIdx)) {
        ColumnFactory.getColumnParser(column.getType())
            .skipFrom(bodyInput, varLenArray.get(varLenIdx), column.getJavaCharset());
      } else {
        skipOverflowPage(bodyInput, record, column, varLenArray.get(varLenIdx));
      }
    } else if (column.isFixedLength()) {
      ColumnFactory.getColumnParser(column.getType())
          .skipFrom(bodyInput, column.getLength(), column.getJavaCharset());
    } else {
      ColumnFactory.getColumnParser(column.getType()).skipFrom(bodyInput, column);
    }
  }

  private void putColumnValueToRecord(SliceInput bodyInput, List<Integer> varLenArray, List<Boolean> overflowPageArray,
                                      GenericRecord record, int varLenIdx, Column column) {
    if (column.isVariableLength()) {
      checkState(varLenArray != null && overflowPageArray != null);
      checkElementIndex(varLenIdx, varLenArray.size());
      // https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html
      // Tables that use the COMPACT row format store the first 768 bytes of variable-length
      // column values (VARCHAR, VARBINARY, and BLOB and TEXT types) in the index record
      // within the B-tree node, with the remainder stored on overflow pages.
      // When a table is created with ROW_FORMAT=DYNAMIC, InnoDB can store long variable-length
      // column values (for VARCHAR, VARBINARY, and BLOB and TEXT types) fully off-page, with
      // the clustered index record containing only a 20-byte pointer to the overflow page.
      // if (varLenArray[varLenIdx] <= 768) {
      if (!overflowPageArray.get(varLenIdx)) {
        Object val = ColumnFactory.getColumnParser(column.getType())
            .readFrom(bodyInput, varLenArray.get(varLenIdx), column.getJavaCharset());
        record.put(column.getName(), val);
      } else {
        handleOverflowPage(bodyInput, record, column, varLenArray.get(varLenIdx));
      }
    } else if (column.isFixedLength()) {
      Object val = ColumnFactory.getColumnParser(column.getType())
          .readFrom(bodyInput, column.getLength(), column.getJavaCharset());
      record.put(column.getName(), val);
    } else {
      Object val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput, column);
      record.put(column.getName(), val);
    }
  }

  private boolean columnValueIsNull(List<String> nullColumnNames, Column column) {
    return column.isNullable() && nullColumnNames != null && nullColumnNames.contains(column.getName());
  }

  /**
   * If the var-len needs 2 bytes
   * <p>
   * see rem0rec.cc
   * <p>
   * https://docs.oracle.com/cd/E17952_01/mysql-8.0-en/innodb-row-format.html
   * <p>
   * For each non-NULL variable-length field, the record header contains the length of
   * the column in one or two bytes. Two bytes are only needed if part of the column is
   * stored externally in overflow pages or the maximum length exceeds 255 bytes and the
   * actual length exceeds 127 bytes. For an externally stored column, the 2-byte length
   * indicates the length of the internally stored part plus the 20-byte pointer to the
   * externally stored part. The internal part is 768 bytes, so the length is 768+20.
   * The 20-byte pointer stores the true length of the column.
   * <p>
   * Add by author. Note that charset will be considered when calculating max length of
   * var-len field.
   *
   * @param varColumn column
   * @param len       first bytes read converted to unsigned int
   * @return if need to read more bytes
   */
  private boolean isTwoBytesLen(Column varColumn, int len) {
    int factor = 1;
    if (CHAR_TYPES.contains(varColumn.getType())) {
      factor = tableDef.getMaxBytesPerChar();
    }
    return len > 127
        && (BLOB_TEXT_TYPES.contains(varColumn.getType()) || (varColumn.getLength() * factor) > 255);
  }

  private void handleOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    if (BLOB_TYPES.contains(column.getType())
        || VARBINARY.equals(column.getType())) {
      handleBlobOverflowPage(bodyInput, record, column, varLen);
    } else if (TEXT_TYPES.contains(column.getType())
        || VARCHAR.equals(column.getType())
        || CHAR.equals(column.getType())) {
      handleCharacterOverflowPage(bodyInput, record, column, varLen);
    } else {
      throw new UnsupportedOperationException("Handle overflow page unsupported for type " + column.getType());
    }
  }

  private void handleCharacterOverflowPage(SliceInput bodyInput, GenericRecord record,
                                           Column column, int varLen) {
    ByteBuffer buffer = readOverflowPageByteBuffer(bodyInput, record, column, varLen);
    try {
      record.put(column.getName(), new String(buffer.array(), column.getJavaCharset()));
    } catch (UnsupportedEncodingException e) {
      throw new ReaderException(e);
    }
  }

  private void handleBlobOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    ByteBuffer buffer = readOverflowPageByteBuffer(bodyInput, record, column, varLen);
    record.put(column.getName(), buffer.array());
  }

  private ByteBuffer readOverflowPageByteBuffer(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    int varLenWithoutOffPagePointer = varLen - 20;
    Object val = null;
    if (varLenWithoutOffPagePointer > 0) {
      val = bodyInput.readByteArray(768);
    }
    OverflowPagePointer overflowPagePointer = OverflowPagePointer.fromSlice(bodyInput);
    ByteBuffer buffer = ByteBuffer.allocate(varLenWithoutOffPagePointer + (int) overflowPagePointer.getLength());
    if (val != null) {
      buffer.put((byte[]) val);
    }
    Blob blob;
    long nextPageNumber = overflowPagePointer.getPageNumber();
    do {
      blob = loadBlobPage(nextPageNumber, overflowPagePointer.getPageOffset());
      // When blob cannot be handled quite
      if (blob == null) {
        break;
      }
      byte[] content = blob.read();
      buffer.put(content);
      if (blob.hasNext()) {
        nextPageNumber = blob.getNextPageNumber();
      }
      log.trace("Read overflow page {}, content length={}, is end? = {}",
          overflowPagePointer, content.length, !blob.hasNext());
    } while (blob.hasNext());
    return buffer;
  }

  private void skipOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    if (BLOB_TYPES.contains(column.getType())
        || VARBINARY.equals(column.getType())
        || TEXT_TYPES.contains(column.getType())
        || VARCHAR.equals(column.getType())
        || CHAR.equals(column.getType())) {
      int varLenWithoutOffPagePointer = varLen - 20;
      if (varLenWithoutOffPagePointer > 0) {
        bodyInput.skipBytes(768);
      }
      OverflowPagePointer.fromSlice(bodyInput);
    } else {
      throw new UnsupportedOperationException("Handle overflow page unsupported for type " + column.getType());
    }
  }

  private BitSet transformProjection(Optional<List<String>> projection) {
    return isOptionalPresent(projection)
        ? transformProjection(projection.get()) : NOP_PROJECTION;
  }

  /**
   * Convert projection list with column names to bitmap with column ordinal.
   *
   * @param projection projection list with column names
   * @return projection bitmap
   */
  private BitSet transformProjection(List<String> projection) {
    checkArgument(CollectionUtils.isNotEmpty(projection), "Projection list should not be empty");
    BitSet result = tableDef.createBitmapWithPkIncluded();
    for (String p : projection) {
      TableDef.Field field = tableDef.getField(p);
      if (field == null) {
        throw new ReaderException("Column " + p + " not found in TableDef");
      }
      result.set(field.getOrdinal());
    }
    return result;
  }

  private void checkKey(List<Object> lower, ComparisonOperator lowerOperator,
                        List<Object> upper, ComparisonOperator upperOperator) {
    checkArgument(lower != null, "lower should not be null");
    checkArgument(upper != null, "upper should not be null");
    checkArgument(lowerOperator != null, "lowerOperator is null");
    checkArgument(upperOperator != null, "upperOperator is null");
    checkArgument(!anyElementEmpty(lower), "lower should not contain null elements");
    checkArgument(!anyElementEmpty(upper), "upper should not contain null elements");
  }

  private boolean qualified(List<Column> keyColumnList, List<Object> primaryKey,
                            List<Object> lower, ComparisonOperator lowerOperator,
                            List<Object> upper, ComparisonOperator upperOperator) {
    if (lowerOperator == GT && upperOperator == LT) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) > 0
          && keyComparator.compare(primaryKey, upper, keyColumnList) < 0;
    } else if (lowerOperator == GT && upperOperator == LTE) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) > 0
          && keyComparator.compare(primaryKey, upper, keyColumnList) <= 0;
    } else if (lowerOperator == GTE && upperOperator == LT) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) >= 0
          && keyComparator.compare(primaryKey, upper, keyColumnList) < 0;
    } else if (lowerOperator == GTE && upperOperator == LTE) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) >= 0
          && keyComparator.compare(primaryKey, upper, keyColumnList) <= 0;
    }
    throw new ReaderException("Operator is invalid, lower should be >= or >, upper should be "
        + "<= or <, actual lower " + lowerOperator + ", upper " + upperOperator);
  }

  private boolean lowerQualified(List<Column> keyColumnList, List<Object> primaryKey,
                                 List<Object> lower, ComparisonOperator lowerOperator) {
    if (lowerOperator == GT) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) > 0;
    } else if (lowerOperator == GTE) {
      return keyComparator.compare(primaryKey, lower, keyColumnList) >= 0;
    }
    throw new ReaderException("Operator is invalid, lower should be >= or >, upper should be "
        + "<= or <, actual lower " + lowerOperator);
  }

  private boolean upperQualified(List<Column> keyColumnList, List<Object> primaryKey,
                                 List<Object> upper, ComparisonOperator upperOperator) {
    if (upperOperator == LT) {
      return keyComparator.compare(primaryKey, upper, keyColumnList) < 0;
    } else if (upperOperator == LTE) {
      return keyComparator.compare(primaryKey, upper, keyColumnList) <= 0;
    }
    throw new ReaderException("Operator is invalid, lower should be >= or >, upper should be "
        + "<= or <, actual upper " + upperOperator);
  }

}
