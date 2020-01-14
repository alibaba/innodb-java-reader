/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.google.common.collect.Lists;

import com.alibaba.innodb.java.reader.Constants;
import com.alibaba.innodb.java.reader.column.ColumnFactory;
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
import com.alibaba.innodb.java.reader.schema.Schema;
import com.alibaba.innodb.java.reader.service.IndexService;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.SliceInput;
import com.alibaba.innodb.java.reader.util.Utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_BODY;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_REC_HEADER;
import static com.alibaba.innodb.java.reader.column.ColumnType.BLOB_TEXT_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.BLOB_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR;
import static com.alibaba.innodb.java.reader.column.ColumnType.CHAR_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.TEXT_TYPES;
import static com.alibaba.innodb.java.reader.column.ColumnType.VARBINARY;
import static com.alibaba.innodb.java.reader.column.ColumnType.VARCHAR;
import static com.alibaba.innodb.java.reader.util.Utils.MAX;
import static com.alibaba.innodb.java.reader.util.Utils.MIN;
import static com.alibaba.innodb.java.reader.util.Utils.castCompare;
import static com.alibaba.innodb.java.reader.util.Utils.tryCastString;

/**
 * Index service implementation
 * <p>
 * Only work for clustered index currently.
 *
 * @author xu.zx
 */
@Slf4j
public class IndexServiceImpl implements IndexService, Constants {

  private Schema schema;

  private StorageService storageService;

  public IndexServiceImpl(StorageService storageService, Schema schema) {
    this.storageService = storageService;
    this.schema = schema;
  }

  @Override
  public List<GenericRecord> queryByPageNumber(int pageNumber) {
    return queryByPageNumber((long) pageNumber);
  }

  @Override
  public List<GenericRecord> queryByPageNumber(long pageNumber) {
    return queryByIndexPage(loadIndexPage(pageNumber));
  }

  private List<GenericRecord> queryByIndexPage(Index index) {
    return queryByIndexPage(index, false, null, null);
  }

  /**
   * query within one index page
   *
   * @param index        index page
   * @param rangeQuery   if the query is range enabled
   * @param minInclusive if rangeQuery is true, then this is the lower bound
   * @param maxExclusive if rangeQuery is true, then this is the upper bound
   * @return list of records
   */
  private List<GenericRecord> queryByIndexPage(Index index, boolean rangeQuery, Object minInclusive, Object maxExclusive) {
    // num of heap records - system records
    List<GenericRecord> result = new ArrayList<>(index.getIndexHeader().getNumOfRecs());
    SliceInput sliceInput = index.getSliceInput();

    log.debug("{}, {}", index.getIndexHeader(), index.getFsegHeader());
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    int recCounter = 0;
    sliceInput.setPosition(nextRecPos);

    while (nextRecPos != supremum.getPrimaryKeyPosition()) {
      GenericRecord record = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
      if (rangeQuery) {
        if (minInclusive != null && maxExclusive != null) {
          if (castCompare(record.getPrimaryKey(), minInclusive) >= 0
              && castCompare(record.getPrimaryKey(), maxExclusive) < 0) {
            result.add(record);
          }
        } else if (minInclusive != null) {
          if (castCompare(record.getPrimaryKey(), minInclusive) >= 0) {
            result.add(record);
          }
        } else if (maxExclusive != null) {
          if (castCompare(record.getPrimaryKey(), maxExclusive) < 0) {
            result.add(record);
            break;
          }
        } else {
          throw new ReaderException("minInclusive and maxExclusive is not set correctly");
        }
      } else {
        result.add(record);
      }
      nextRecPos = record.nextRecordPosition();
      recCounter++;
    }

    // double-check
    if (recCounter != index.getIndexHeader().getNumOfRecs()) {
      log.error("records read and numOfRecs in index header not match!");
    }

    return result;
  }

  @Override
  public GenericRecord queryByPrimaryKey(Object key) {
    key = tryCastString(key, schema.getPrimaryKeyColumn().getType());
    Index index = loadIndexPage(ROOT_PAGE_NUMBER);
    checkState(index.isRootPage(), "root page is wrong which should not happen");
    GenericRecord record = binarySearchByDirectory(ROOT_PAGE_NUMBER, index, key);
    if (record == null || DumbGenericRecord.class.equals(record.getClass())) {
      return null;
    }
    return record;
  }

  @Override
  public List<GenericRecord> queryAll(Optional<Predicate<GenericRecord>> recordPredicate) {
    List<GenericRecord> recordList = new ArrayList<>();
    traverseBPlusTree(ROOT_PAGE_NUMBER, recordList, recordPredicate);
    return recordList;
  }

  /**
   * Leverage {@link #getRangeQueryIterator(Object, Object)}
   */
  @Override
  public Iterator<GenericRecord> getQueryAllIterator() {
    return getRangeQueryIterator(null, null);
  }

  /**
   * The implementation is different from the way {@link #queryAll(Optional)} works.
   * This method will do point query to search the nearest lower and upper bound record, then visit the leaf
   * page, go through all the level 0 pages by the double-linked pages.
   * While {@link #queryAll(Optional)} traverses b+ tree in a depth-first way.
   */
  @Override
  public Iterator<GenericRecord> getRangeQueryIterator(Object lowerInclusiveKey, Object upperExclusiveKey) {
    Object lower = tryCastString(lowerInclusiveKey, schema.getPrimaryKeyColumn().getType());
    Object upper = tryCastString(upperExclusiveKey, schema.getPrimaryKeyColumn().getType());
    if (lower != null && upper != null) {
      if (castCompare(lower, upper) > 0) {
        throw new IllegalArgumentException("lower is greater than upper");
      }
      if (castCompare(lower, upper) == 0) {
        GenericRecord record = queryByPrimaryKey(lower);
        return record == null ? new RecordIterator(Collections.emptyList()) : new RecordIterator(Lists.newArrayList(record));
      }
    }
    if (lower == null) {
      lower = MIN;
    }
    if (upper == null) {
      upper = MAX;
    }
    final Object finalLowerInclusiveKey = lower;
    final Object finalUpperExclusiveKey = upper;

    Index index = loadIndexPage(ROOT_PAGE_NUMBER);
    GenericRecord startRecord = binarySearchByDirectory(ROOT_PAGE_NUMBER, index, lower);
    GenericRecord endRecord = binarySearchByDirectory(ROOT_PAGE_NUMBER, index, upper);
    log.debug("rangeQuery, start record(inc) is {}, end record(exc) is {}", startRecord, endRecord);
    long pageNumber = startRecord.getPageNumber();
    long endPageNumber = endRecord.getPageNumber();

    // read from start page
    Index startIndexPage = loadIndexPage(pageNumber);
    List<GenericRecord> startPageResult = queryByIndexPage(startIndexPage, true, lower, upper);
    log.debug("rangeQuery, start page {} records, {}", startPageResult.size(), startIndexPage.getIndexHeader());

    return new RecordIterator(startIndexPage, endPageNumber, startPageResult) {

      @Override
      public boolean hasNext() {
        if (currIndex == curr.size()) {
          if (currPageNumber != endPageNumber) {
            currPageNumber = indexPage.getInnerPage().getFilHeader().getNextPage();
            Index nextIndexPage = loadIndexPage(currPageNumber);
            if (log.isDebugEnabled()) {
              log.debug("rangeQuery, load page {} records, {}", nextIndexPage.getIndexHeader());
            }
            this.indexPage = nextIndexPage;
            if (currPageNumber != endPageNumber) {
              this.curr = queryByIndexPage(nextIndexPage);
            } else {
              this.curr = queryByIndexPage(nextIndexPage, true, finalLowerInclusiveKey, finalUpperExclusiveKey);
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

  @Override
  public List<GenericRecord> rangeQueryByPrimaryKey(Object lowerInclusiveKey, Object upperExclusiveKey, Optional<Predicate<GenericRecord>> recordPredicate) {
    // quick way to query all
    if (lowerInclusiveKey == null && upperExclusiveKey == null) {
      return queryAll(recordPredicate);
    }

    Iterator<GenericRecord> iterator = getRangeQueryIterator(lowerInclusiveKey, upperExclusiveKey);
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
   * @param pageNumber      page number
   * @param recordList      where record will be add to
   * @param recordPredicate evaluating record, if true then it will be added to result set, else skip it
   */
  public void traverseBPlusTree(long pageNumber, List<GenericRecord> recordList, Optional<Predicate<GenericRecord>> recordPredicate) {
    Index index = loadIndexPage(pageNumber);
    SliceInput sliceInput = index.getSliceInput();

    log.debug("{}", index.getIndexHeader());
    GenericRecord infimum = index.getInfimum();
    GenericRecord supremum = index.getSupremum();
    int nextRecPos = infimum.nextRecordPosition();
    int recCounter = 0;
    sliceInput.setPosition(nextRecPos);

    if (recordPredicate != null && recordPredicate.isPresent()) {
      // duplicate some code to avoid break branch prediction
      Predicate<GenericRecord> predicate = recordPredicate.get();
      while (nextRecPos != supremum.getPrimaryKeyPosition()) {
        GenericRecord record = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
        if (record.isLeafRecord()) {
          if (predicate.test(record)) {
            recordList.add(record);
          }
        } else {
          traverseBPlusTree(record.getChildPageNumber(), recordList, recordPredicate);
        }
        nextRecPos = record.nextRecordPosition();
        recCounter++;
      }
    } else {
      while (nextRecPos != supremum.getPrimaryKeyPosition()) {
        GenericRecord record = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
        if (record.isLeafRecord()) {
          recordList.add(record);
        } else {
          traverseBPlusTree(record.getChildPageNumber(), recordList, recordPredicate);
        }
        nextRecPos = record.nextRecordPosition();
        recCounter++;
      }
    }

    // double-check
    if (recCounter != index.getIndexHeader().getNumOfRecs()) {
      log.error("records read and numOfRecs in index header not match!");
    }
  }

  /**
   * linear search from a record in one page.
   * <p>
   * Algorithm looks like <code>page_cur_search_with_match</code> function in <code>page0page.cc</code>.
   *
   * @param pageNumber page number
   * @param index      page index
   * @param position   record starting position, usually it is the primary key position
   * @param targetKey  search target key
   * @return GenericRecord if found, or else DumbGenericRecord representing a closest record
   */
  private GenericRecord linearSearch(long pageNumber, Index index, int position, Object targetKey) {
    SliceInput sliceInput = index.getSliceInput();
    sliceInput.setPosition(position);
    GenericRecord record = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
    checkNotNull(record, "record should not be null");
    log.debug("linearSearch: page={}, level={}, key={}, header={}",
        pageNumber, index.getIndexHeader().getPageLevel(), record.getPrimaryKey(), record.getHeader());
    GenericRecord preRecord = record;
    boolean isLeafPage = index.isLeafPage();
    while (!record.equals(index.getSupremum())) {
      int compare = castCompare(record.getPrimaryKey(), targetKey);
      // if compare < 0 then continue to check next
      if (compare > 0) {
        if (isLeafPage) {
          return new DumbGenericRecord(record);
        } else {
          // corner case,对于比smallest还小的需要判断infimum
          long childPageNumber = Utils.cast(preRecord.equals(index.getInfimum()) ? record.getChildPageNumber() : preRecord.getChildPageNumber());
          return binarySearchByDirectory(childPageNumber, loadIndexPage(childPageNumber), targetKey);
        }
      } else if (compare == 0) {
        if (isLeafPage) {
          return record;
        } else {
          long childPageNumber = Utils.cast(record.getChildPageNumber());
          return binarySearchByDirectory(childPageNumber, loadIndexPage(childPageNumber), targetKey);
        }
      }

      sliceInput.setPosition(record.nextRecordPosition());
      GenericRecord nextRecord = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
      preRecord = record;
      record = nextRecord;
    }
    if (isLeafPage) {
      return new DumbGenericRecord(record);
    } else {
      long childPageNumber = Utils.cast(preRecord.getChildPageNumber());
      return binarySearchByDirectory(childPageNumber, loadIndexPage(childPageNumber), targetKey);
    }
  }

  /**
   * Search from directory slots in binary search way, and then call {@link #linearSearch(long, Index, int, Object)}
   * to search the specific record.
   *
   * @param pageNumber page number
   * @param index      index page
   * @param targetKey  search target key
   * @return GenericRecord
   * @see https://leetcode-cn.com/problems/search-insert-position
   */
  private GenericRecord binarySearchByDirectory(long pageNumber, Index index, Object targetKey) {
    checkNotNull(index);
    checkNotNull(targetKey);
    int[] dirSlots = index.getDirSlots();
    SliceInput sliceInput = index.getSliceInput();
    if (log.isDebugEnabled()) {
      log.debug("dirSlots is {}", Arrays.toString(dirSlots));
    }

    int start = 0;
    int end = dirSlots.length - 1;
    GenericRecord record = null;
    while (start <= end) {
      int mid = (start + end) / 2;
      int recPos = dirSlots[mid];
      sliceInput.setPosition(recPos);
      record = readRecord(sliceInput, index.isLeafPage(), index.getPageNumber());
      checkNotNull(record, "record should not be null");
      if (log.isDebugEnabled()) {
        log.debug("searchByDir: page={}, level={}, recordKey={}, targetKey={}, dirSlotSize={}, start={}, end={}, mid={}",
            pageNumber, index.getIndexHeader().getPageLevel(), record.getPrimaryKey(), targetKey, dirSlots.length, start, end, mid);
      }

      Object midVal = record.getPrimaryKey();
      int compare = castCompare(midVal, targetKey);
      if (compare > 0) {
        end = mid - 1;
      } else if (compare < 0) {
        start = mid + 1;
      } else {
        return linearSearch(pageNumber, index, recPos, targetKey);
      }
    }
    log.debug("searchByDir, start={}", start);
    return linearSearch(pageNumber, index, dirSlots[start - 1], targetKey);
  }

  private Index loadIndexPage(long pageNumber) {
    InnerPage page = storageService.loadPage(pageNumber);
    int sdiPageNum = 0;
    while (sdiPageNum++ < ROOT_PAGE_NUMBER + 1 && page.pageType() != null && PageType.SDI.equals(page.pageType())) {
      log.debug("Skip SDI page " + page.getPageNumber() + " since this is mysql8");
      page = storageService.loadPage(++pageNumber);
    }
    checkState(page.getFilHeader().getPageType() == PageType.INDEX, "page " + pageNumber + " is not index page but " + page.getFilHeader().getPageType());
    Index index = new Index(page, schema);
    log.debug("load {} page {}, {} records", index.isLeafPage() ? "leaf" : "non-leaf", pageNumber, index.getIndexHeader().getNumOfRecs());
    return index;
  }

  private Blob loadBlobPage(final long pageNumber, long offset) {
    InnerPage page = storageService.loadPage(pageNumber);
    if (page.pageType() != null && PageType.LOB_FIRST.equals(page.pageType())) {
      throw new IllegalStateException("New format of LOB page type not supported currently");
    }
    checkState(page.getFilHeader().getPageType() == PageType.BLOB, "page " + pageNumber + " is not blob page but " + page.getFilHeader().getPageType());
    Blob blob = new Blob(page, offset);
    log.debug("load page {}, {}", pageNumber, blob);
    return blob;
  }

  private GenericRecord readRecord(SliceInput bodyInput, boolean isLeafPage, long pageNumber) {
    int primaryKeyPos = bodyInput.position();

    bodyInput.decrPosition(SIZE_OF_REC_HEADER);
    RecordHeader header = RecordHeader.fromSlice(bodyInput);
    bodyInput.decrPosition(SIZE_OF_REC_HEADER);

    if (header.getRecordType() == RecordType.INFIMUM || header.getRecordType() == RecordType.SUPREMUM) {
      GenericRecord mum = new GenericRecord(header, schema, pageNumber);
      mum.setPrimaryKeyPosition(primaryKeyPos);
      log.debug("read system record recordHeader={}", header);
      return mum;
    }

    // nullByteSize is an array indicating which fields(nullable) are null.
    List<String> nullColumnNames = null;
    int nullableColumnNum = schema.getNullableColumnNum();
    int nullByteSize = (nullableColumnNum + 7) / 8;
    if (isLeafPage && schema.containsNullColumn()) {
      int[] nullBitmap = Utils.getBitArray(bodyInput, nullableColumnNum);
      nullColumnNames = Utils.getFromBitArray(schema.getNullableColumnList(), nullBitmap, Column::getName);
    }

    // For each non-NULL variable-length field, the record header contains the length in one or two bytes.
    int[] varLenArray = null;
    boolean[] overflowPageArray = null;
    if (isLeafPage && schema.containsVariableLengthColumn()) {
      varLenArray = new int[schema.getVariableLengthColumnNum()];
      overflowPageArray = new boolean[schema.getVariableLengthColumnNum()];
      bodyInput.decrPosition(nullByteSize);
      for (int i = 0; i < schema.getVariableLengthColumnNum(); i++) {
        Column varColumn = schema.getVariableLengthColumnList().get(i);
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
        varLenArray[i] = len;
        overflowPageArray[i] = overflowPageFlag;
      }
    }

    // read primary key
    bodyInput.setPosition(primaryKeyPos);
    GenericRecord record = new GenericRecord(header, schema, pageNumber);
    String primaryKeyColumnType = schema.getPrimaryKeyColumn().getType();
    Object primaryKey = ColumnFactory.getColumnParser(primaryKeyColumnType).readFrom(bodyInput);
    if (log.isDebugEnabled()) {
      log.debug("read record, pkPos={}, key={}, recordHeader={}, nullColumnNames={}, varLenArray={}",
          primaryKeyPos, primaryKey, header, nullColumnNames, Arrays.toString(varLenArray));
    }
    record.put(schema.getPrimaryKeyColumn().getName(), primaryKey);
    record.setPrimaryKeyPosition(primaryKeyPos);

    if (isLeafPage) {
      // skip 13 bytes, 6-byte transaction ID field and a 7-byte roll pointer field.
      bodyInput.skipBytes(13);
    }

    if (isLeafPage) {
      // read all columns
      int varLenIdx = 0;
      for (Column column : schema.getColumnList()) {
        if (column.isPrimaryKey()) {
          continue;
        }
        if (columnValueIsNull(nullColumnNames, column)) {
          record.put(column.getName(), null);
          if (column.isVariableLength()) {
            varLenIdx++;
          }
        } else {
          if (column.isVariableLength()) {
            checkState(varLenArray != null);
            // https://dev.mysql.com/doc/refman/5.7/en/innodb-row-format.html
            // Tables that use the COMPACT row format store the first 768 bytes of variable-length column values
            // (VARCHAR, VARBINARY, and BLOB and TEXT types) in the index record within the B-tree node, with the
            // remainder stored on overflow pages.
            // When a table is created with ROW_FORMAT=DYNAMIC, InnoDB can store long variable-length column values
            // (for VARCHAR, VARBINARY, and BLOB and TEXT types) fully off-page, with the clustered index record containing
            // only a 20-byte pointer to the overflow page.
            // if (varLenArray[varLenIdx] <= 768) {
            if (!overflowPageArray[varLenIdx]) {
              Object val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput, varLenArray[varLenIdx], schema.getCharset());
              record.put(column.getName(), val);
            } else {
              handleOverflowPage(bodyInput, record, column, varLenArray[varLenIdx]);
            }
            varLenIdx++;
          } else if (column.isFixedLength()) {
            Object val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput, column.getMaxVarLen(), schema.getCharset());
            record.put(column.getName(), val);
          } else {
            Object val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput);
            record.put(column.getName(), val);
          }
        }
      }
    } else {
      long childPageNumber = bodyInput.readUnsignedInt();
      log.debug("read record, pkPos={}, key={}, childPage={}", primaryKeyPos, primaryKey, childPageNumber);
      record.setChildPageNumber(childPageNumber);
    }

    // set to next record position
    checkPositionIndex(record.nextRecordPosition(), SIZE_OF_BODY, "next record position out of bound");
    bodyInput.setPosition(record.nextRecordPosition());

    return record;
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
   * For each non-NULL variable-length field, the record header contains the length of the column in one or
   * two bytes. Two bytes are only needed if part of the column is stored externally in overflow pages
   * or the maximum length exceeds 255 bytes and the actual length exceeds 127 bytes.
   * For an externally stored column, the 2-byte length indicates the length of the internally stored
   * part plus the 20-byte pointer to the externally stored part. The internal part is 768 bytes, so the length
   * is 768+20. The 20-byte pointer stores the true length of the column.
   * <p>
   * Add by me. Note that charset will be considered when calculating max length of var-len field.
   *
   * @param varColumn column
   * @param len       first bytes read converted to unsigned int
   * @return if need to read more bytes
   */
  private boolean isTwoBytesLen(Column varColumn, int len) {
    int factor = 1;
    if (CHAR_TYPES.contains(varColumn.getType())) {
      factor = schema.getMaxBytesForOneChar();
    }
    return len > 127
        && (BLOB_TEXT_TYPES.contains(varColumn.getType()) || (varColumn.getMaxVarLen() * factor) > 255);
  }

  private void handleOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    if (BLOB_TYPES.contains(column.getType()) || VARBINARY.equals(column.getType())) {
      handleBlobOverflowPage(bodyInput, record, column, varLen);
    } else if (TEXT_TYPES.contains(column.getType()) || VARCHAR.equals(column.getType()) || CHAR.equals(column.getType())) {
      handleCharacterOverflowPage(bodyInput, record, column, varLen);
    } else {
      throw new UnsupportedOperationException("handle overflow page unsupported for type " + column.getType());
    }
  }

  private void handleCharacterOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    int varLenWithoutOffPagePointer = varLen - 20;
    Object val = null;
    if (varLenWithoutOffPagePointer > 0) {
      val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput, varLenWithoutOffPagePointer, schema.getCharset());
    }
    OverflowPagePointer overflowPagePointer = OverflowPagePointer.fromSlice(bodyInput);
    StringBuilder sb = new StringBuilder(varLenWithoutOffPagePointer + (int) overflowPagePointer.getLength());
    if (val != null) {
      sb.append((String) val);
    }
    Blob blob;
    long nextPageNumber = overflowPagePointer.getPageNumber();
    do {
      blob = loadBlobPage(nextPageNumber, overflowPagePointer.getPageOffset());
      byte[] content = blob.read();
      try {
        sb.append(new String(content, schema.getCharset()));
      } catch (UnsupportedEncodingException e) {
        throw new ReaderException("read blob page content failed");
      }
      if (blob.hasNext()) {
        nextPageNumber = blob.getNextPageNumber();
      }
      log.debug("read overflow page {}, content length={}, is end? = {}", overflowPagePointer, content.length, !blob.hasNext());
    } while (blob.hasNext());
    record.put(column.getName(), sb.toString());
  }

  private void handleBlobOverflowPage(SliceInput bodyInput, GenericRecord record, Column column, int varLen) {
    int varLenWithoutOffPagePointer = varLen - 20;
    Object val = null;
    if (varLenWithoutOffPagePointer > 0) {
      val = ColumnFactory.getColumnParser(column.getType()).readFrom(bodyInput, 768, schema.getCharset());
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
      byte[] content = blob.read();
      buffer.put(content);
      if (blob.hasNext()) {
        nextPageNumber = blob.getNextPageNumber();
      }
      log.debug("read overflow page {}, content length={}, is end? = {}", overflowPagePointer, content.length, !blob.hasNext());
    } while (blob.hasNext());
    record.put(column.getName(), buffer.array());
  }

}
