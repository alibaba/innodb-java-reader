/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.AllocatedPage;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.InnerPage;
import com.alibaba.innodb.java.reader.page.SdiPage;
import com.alibaba.innodb.java.reader.page.blob.Blob;
import com.alibaba.innodb.java.reader.page.fsphdr.FspHdrXes;
import com.alibaba.innodb.java.reader.page.ibuf.IbufBitmap;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.inode.Inode;
import com.alibaba.innodb.java.reader.schema.Schema;
import com.alibaba.innodb.java.reader.schema.SchemaUtil;
import com.alibaba.innodb.java.reader.service.IndexService;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.service.impl.FileChannelStorageServiceImpl;
import com.alibaba.innodb.java.reader.service.impl.IndexServiceImpl;
import com.alibaba.innodb.java.reader.util.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.alibaba.innodb.java.reader.page.PageType.INDEX;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Table space reader to manipulate *.ibd file. Note that operations are thread-safe.
 *
 * @author xu.zx
 */
@Slf4j
public class TableReader implements Closeable {

  private String ibdFilePath;

  private Schema schema;

  private IndexService indexService;

  private StorageService storageService;

  public TableReader(String ibdFilePath, String createTableSql) {
    this(ibdFilePath, SchemaUtil.covertFromSqlToSchema(createTableSql));
  }

  public TableReader(String ibdFilePath, Schema schema) {
    this.ibdFilePath = ibdFilePath;
    this.schema = schema;
    this.schema.validate();
  }

  public void open() {
    try {
      if (storageService != null) {
        throw new ReaderException("TableReader can only be opened once");
      }
      storageService = new FileChannelStorageServiceImpl();
      storageService.open(ibdFilePath);
      indexService = new IndexServiceImpl(storageService, schema);
      log.debug("{}", schema);
    } catch (IOException e) {
      throw new ReaderException("open failed", e);
    }
  }

  /**
   * Get number of pages. If page size is 16KiB, then the value will be <code>file size / 16384</code>.
   * @return
   */
  public long getNumOfPages() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    return storageService.numOfPages();
  }

  /**
   * Read all pages into memory which may cause OutOfMemory when tablespace file size is too big.
   * Use {@link #getPageIterator()} if file size is too big.
   *
   * @return list of AbstractPage
   */
  public List<AbstractPage> readAllPages() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    List<AbstractPage> result = new ArrayList<>((int) storageService.numOfPages());
    for (int i = 0; i < storageService.numOfPages(); i++) {
      result.add(readPage(i));
    }
    return result;
  }

  /**
   * Get page iterator.
   *
   * @return Iterator of AbstractPage
   */
  public Iterator<AbstractPage> getPageIterator() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    final int numOfPages = (int) storageService.numOfPages();
    return new Iterator<AbstractPage>() {

      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < numOfPages;
      }

      @Override
      public AbstractPage next() {
        return readPage(index++);
      }
    };
  }

  /**
   * Read all page headers into memory.
   *
   * @return list of FilHeader
   */
  public List<FilHeader> readAllPageHeaders() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    List<FilHeader> result = new ArrayList<>((int) storageService.numOfPages());
    for (long i = 0L; i < storageService.numOfPages(); i++) {
      result.add(storageService.loadPageHeader(i));
    }
    return result;
  }

  /**
   * Read one page as {@link AbstractPage}.
   *
   * @param pageNumber page number
   * @return AbstractPage
   */
  public AbstractPage readPage(long pageNumber) {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    InnerPage page = storageService.loadPage(pageNumber);
    checkNotNull(page, "page cannot be null which should not happen");
    if (page.pageType() == null) {
      throw new ReaderException("page type not supported, " + page.getFilHeader());
    }
    switch (page.pageType()) {
      case FILE_SPACE_HEADER:
        return new FspHdrXes(page);
      case EXTENT_DESCRIPTOR:
        return new FspHdrXes(page);
      case IBUF_BITMAP:
        return new IbufBitmap(page);
      case INODE:
        return new Inode(page);
      case INDEX:
        return new Index(page, schema);
      case ALLOCATED:
        return new AllocatedPage(page);
      case BLOB:
        return new Blob(page);
      case SDI:
        return new SdiPage(page);
      default:
        throw new ReaderException("InnerPage type " + page.pageType() + " not supported");
    }
  }

  /**
   * Get all index page filling rate, use iterator pattern to avoid OutOfMemory.
   *
   * @return filling rate
   */
  public double getAllIndexPageFillingRate() {
    Iterator<AbstractPage> iterator = getPageIterator();
    long totalUsedBytes = 0L;
    long indexPageNum = 0;
    while (iterator.hasNext()) {
      AbstractPage page = iterator.next();
      if (INDEX.equals(page.pageType())) {
        totalUsedBytes += ((Index) page).usedBytesInIndexPage();
        indexPageNum++;
      }
    }
    return totalUsedBytes * 1.0D / (indexPageNum * SIZE_OF_PAGE);
  }

  /**
   * Get single index page filling rate.
   *
   * @param pageNumber page number
   * @return filling rate
   */
  public double getIndexPageFillingRate(int pageNumber) {
    AbstractPage page = readPage(pageNumber);
    if (!INDEX.equals(page.pageType())) {
      throw new ReaderException("page type is not index, " + page.getFilHeader());
    }
    return ((Index) page).usedBytesInIndexPage() * 1.0D / SIZE_OF_PAGE;
  }

  public List<GenericRecord> queryByPageNumber(int pageNumber) {
    return indexService.queryByPageNumber(pageNumber);
  }

  public List<GenericRecord> queryByPageNumber(long pageNumber) {
    return indexService.queryByPageNumber(pageNumber);
  }

  public GenericRecord queryByPrimaryKey(Object key) {
    return indexService.queryByPrimaryKey(key);
  }

  public List<GenericRecord> queryAll() {
    return indexService.queryAll(Optional.empty());
  }

  public List<GenericRecord> queryAll(Predicate<GenericRecord> recordPredicate) {
    return indexService.queryAll(Optional.of(recordPredicate));
  }

  public List<GenericRecord> rangeQueryByPrimaryKey(Object lowerInclusiveKey, Object upperExclusiveKey) {
    return indexService.rangeQueryByPrimaryKey(lowerInclusiveKey, upperExclusiveKey, Optional.empty());
  }

  public List<GenericRecord> rangeQueryByPrimaryKey(Object lowerInclusiveKey, Object upperExclusiveKey, Predicate<GenericRecord> recordPredicate) {
    return indexService.rangeQueryByPrimaryKey(lowerInclusiveKey, upperExclusiveKey, Optional.of(recordPredicate));
  }

  public Iterator<GenericRecord> getQueryAllIterator() {
    return indexService.getQueryAllIterator();
  }

  public Iterator<GenericRecord> getRangeQueryIterator(Object lowerInclusiveKey, Object upperExclusiveKey) {
    return indexService.getRangeQueryIterator(lowerInclusiveKey, upperExclusiveKey);
  }

  @Override
  public void close() {
    try {
      Utils.close(storageService);
    } catch (IOException e) {
      throw new ReaderException(e);
    }
  }

}
