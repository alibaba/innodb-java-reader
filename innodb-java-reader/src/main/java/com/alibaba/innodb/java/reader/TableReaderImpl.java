/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.comparator.DefaultKeyComparator;
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
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.TableDefUtil;
import com.alibaba.innodb.java.reader.service.IndexService;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.service.impl.FileChannelStorageServiceImpl;
import com.alibaba.innodb.java.reader.service.impl.IndexServiceImpl;
import com.alibaba.innodb.java.reader.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.alibaba.innodb.java.reader.page.PageType.INDEX;
import static com.alibaba.innodb.java.reader.util.Utils.makeNotNull;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reader to query upon an Innodb file with suffix of <tt>*.ibd</tt>.
 * All operations are thread-safe.
 *
 * @author xu.zx
 */
@Slf4j
public class TableReaderImpl implements TableReader {

  private String ibdFilePath;

  private TableDef tableDef;

  private IndexService indexService;

  private StorageService storageService;

  private Comparator<List<Object>> keyComparator;

  public TableReaderImpl(String ibdFilePath, String createTableSql) {
    this(ibdFilePath, TableDefUtil.covertToTableDef(createTableSql), new DefaultKeyComparator());
  }

  public TableReaderImpl(String ibdFilePath, String createTableSql, Comparator<List<Object>> keyComparator) {
    this(ibdFilePath, TableDefUtil.covertToTableDef(createTableSql), keyComparator);
  }

  public TableReaderImpl(String ibdFilePath, TableDef tableDef) {
    this(ibdFilePath, tableDef, new DefaultKeyComparator());
  }

  public TableReaderImpl(String ibdFilePath, TableDef tableDef, Comparator<List<Object>> keyComparator) {
    this.ibdFilePath = ibdFilePath;
    this.tableDef = tableDef;
    this.keyComparator = keyComparator;
    this.tableDef.validate();
  }

  @Override
  public void open() {
    try {
      if (storageService != null) {
        throw new ReaderException("TableReader can only be opened once");
      }
      storageService = new FileChannelStorageServiceImpl();
      storageService.open(ibdFilePath);
      indexService = new IndexServiceImpl(storageService, tableDef, keyComparator);
      log.debug("{}", tableDef);
    } catch (IOException e) {
      throw new ReaderException("Open " + ibdFilePath + " failed", e);
    }
  }

  @Override
  public long getNumOfPages() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    return storageService.numOfPages();
  }

  @Override
  public List<AbstractPage> readAllPages() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    List<AbstractPage> result = new ArrayList<>((int) storageService.numOfPages());
    for (int i = 0; i < storageService.numOfPages(); i++) {
      result.add(readPage(i));
    }
    return result;
  }

  @Override
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

  @Override
  public List<FilHeader> readAllPageHeaders() {
    checkNotNull(storageService, "storageService should not null, please make sure TableReader is opened");
    List<FilHeader> result = new ArrayList<>((int) storageService.numOfPages());
    for (long i = 0L; i < storageService.numOfPages(); i++) {
      result.add(storageService.loadPageHeader(i));
    }
    return result;
  }

  @Override
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
        return new Index(page, tableDef);
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

  @Override
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

  @Override
  public double getIndexPageFillingRate(int pageNumber) {
    AbstractPage page = readPage(pageNumber);
    if (!INDEX.equals(page.pageType())) {
      throw new ReaderException("page type is not index, " + page.getFilHeader());
    }
    return ((Index) page).usedBytesInIndexPage() * 1.0D / SIZE_OF_PAGE;
  }

  @Override
  public List<GenericRecord> queryByPageNumber(int pageNumber) {
    return indexService.queryByPageNumber(pageNumber);
  }

  @Override
  public List<GenericRecord> queryByPageNumber(long pageNumber) {
    return indexService.queryByPageNumber(pageNumber);
  }

  @Override
  public GenericRecord queryByPrimaryKey(List<Object> key) {
    return indexService.queryByPrimaryKey(key);
  }

  @Override
  public List<GenericRecord> queryAll() {
    return indexService.queryAll(Optional.empty());
  }

  @Override
  public List<GenericRecord> queryAll(Predicate<GenericRecord> recordPredicate) {
    return indexService.queryAll(Optional.of(recordPredicate));
  }

  @Override
  public List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lowerInclusiveKey, List<Object> upperExclusiveKey) {
    return indexService.rangeQueryByPrimaryKey(
        makeNotNull(lowerInclusiveKey), makeNotNull(upperExclusiveKey), Optional.empty());
  }

  @Override
  public List<GenericRecord> rangeQueryByPrimaryKey(List<Object> lowerInclusiveKey, List<Object> upperExclusiveKey,
                                                    Predicate<GenericRecord> recordPredicate) {
    return indexService.rangeQueryByPrimaryKey(
        makeNotNull(lowerInclusiveKey), makeNotNull(upperExclusiveKey), Optional.of(recordPredicate));
  }

  @Override
  public Iterator<GenericRecord> getQueryAllIterator() {
    return indexService.getQueryAllIterator();
  }

  @Override
  public Iterator<GenericRecord> getRangeQueryIterator(List<Object> lowerInclusiveKey, List<Object> upperExclusiveKey) {
    return indexService.getRangeQueryIterator(makeNotNull(lowerInclusiveKey), makeNotNull(upperExclusiveKey));
  }

  @Override
  public TableDef getTableDef() {
    return tableDef;
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
