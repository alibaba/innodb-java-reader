/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.InnerPage;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.Slices;
import com.alibaba.innodb.java.reader.util.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_HEADER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.alibaba.innodb.java.reader.util.Utils.humanReadableBytes;
import static com.google.common.base.Preconditions.checkState;

/**
 * FileChannelStorageServiceImpl
 * <p>
 * Use Java NIO2.0 to read file, leverage page cache, data are read into JVM direct memory then copy to heap.
 * <p>
 * Note that to achieve GC-less load, we prefer to use DirectBuffer in ThreadLocal. We could create one
 * DirectByteBuffer pool to make the same result, but here we use HeapByteBuffer
 * because direct byte buffer cannot be released once page is load, the lifecycle has to be extended until decoding by
 * {@link com.alibaba.innodb.java.reader.service.IndexService} is done, so we cannot simply add
 * try-finally to clean the buffer after load and recycle them to the pool. The lifecycle of the direct byte buffer must be
 * managed by index service as well, this will be enhanced in the future.
 *
 * @author xu.zx
 */
@Slf4j
public class FileChannelStorageServiceImpl implements StorageService {

  private FileChannel fileChannel;

  private long numOfPages;

  private ThreadLocal<ByteBuffer> pageHeaderBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(SIZE_OF_FIL_HEADER));

  @Override
  public void open(String ibdFilePath) throws IOException {
    fileChannel = new FileInputStream(new File(ibdFilePath)).getChannel();
    long tableFileLength = fileChannel.size();
    checkState(tableFileLength % SIZE_OF_PAGE == 0, "table file length is invalid");
    this.numOfPages = tableFileLength / SIZE_OF_PAGE;
    log.debug("Open {} done, len={}({}), numOfPages={}", ibdFilePath, tableFileLength, humanReadableBytes(tableFileLength), numOfPages);
  }

  @Override
  public InnerPage loadPage(long pageNumber) throws ReaderException {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(SIZE_OF_PAGE);
      fileChannel.read(buffer, pageNumber * SIZE_OF_PAGE);
      return new InnerPage(pageNumber, Slices.fromByteBuffer(buffer));
    } catch (IOException e) {
      throw new ReaderException("load page " + pageNumber + "failed", e);
    }
  }

  @Override
  public FilHeader loadPageHeader(long pageNumber) throws ReaderException {
    try {
      // GC-less, get thread local buffer for reusing
      ByteBuffer buffer = pageHeaderBuffer.get();
      buffer.clear();
      fileChannel.read(buffer, pageNumber * SIZE_OF_PAGE);
      return FilHeader.fromSlice(Slices.fromByteBuffer(buffer).input());
    } catch (IOException e) {
      throw new ReaderException("load page " + pageNumber + "failed", e);
    }
  }

  @Override
  public void close() throws IOException {
    Utils.close(fileChannel);
  }

  @Override
  public long numOfPages() {
    return numOfPages;
  }
}
