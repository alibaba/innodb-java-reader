/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

import com.alibaba.innodb.java.reader.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use mmap to avoid copying from Java <code>DirectByteBuffer</code> to kernel memory.
 *
 * @author xu.zx
 */
public class MmapWriter implements Writer {

  private static final int MAPPED_BUFFER_SIZE = 16 * 1024 * 1024;

  private String outputFilePath;

  private FileChannel fileChannel;

  private MappedByteBuffer mappedByteBuffer;

  private long fileOffset;

  public MmapWriter(String outputFilePath) {
    this.outputFilePath = outputFilePath;
  }

  @Override
  public void open() {
    checkNotNull(outputFilePath);
    try {
      this.fileChannel = new RandomAccessFile(new File(outputFilePath), "rw").getChannel();
      this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_BUFFER_SIZE);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void write(String text) throws WriterException {
    try {
      byte[] bytes = text.getBytes("UTF-8");
      ensureCapacity(bytes.length);
      mappedByteBuffer.put(bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void close() throws WriterException {
    try {
      fileOffset += mappedByteBuffer.position();
      MMapUtil.unmap(mappedByteBuffer);
      if (fileChannel.isOpen()) {
        fileChannel.truncate(fileOffset);
      }
      Utils.close(fileChannel);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public boolean ifNewLineAfterWrite() {
    return true;
  }

  private void ensureCapacity(int bytes) throws IOException {
    if (mappedByteBuffer.remaining() < bytes) {
      fileOffset += mappedByteBuffer.position();
      MMapUtil.unmap(mappedByteBuffer);
      mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileOffset, MAPPED_BUFFER_SIZE);
    }
  }
}
