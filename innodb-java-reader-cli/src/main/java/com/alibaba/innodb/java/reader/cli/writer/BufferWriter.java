/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

import com.alibaba.innodb.java.reader.util.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use Java NIO 2.0 to writer file. Leverage system DFS to write to disk, write into page cache.
 *
 * @author xu.zx
 */
@Slf4j
public class BufferWriter implements Writer {

  private final String outputFilePath;

  private FileChannel fileChannel;

  public BufferWriter(String outputFilePath) {
    this.outputFilePath = outputFilePath;
  }

  @Override
  public void open() {
    checkNotNull(outputFilePath);
    try {
      this.fileChannel = new FileOutputStream(new File(outputFilePath), false).getChannel();
      log.debug("Use buffer io to write file {}", outputFilePath);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void write(String text) throws WriterException {
    try {
      fileChannel.write(ByteBuffer.wrap(text.getBytes("UTF-8")));
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void close() throws WriterException {
    try {
      Utils.close(fileChannel);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public boolean ifNewLineAfterWrite() {
    return true;
  }
}
