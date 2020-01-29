/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli.writer;

import com.alibaba.innodb.java.reader.util.Utils;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use direct io to bypass page cache to write file.
 *
 * @author xu.zx
 */
@Slf4j
public class DirectWriter implements Writer {

  private static final int BUFFER_SIZE = 1 * 1024 * 1024;

  private String outputFilePath;

  private DirectRandomAccessFile fout;

  public DirectWriter(String outputFilePath) {
    this.outputFilePath = outputFilePath;
  }

  @Override
  public void open() {
    checkNotNull(outputFilePath);
    try {
      fout = new DirectRandomAccessFile(new File(outputFilePath), "rw", BUFFER_SIZE);
      log.debug("Use direct io to write file {}", outputFilePath);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void write(String text) throws WriterException {
    try {
      byte[] bytes = text.getBytes("UTF-8");
      fout.write(bytes);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public void close() throws WriterException {
    try {
      Utils.close(fout);
    } catch (IOException e) {
      throw new WriterException(e);
    }
  }

  @Override
  public boolean ifNewLineAfterWrite() {
    return true;
  }
}
