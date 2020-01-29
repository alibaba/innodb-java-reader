package com.alibaba.innodb.java.reader.cli.writer;

import com.alibaba.innodb.java.reader.cli.OutputIOMode;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.cli.OutputIOMode.BUFFER;
import static com.alibaba.innodb.java.reader.cli.OutputIOMode.DIRECT;
import static com.alibaba.innodb.java.reader.cli.OutputIOMode.MMAP;

/**
 * WriterFactory
 *
 * @author xu.zx
 */
@Slf4j
public class WriterFactory {

  public static Writer build(OutputIOMode outputIOMode, String outputFilePath) {
    if (BUFFER.equals(outputIOMode)) {
      return new BufferWriter(outputFilePath);
    } else if (DIRECT.equals(outputIOMode)) {
      return new DirectWriter(outputFilePath);
    } else if (MMAP.equals(outputIOMode)) {
      return new MmapWriter(outputFilePath);
    } else {
      throw new WriterException("should not happen");
    }
  }

}
