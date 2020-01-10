package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.Utils;

import org.junit.Test;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;

/**
 * @author xu.zx
 */
@Slf4j
public class FileChannelStorageServiceImplTest extends AbstractTest {

  @Test
  public void testLoadPage() throws IOException {
    try (StorageService storageService = new FileChannelStorageServiceImpl()) {
      storageService.open(IBD_FILE_BASE_PATH_MYSQL56 + "multiple/level/tb11.ibd");
      long numOfPages = storageService.numOfPages();
      log.info("numOfPages={}", numOfPages);
      long start = System.currentTimeMillis();
      int runTimes = 500;
      for (int times = 0; times < runTimes; times++) {
        for (long i = 0; i < numOfPages; i++) {
          storageService.loadPage(i);
        }
      }
      log.info("load {} pages {} times ({}) using {}ms", numOfPages, runTimes,
          Utils.humanReadableBytes(SIZE_OF_PAGE * numOfPages * runTimes), System.currentTimeMillis() - start);
    }
  }

}
