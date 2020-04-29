package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.Utils;

import org.junit.Test;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

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
      assertThat(numOfPages, greaterThan(1000L));
      long start = System.currentTimeMillis();
      int runTimes = 200;
      for (int times = 0; times < runTimes; times++) {
        for (long i = 0; i < numOfPages; i++) {
          storageService.loadPage(i);
        }
      }
      long elapsedTimeInMs = System.currentTimeMillis() - start;
      assertThat(elapsedTimeInMs, lessThan(90000L));
      log.info("load {} pages {} times ({}) using {}ms", numOfPages, runTimes,
          Utils.humanReadableBytes(SIZE_OF_PAGE * numOfPages * runTimes), elapsedTimeInMs);
    }
  }

}
