package com.alibaba.innodb.java.reader.service.impl;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.exception.PageLoadException;
import com.alibaba.innodb.java.reader.service.StorageService;
import com.alibaba.innodb.java.reader.util.Utils;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_HEADER;
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

  @Test(expected = PageLoadException.class)
  public void testLoadPageRejectsTruncatedPage() throws IOException {
    Path file = createSyntheticFile(SIZE_OF_PAGE - 1);
    try (StorageService storageService = new FileChannelStorageServiceImpl()) {
      storageService.open(file.toString());
      storageService.loadPage(0);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test(expected = PageLoadException.class)
  public void testLoadPageHeaderRejectsTruncatedHeader() throws IOException {
    Path file = createSyntheticFile(SIZE_OF_FIL_HEADER - 1);
    try (StorageService storageService = new FileChannelStorageServiceImpl()) {
      storageService.open(file.toString());
      storageService.loadPageHeader(0);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  private Path createSyntheticFile(int length) throws IOException {
    Path targetDirectory = Paths.get("target").toAbsolutePath();
    Files.createDirectories(targetDirectory);
    Path file = Files.createTempFile(targetDirectory, "truncated-page-", ".ibd");
    Files.write(file, new byte[length]);
    return file;
  }

}
