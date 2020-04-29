/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.heatmap;

import com.google.common.collect.Maps;

import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.TableReaderImpl;
import com.alibaba.innodb.java.reader.page.FilHeader;
import com.alibaba.innodb.java.reader.page.PageType;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.TableDefUtil;
import com.alibaba.innodb.java.reader.util.Pair;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

/**
 * Generate LSN heatmap based on the lsn of every page in innodb file.
 *
 * @author xu.zx
 */
@Slf4j
public class GenLsnHeatmapUtil {

  /**
   * Dump page lsn heatmap.
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param createTableSql    create table sql
   * @param pageWrapNum       number of pages per line in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, String createTableSql,
                          int pageWrapNum) throws IOException, TemplateException {
    dump(sourceIbdFilePath, destHtmlFilePath, TableDefUtil.covertToTableDef(createTableSql),
        pageWrapNum, Optional.empty());
  }

  /**
   * Dump page lsn heatmap.
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param createTableSql    create table sql
   * @param pageWrapNum       number of pages per line in heatmap
   * @param widthAndHeight    optional width and height in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, String createTableSql, int pageWrapNum,
                          Optional<Pair<String, String>> widthAndHeight) throws IOException, TemplateException {
    dump(sourceIbdFilePath, destHtmlFilePath, TableDefUtil.covertToTableDef(createTableSql),
        pageWrapNum, widthAndHeight);
  }

  /**
   * Dump page lsn heatmap.
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param tableDef          table definition
   * @param pageWrapNum       number of pages per line in heatmap
   * @param widthAndHeight    optional width and height in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, TableDef tableDef, int pageWrapNum,
                          Optional<Pair<String, String>> widthAndHeight) throws IOException, TemplateException {
    Pair<String, String> defaultWidthAndHeight = new Pair<>("1000", "1000");

    Configuration configuration = new Configuration(Configuration.getVersion());
    configuration.setClassForTemplateLoading(GenLsnHeatmapUtil.class, "/templates");
    configuration.setDefaultEncoding("utf-8");
    Template template = configuration.getTemplate("lsn-heatmap.ftl");

    Map<String, Object> dataModel = Maps.newHashMapWithExpectedSize(4);

    log.info("Start dump {} to {}", sourceIbdFilePath, destHtmlFilePath);
    long start = System.currentTimeMillis();
    try (TableReader reader = new TableReaderImpl(sourceIbdFilePath, tableDef)) {
      reader.open();
      List<FilHeader> pageHeaders = reader.readAllPageHeaders();
      List<FilHeader> printablePageHeaders = pageHeaders.stream()
          .filter(p -> p.getPageType() != PageType.ALLOCATED && p.getPageType() != PageType.INDEX).collect(toList());
      for (FilHeader printablePageHeader : printablePageHeaders) {
        System.out.println(printablePageHeader);
      }
      List<FilHeader> validPageHeaders = pageHeaders.stream()
          .filter(p -> p.getPageType() != PageType.ALLOCATED).collect(toList());
      FilHeader minLsnPageHeader = validPageHeaders.stream()
          .min(Comparator.comparingLong(FilHeader::getLastModifiedLsn)).get();
      FilHeader maxLsnPageHeader = validPageHeaders.stream()
          .max(Comparator.comparingLong(FilHeader::getLastModifiedLsn)).get();
      long minLsn = minLsnPageHeader.getLastModifiedLsn();
      long maxLsn = maxLsnPageHeader.getLastModifiedLsn();
      long range = maxLsn - minLsn;
      List<Line<Long>> lsnList = new ArrayList<>((pageHeaders.size() / pageWrapNum + 1));
      List<String> yList = new ArrayList<>((pageHeaders.size() / pageWrapNum + 1));
      AtomicInteger counter = new AtomicInteger(0);
      pageHeaders.forEach(p -> {
        if (counter.getAndIncrement() % pageWrapNum == 0) {
          lsnList.add(new Line<>());
          yList.add(String.format("page%4d", counter.get() - 1));
        }
        long val = 0L;
        if (!PageType.ALLOCATED.equals(p.getPageType())) {
          val = (p.getLastModifiedLsn() - minLsn) * 100L / range;
        }
        lsnList.get(lsnList.size() - 1).getList().add(val);
      });
      Collections.reverse(lsnList);
      Collections.reverse(yList);
      dataModel.put("lsnlist", lsnList);
      dataModel.put("ylist", yList);
      dataModel.put("width", widthAndHeight.orElse(defaultWidthAndHeight).getFirst());
      dataModel.put("height", widthAndHeight.orElse(defaultWidthAndHeight).getSecond());
    }

    try (Writer out = new FileWriter(new File(destHtmlFilePath))) {
      template.process(dataModel, out);
    }
    log.info("Successfully dump lsn heatmap to {} using {}ms", destHtmlFilePath, (System.currentTimeMillis() - start));
  }

}
