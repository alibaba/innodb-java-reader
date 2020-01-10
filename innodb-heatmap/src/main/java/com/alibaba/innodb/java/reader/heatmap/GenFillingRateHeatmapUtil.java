/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.heatmap;

import com.google.common.collect.Maps;

import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.schema.Schema;
import com.alibaba.innodb.java.reader.schema.SchemaUtil;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;
import static com.alibaba.innodb.java.reader.page.PageType.INDEX;

/**
 * Generate page filling rate heatmap based on the lsn of every page in innodb file.
 *
 * @author xu.zx
 */
@Slf4j
public class GenFillingRateHeatmapUtil {

  /**
   * dump page filling rate heatmap
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param createTableSql    create table sql
   * @param pageWrapNum       number of pages per line in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, String createTableSql, int pageWrapNum) throws IOException, TemplateException {
    dump(sourceIbdFilePath, destHtmlFilePath, SchemaUtil.covertFromSqlToSchema(createTableSql), pageWrapNum, Optional.empty());
  }

  /**
   * dump page filling rate heatmap
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param createTableSql    create table sql
   * @param pageWrapNum       number of pages per line in heatmap
   * @param widthAndHeight    optional width and height in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, String createTableSql, int pageWrapNum,
                          Optional<Pair<String, String>> widthAndHeight) throws IOException, TemplateException {
    dump(sourceIbdFilePath, destHtmlFilePath, SchemaUtil.covertFromSqlToSchema(createTableSql), pageWrapNum, widthAndHeight);
  }

  /**
   * dump page filling rate heatmap
   *
   * @param sourceIbdFilePath innodb ibd file path
   * @param destHtmlFilePath  destination html file path
   * @param schema            table schema
   * @param pageWrapNum       number of pages per line in heatmap
   * @param widthAndHeight    optional width and height in heatmap
   */
  public static void dump(String sourceIbdFilePath, String destHtmlFilePath, Schema schema, int pageWrapNum, Optional<Pair<String, String>> widthAndHeight) throws IOException, TemplateException {
    Pair<String, String> defaultWidthAndHeight = new Pair<>("1000", "1000");

    Configuration configuration = new Configuration(Configuration.getVersion());
    configuration.setClassForTemplateLoading(GenFillingRateHeatmapUtil.class, "/templates");
    configuration.setDefaultEncoding("utf-8");
    Template template = configuration.getTemplate("filling-rate-heatmap.ftl");

    Map<String, Object> dataModel = Maps.newHashMapWithExpectedSize(4);

    log.info("Start dump {} to {}", sourceIbdFilePath, destHtmlFilePath);
    long start = System.currentTimeMillis();
    try (TableReader reader = new TableReader(sourceIbdFilePath, schema)) {
      reader.open();
      Iterator<AbstractPage> pageIterator = reader.getPageIterator();
      List<Line<Float>> fillingRateList = new ArrayList<>((int) reader.getNumOfPages() / pageWrapNum + 1);
      List<String> yList = new ArrayList<>((int) reader.getNumOfPages() / pageWrapNum + 1);
      AtomicInteger counter = new AtomicInteger(0);
      while (pageIterator.hasNext()) {
        AbstractPage page = pageIterator.next();
        if (counter.getAndIncrement() % pageWrapNum == 0) {
          fillingRateList.add(new Line<>());
          yList.add(String.format("page%4d", counter.get() - 1));
        }
        float val = 0.0F;
        if (INDEX.equals(page.pageType())) {
          val = ((Index) page).usedBytesInIndexPage() * 1.0F / SIZE_OF_PAGE;
        }
        fillingRateList.get(fillingRateList.size() - 1).getList().add(val);
      }
      Collections.reverse(fillingRateList);
      Collections.reverse(yList);
      dataModel.put("fillingRateList", fillingRateList);
      dataModel.put("ylist", yList);
      dataModel.put("width", widthAndHeight.orElse(defaultWidthAndHeight).getFirst());
      dataModel.put("height", widthAndHeight.orElse(defaultWidthAndHeight).getSecond());
    }

    try (Writer out = new FileWriter(new File(destHtmlFilePath))) {
      template.process(dataModel, out);
    }
    log.info("Successfully dump filling rate heatmap to {} using {}ms", destHtmlFilePath, (System.currentTimeMillis() - start));
  }

}
