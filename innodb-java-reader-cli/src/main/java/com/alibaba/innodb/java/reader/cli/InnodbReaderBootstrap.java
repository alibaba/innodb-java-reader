/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.heatmap.GenFillingRateHeatmapUtil;
import com.alibaba.innodb.java.reader.heatmap.GenLsnHeatmapUtil;
import com.alibaba.innodb.java.reader.heatmap.Pair;
import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.fsphdr.FspHdrXes;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.page.index.Index;
import com.alibaba.innodb.java.reader.page.inode.Inode;
import com.alibaba.innodb.java.reader.schema.Schema;
import com.alibaba.innodb.java.reader.util.Utils;

import freemarker.template.TemplateException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.innodb.java.reader.cli.CommandType.GEN_FILLING_RATE_HEATMAP;
import static com.alibaba.innodb.java.reader.cli.CommandType.GEN_LSN_HEATMAP;
import static com.alibaba.innodb.java.reader.page.PageType.EXTENT_DESCRIPTOR;
import static com.alibaba.innodb.java.reader.page.PageType.FILE_SPACE_HEADER;
import static com.alibaba.innodb.java.reader.page.PageType.INDEX;
import static com.alibaba.innodb.java.reader.page.PageType.INODE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

/**
 * InnodbReaderBootstrap
 *
 * @author xu.zx
 */
public class InnodbReaderBootstrap {

  public static final String JAR_NAME = "innodb-java-reader-cli.jar";

  private static String ALL_COMMANDS = Arrays.stream(CommandType.values()).map(CommandType::getType).collect(Collectors.joining(","));

  public static final JsonMapper JSON_MAPPER = JsonMapper.buildNormalMapper();

  private static boolean SHOW_HEADER = false;

  public static void main(String[] arguments) {
    CommandLineParser parser = new DefaultParser();

    Options options = new Options();
    options.addOption("h", "help", false, "usage");
    options.addOption("i", "ibd-file-path", true, "mandatory. innodb file path with suffix of .ibd");
    options.addOption("c", "command", true, "mandatory. command to run, valid commands are: " + ALL_COMMANDS);
    options.addOption("s", "create-table-sql-file-path", true, "create table sql file path by running SHOW CREATE TABLE <table_name>");
    options.addOption("json", "json-style", false, "set to true if you would like to show page info in json format style");
    options.addOption("jsonpretty", "json-pretty-style", false, "set to true if you would like to show page info in json pretty format style");
    options.addOption("showheader", "show-header", false, "set to true if you want to show table header");
    options.addOption("args", true, "arguments");

    String command = null;
    String ibdFilePath = null;
    String createTableSql = null;
    String args = null;
    // show all pages or show one page
    boolean jsonStyle = false;
    boolean jsonPrettyStyle = false;

    try {
      CommandLine line = parser.parse(options, arguments);

      if (line.hasOption("help")) {
        showHelp(options, 0);
      }

      if (line.hasOption("ibd-file-path")) {
        ibdFilePath = line.getOptionValue("ibd-file-path");
        Preconditions.checkArgument(StringUtils.isNotEmpty(ibdFilePath), "ibd-file-path is empty");
      } else {
        System.err.println("please input ibd-file-path");
        showHelp(options, 1);
      }

      if (line.hasOption("create-table-sql-file-path")) {
        String createTableSqlFilePath = line.getOptionValue("create-table-sql-file-path");
        Preconditions.checkArgument(StringUtils.isNotEmpty(createTableSqlFilePath), "create-table-sql-file-path is empty");
        List<String> lines = Files.readLines(new File(createTableSqlFilePath), Charset.defaultCharset());
        createTableSql = String.join(" ", lines);
      } else {
        System.err.println("please input create-table-sql");
        showHelp(options, 1);
      }

      if (line.hasOption("command")) {
        command = line.getOptionValue("command");
      } else {
        System.err.println("please input command to run, cmd=" + ALL_COMMANDS);
        showHelp(options, 1);
      }

      if (line.hasOption("args")) {
        args = line.getOptionValue("args");
      }

      if (line.hasOption("json-style")) {
        jsonStyle = true;
      }
      if (line.hasOption("json-pretty-style")) {
        jsonPrettyStyle = true;
      }
      if (line.hasOption("show-header")) {
        SHOW_HEADER = true;
      }

      CommandType commandType = EnumUtils.getEnum(CommandType.class, command.replace("-", "_").toUpperCase());

      if (commandType == null) {
        System.err.println("invalid command type, should be " + ALL_COMMANDS);
        showHelp(options, 1);
      }

      checkNotNull(commandType);
      switch (commandType) {
        case SHOW_ALL_PAGES:
          showAllPages(ibdFilePath, createTableSql);
          break;
        case SHOW_PAGES:
          checkNotNull(args, "args should not be null");
          List<Long> pageNumbers = Stream.of(args.split(",")).map(Long::parseLong).collect(toList());
          showPages(ibdFilePath, createTableSql, pageNumbers, jsonStyle, jsonPrettyStyle);
          break;
        case QUERY_ALL:
          queryAll(ibdFilePath, createTableSql);
          break;
        case QUERY_BY_PAGE_NUMBER:
          checkNotNull(args, "args should not be null");
          long pageNumber = Long.parseLong(args);
          queryByPageNumber(ibdFilePath, createTableSql, pageNumber);
          break;
        case QUERY_BY_PK:
          checkNotNull(args, "args should not be null");
          queryByPrimaryKey(ibdFilePath, createTableSql, args);
          break;
        case RANGE_QUERY_BY_PK:
          checkNotNull(args, "args should not be null");
          List<String> range = Stream.of(args.split("[, ]")).collect(toList());
          checkState(range != null && range.size() == 2, "argument number should not exactly two");
          Object lowerInclusiveKey = "null".equalsIgnoreCase(range.get(0)) ? null : range.get(0);
          Object upperExclusiveKey = "null".equalsIgnoreCase(range.get(1)) ? null : range.get(1);
          rangeQueryByPrimaryKey(ibdFilePath, createTableSql, lowerInclusiveKey, upperExclusiveKey);
          break;
        case GEN_LSN_HEATMAP:
          genHeatmap(ibdFilePath, createTableSql, args, commandType);
          break;
        case GEN_FILLING_RATE_HEATMAP:
          genHeatmap(ibdFilePath, createTableSql, args, commandType);
          break;
        case GET_ALL_INDEX_PAGE_FILLING_RATE:
          getAllIndexPageFillingRate(ibdFilePath, createTableSql);
          break;
        default:
          System.err.println("invalid command type, cmd=" + ALL_COMMANDS);
          showHelp(options, 1);
      }
    } catch (ParseException e) {
      System.err.println("Unexpected exception:" + e.getMessage());
    } catch (IOException e) {
      System.err.println("Unexpected IO exception:" + e.getMessage());
    }
  }

  private static void queryAll(String ibdFilePath, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader);
      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      StringBuilder b = new StringBuilder();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        System.out.println(Utils.arrayToString(record.getValues(), b));
      }
    }
  }

  private static void queryByPageNumber(String ibdFilePath, String createTableSql, long pageNumber) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader);
      List<GenericRecord> recordList = reader.queryByPageNumber(pageNumber);
      StringBuilder b = new StringBuilder();
      if (CollectionUtils.isNotEmpty(recordList)) {
        for (GenericRecord record : recordList) {
          System.out.println(Utils.arrayToString(record.getValues(), b));
        }
      }
    }
  }

  private static void queryByPrimaryKey(String ibdFilePath, String createTableSql, String primaryKey) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader);
      GenericRecord record = reader.queryByPrimaryKey(primaryKey);
      StringBuilder b = new StringBuilder();
      if (record != null) {
        System.out.println(Utils.arrayToString(record.getValues(), b));
      }
    }
  }

  private static void rangeQueryByPrimaryKey(String ibdFilePath, String createTableSql, Object lowerInclusiveKey, Object upperExclusiveKey) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader);
      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(lowerInclusiveKey, upperExclusiveKey);
      StringBuilder b = new StringBuilder();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        System.out.println(Utils.arrayToString(record.getValues(), b));
      }
    }
  }

  private static void showHelp(Options options, int exitCode) {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp("java -jar " + JAR_NAME, options, true);
    System.exit(exitCode);
  }

  private static void genHeatmap(String ibdFilePath, String createTableSql, String args, CommandType commandType) {
    checkNotNull(args, "args should not be null");
    String destHtmlPath = args;
    Optional<Pair<String, String>> widthAndHeight = Optional.empty();
    if (args.contains(" ")) {
      checkState(args.split(" ").length == 3, "argument number should not three when you want to specify width and height");
      destHtmlPath = args.split(" ")[0];
      List<String> wh = Stream.of(args.split(" ")).skip(1).collect(toList());
      widthAndHeight = Optional.of(new Pair<>(wh.get(0), wh.get(1)));
    }
    if (GEN_LSN_HEATMAP.equals(commandType)) {
      genLsnHeatmap(ibdFilePath, createTableSql, destHtmlPath, widthAndHeight);
    } else if (GEN_FILLING_RATE_HEATMAP.equals(commandType)) {
      genFillingRateHeatmap(ibdFilePath, createTableSql, destHtmlPath, widthAndHeight);
    }
  }

  private static void genLsnHeatmap(String ibdFilePath, String createTableSql, String destHtmlPath, Optional<Pair<String, String>> widthAndHeight) {
    try {
      GenLsnHeatmapUtil.dump(ibdFilePath, destHtmlPath, createTableSql, 64, widthAndHeight);
    } catch (IOException | TemplateException e) {
      e.printStackTrace();
    }
  }

  private static void genFillingRateHeatmap(String ibdFilePath, String createTableSql, String destHtmlPath, Optional<Pair<String, String>> widthAndHeight) {
    try {
      GenFillingRateHeatmapUtil.dump(ibdFilePath, destHtmlPath, createTableSql, 64, widthAndHeight);
    } catch (IOException | TemplateException e) {
      e.printStackTrace();
    }
  }

  private static void showAllPages(String ibdFilePath, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      Iterator<AbstractPage> iterator = reader.getPageIterator();
      System.out.println(StringUtils.repeat("=", 5) + "page number, page type, other info" + StringUtils.repeat("=", 5));
      while (iterator.hasNext()) {
        AbstractPage page = iterator.next();
        StringBuilder sb = new StringBuilder();
        sb.append(page.getPageNumber());
        sb.append(",");
        sb.append(page.pageType().name());
        if (FILE_SPACE_HEADER.equals(page.pageType()) || EXTENT_DESCRIPTOR.equals(page.pageType())) {
          sb.append(",space=");
          sb.append(((FspHdrXes) page).getFspHeader().getSpace());
          sb.append(",numPagesUsed=");
          sb.append(((FspHdrXes) page).getFspHeader().getNumberOfPagesUsed());
          sb.append(",size=");
          sb.append(((FspHdrXes) page).getFspHeader().getSize());
          sb.append(",xdes.size=");
          sb.append(((FspHdrXes) page).getXdesList().size());
        } else if (INODE.equals(page.pageType())) {
          sb.append(",inode.size=");
          sb.append(((Inode) page).getInodeEntryList().size());
        } else if (INDEX.equals(page.pageType())) {
          // && !((Index) page).isLeafPage()
          if (((Index) page).isRootPage()) {
            sb.append(",root.page=true");
          }
          sb.append(",index.id=");
          sb.append(((Index) page).getIndexHeader().getIndexId());
          sb.append(",level=");
          sb.append(((Index) page).getIndexHeader().getPageLevel());
          sb.append(",numOfRecs=");
          sb.append(((Index) page).getIndexHeader().getNumOfRecs());
          sb.append(",num.dir.slot=");
          sb.append(((Index) page).getIndexHeader().getNumOfDirSlots());
          sb.append(",garbage.space=");
          sb.append(((Index) page).getIndexHeader().getGarbageSpace());
        }
        // else {
        //continue;
        // }
        System.out.println(sb.toString());
      }
    }
  }

  private static void showPages(String ibdFilePath, String createTableSql, List<Long> pageNumberList, boolean jsonStyle, boolean jsonPrettyStyle) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      for (Long pageNumber : pageNumberList) {
        AbstractPage page = reader.readPage(pageNumber);
        if (jsonStyle) {
          System.out.println(JSON_MAPPER.toJson(page));
        } else if (jsonPrettyStyle) {
          System.out.println(JSON_MAPPER.toPrettyJson(page));
        } else {
          System.out.println(page);
        }
      }
    }
  }

  private static void getAllIndexPageFillingRate(String ibdFilePath, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      System.out.println("Number of pages is " + reader.getNumOfPages());
      System.out.println("Index page filling rate is " + reader.getAllIndexPageFillingRate());
    }
  }

  private static void showHeaderIfSet(TableReader reader) {
    if (SHOW_HEADER) {
      Schema schema = reader.getSchema();
      System.out.println(schema.getColumnNames().stream().collect(Collectors.joining(",")));
    }
  }

}
