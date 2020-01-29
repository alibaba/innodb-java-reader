/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.cli.writer.SysoutWriter;
import com.alibaba.innodb.java.reader.cli.writer.Writer;
import com.alibaba.innodb.java.reader.cli.writer.WriterFactory;
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

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class InnodbReaderBootstrap {

  private static final String JAR_NAME = "innodb-java-reader-cli.jar";

  private static String ALL_COMMANDS = Arrays.stream(CommandType.values()).map(CommandType::getType).collect(Collectors.joining(","));

  private static String ALL_OUTPUT_IO_MODE = Arrays.stream(OutputIOMode.values()).map(OutputIOMode::getMode).collect(Collectors.joining(","));

  private static final JsonMapper JSON_MAPPER = JsonMapper.buildNormalMapper();

  private static boolean SHOW_HEADER = false;

  private static String FIELD_DELIMITER = "\t";

  public static void main(String[] arguments) {
    CommandLineParser parser = new DefaultParser();

    Options options = new Options();
    options.addOption("h", "help", false, "usage");
    options.addOption("i", "ibd-file-path", true, "mandatory. innodb file path with suffix of .ibd");
    options.addOption("c", "command", true, "mandatory. command to run, valid commands are: " + ALL_COMMANDS);
    options.addOption("s", "create-table-sql-file-path", true, "create table sql file path by running SHOW CREATE TABLE <table_name>");
    options.addOption("json", "json-style", false, "set to true if you would like to show page info in json format style");
    options.addOption("jsonpretty", "json-pretty-style", false, "set to true if you would like to show page info in json pretty format style");
    options.addOption("showheader", "show-header", false, "set to true if you want to show table header when dumping table");
    options.addOption("o", "output", true, "save result to file instead of console, the argument is the file path");
    options.addOption("iomode", "output-io-mode", true, "output io mode, valid mode are: " + ALL_OUTPUT_IO_MODE);
    options.addOption("delimiter", "delimiter", true, "field delimiter, default is tab");
    options.addOption("args", true, "arguments");

    String command = null;
    String ibdFilePath = null;
    String createTableSql = null;
    String outputFilePath;
    String args = null;
    // show all pages or show one page
    boolean jsonStyle = false;
    boolean jsonPrettyStyle = false;
    Writer writer = new SysoutWriter();
    writer.open();

    try {
      CommandLine line = parser.parse(options, arguments);

      if (line.hasOption("help")) {
        showHelp(options, 0);
      }

      if (line.hasOption("ibd-file-path")) {
        ibdFilePath = line.getOptionValue("ibd-file-path");
        Preconditions.checkArgument(StringUtils.isNotEmpty(ibdFilePath), "ibd-file-path is empty");
      } else {
        log.error("please input ibd-file-path");
        showHelp(options, 1);
      }

      if (line.hasOption("create-table-sql-file-path")) {
        String createTableSqlFilePath = line.getOptionValue("create-table-sql-file-path");
        Preconditions.checkArgument(StringUtils.isNotEmpty(createTableSqlFilePath), "create-table-sql-file-path is empty");
        List<String> lines = Files.readLines(new File(createTableSqlFilePath), Charset.defaultCharset());
        createTableSql = String.join(" ", lines);
      } else {
        log.error("please input create-table-sql");
        showHelp(options, 1);
      }

      if (line.hasOption("command")) {
        command = line.getOptionValue("command");
      } else {
        log.error("please input command to run, cmd=" + ALL_COMMANDS);
        showHelp(options, 1);
      }

      if (line.hasOption("args")) {
        args = line.getOptionValue("args");
      }
      if (line.hasOption("output")) {
        outputFilePath = line.getOptionValue("output");
        OutputIOMode outputIOMode = OutputIOMode.parse(line.getOptionValue("output-io-mode"));
        writer = WriterFactory.build(outputIOMode, outputFilePath);
        writer.open();
      }
      if (line.hasOption("delimiter")) {
        FIELD_DELIMITER = line.getOptionValue("delimiter");
      } else {
        FIELD_DELIMITER = "\t";
      }

      if (line.hasOption("json-style")) {
        jsonStyle = true;
      }
      if (line.hasOption("json-pretty-style")) {
        jsonPrettyStyle = true;
      }
      if (line.hasOption("show-header")) {
        SHOW_HEADER = true;
      } else {
        SHOW_HEADER = false;
      }

      CommandType commandType = EnumUtils.getEnum(CommandType.class, command.replace("-", "_").toUpperCase());

      if (commandType == null) {
        log.error("invalid command type, should be " + ALL_COMMANDS);
        showHelp(options, 1);
      }

      checkNotNull(commandType);
      switch (commandType) {
        case SHOW_ALL_PAGES:
          showAllPages(ibdFilePath, writer, createTableSql);
          break;
        case SHOW_PAGES:
          checkNotNull(args, "args should not be null");
          List<Long> pageNumbers = Stream.of(args.split(",")).map(Long::parseLong).collect(toList());
          showPages(ibdFilePath, writer, createTableSql, pageNumbers, jsonStyle, jsonPrettyStyle);
          break;
        case QUERY_ALL:
          queryAll(ibdFilePath, writer, createTableSql);
          break;
        case QUERY_BY_PAGE_NUMBER:
          checkNotNull(args, "args should not be null");
          long pageNumber = Long.parseLong(args);
          queryByPageNumber(ibdFilePath, writer, createTableSql, pageNumber);
          break;
        case QUERY_BY_PK:
          checkNotNull(args, "args should not be null");
          queryByPrimaryKey(ibdFilePath, writer, createTableSql, args);
          break;
        case RANGE_QUERY_BY_PK:
          checkNotNull(args, "args should not be null");
          List<String> range = Stream.of(args.split("[, ]")).collect(toList());
          checkState(range != null && range.size() == 2, "argument number should not exactly two");
          Object lowerInclusiveKey = "null".equalsIgnoreCase(range.get(0)) ? null : range.get(0);
          Object upperExclusiveKey = "null".equalsIgnoreCase(range.get(1)) ? null : range.get(1);
          rangeQueryByPrimaryKey(ibdFilePath, writer, createTableSql, lowerInclusiveKey, upperExclusiveKey);
          break;
        case GEN_LSN_HEATMAP:
          genHeatmap(ibdFilePath, createTableSql, args, commandType);
          break;
        case GEN_FILLING_RATE_HEATMAP:
          genHeatmap(ibdFilePath, createTableSql, args, commandType);
          break;
        case GET_ALL_INDEX_PAGE_FILLING_RATE:
          getAllIndexPageFillingRate(ibdFilePath, writer, createTableSql);
          break;
        default:
          log.error("invalid command type, cmd=" + ALL_COMMANDS);
          showHelp(options, 1);
      }
    } catch (ParseException e) {
      log.error("Parse error occurred: " + e.getMessage(), e);
    } catch (IOException e) {
      log.error("IO error occurred: " + e.getMessage(), e);
    } catch (Exception e) {
      log.error("Error occurred: " + e.getMessage(), e);
    } finally {
      try {
        Utils.close(writer);
      } catch (IOException e) {
        log.error("Close writer failed: " + e.getMessage(), e);
      }
    }
  }

  private static void queryAll(String ibdFilePath, Writer writer, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader, writer);
      Iterator<GenericRecord> iterator = reader.getQueryAllIterator();
      StringBuilder b = new StringBuilder();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        writer.write(Utils.arrayToString(record.getValues(), b, FIELD_DELIMITER, writer.ifNewLineAfterWrite()));
      }
    }
  }

  private static void queryByPageNumber(String ibdFilePath, Writer writer, String createTableSql, long pageNumber) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader, writer);
      List<GenericRecord> recordList = reader.queryByPageNumber(pageNumber);
      StringBuilder b = new StringBuilder();
      if (CollectionUtils.isNotEmpty(recordList)) {
        for (GenericRecord record : recordList) {
          writer.write(Utils.arrayToString(record.getValues(), b, FIELD_DELIMITER, writer.ifNewLineAfterWrite()));
        }
      }
    }
  }

  private static void queryByPrimaryKey(String ibdFilePath, Writer writer, String createTableSql, String primaryKey) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader, writer);
      GenericRecord record = reader.queryByPrimaryKey(primaryKey);
      StringBuilder b = new StringBuilder();
      if (record != null) {
        writer.write(Utils.arrayToString(record.getValues(), b, FIELD_DELIMITER, writer.ifNewLineAfterWrite()));
      }
    }
  }

  private static void rangeQueryByPrimaryKey(String ibdFilePath, Writer writer, String createTableSql, Object lowerInclusiveKey, Object upperExclusiveKey) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      showHeaderIfSet(reader, writer);
      Iterator<GenericRecord> iterator = reader.getRangeQueryIterator(lowerInclusiveKey, upperExclusiveKey);
      StringBuilder b = new StringBuilder();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        writer.write(Utils.arrayToString(record.getValues(), b, FIELD_DELIMITER, writer.ifNewLineAfterWrite()));
      }
    }
  }

  private static void showHelp(Options options, int exitCode) {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp("java -jar " + JAR_NAME, options, true);
    System.exit(exitCode);
  }

  private static void genHeatmap(String ibdFilePath, String createTableSql, String args, CommandType commandType) throws IOException, TemplateException {
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

  private static void genLsnHeatmap(String ibdFilePath, String createTableSql, String destHtmlPath, Optional<Pair<String, String>> widthAndHeight)
      throws IOException, TemplateException {
    GenLsnHeatmapUtil.dump(ibdFilePath, destHtmlPath, createTableSql, 64, widthAndHeight);
  }

  private static void genFillingRateHeatmap(String ibdFilePath, String createTableSql, String destHtmlPath, Optional<Pair<String, String>> widthAndHeight)
      throws IOException, TemplateException {
    GenFillingRateHeatmapUtil.dump(ibdFilePath, destHtmlPath, createTableSql, 64, widthAndHeight);
  }

  private static void showAllPages(String ibdFilePath, Writer writer, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      Iterator<AbstractPage> iterator = reader.getPageIterator();
      writer.write(StringUtils.repeat("=", 5) + "page number, page type, other info" + StringUtils.repeat("=", 5));
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
        writer.write(sb.toString());
      }
    }
  }

  private static void showPages(String ibdFilePath, Writer writer, String createTableSql, List<Long> pageNumberList, boolean jsonStyle, boolean jsonPrettyStyle) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      for (Long pageNumber : pageNumberList) {
        AbstractPage page = reader.readPage(pageNumber);
        if (jsonStyle) {
          writer.write(JSON_MAPPER.toJson(page));
        } else if (jsonPrettyStyle) {
          writer.write(JSON_MAPPER.toPrettyJson(page));
        } else {
          writer.write(page.toString());
        }
      }
    }
  }

  private static void getAllIndexPageFillingRate(String ibdFilePath, Writer writer, String createTableSql) {
    try (TableReader reader = new TableReader(ibdFilePath, createTableSql)) {
      reader.open();
      writer.write("Number of pages is " + reader.getNumOfPages());
      writer.write("Index page filling rate is " + reader.getAllIndexPageFillingRate());
    }
  }

  private static void showHeaderIfSet(TableReader reader, Writer writer) {
    if (SHOW_HEADER) {
      Schema schema = reader.getSchema();
      writer.write(schema.getColumnNames().stream().collect(Collectors.joining(FIELD_DELIMITER)));
      if (writer.ifNewLineAfterWrite()) {
        writer.write("\n");
      }
    }
  }

}
