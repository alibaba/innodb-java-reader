package com.alibaba.innodb.java.reader.heatmap;

import com.google.common.io.Files;

import freemarker.template.TemplateException;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class HeatmapTest {

  private String sourceIbdFilePath = "../innodb-java-reader/src/test/resources/testsuite/mysql57/multiple/level/tb10.ibd";

  private String createTableSql = "CREATE TABLE `tb10`\n"
      + "(`id` int(11) NOT NULL ,\n"
      + "`a` bigint(20) NOT NULL,\n"
      + "`b` varchar(64) NOT NULL,\n"
      + "`c` varchar(1024) NOT NULL,\n"
      + "PRIMARY KEY (`id`))\n"
      + "ENGINE=InnoDB;";

  @Test
  public void testGenLsnHeatmap() throws IOException, TemplateException {
    String destHtmlFilePath = "/tmp/lsn-heatmap.html";
    int pageWrapNum = 64;
    GenLsnHeatmapUtil.dump(sourceIbdFilePath, destHtmlFilePath, createTableSql, pageWrapNum, Optional.of(new Pair<>("800", "500")));
    List<String> fileContent = Files.readLines(new File(destHtmlFilePath), Charset.defaultCharset());
    assertThat(fileContent.get(0), is("<head>"));
    assertThat(fileContent.get(1054).trim(), is("97"));
  }

  @Test
  public void testGenFillingRateHeatmap() throws IOException, TemplateException {
    String destHtmlFilePath = "/tmp/filling-rate-heatmap.html";
    int pageWrapNum = 64;
    GenFillingRateHeatmapUtil.dump(sourceIbdFilePath, destHtmlFilePath, createTableSql, pageWrapNum, Optional.of(new Pair<>("800", "500")));
    List<String> fileContent = Files.readLines(new File(destHtmlFilePath), Charset.defaultCharset());
    assertThat(fileContent.get(0), is("<head>"));
    assertThat(fileContent.get(1067).trim(), is("0.466"));
  }

}
