package com.alibaba.innodb.java.reader.heatmap;

import com.alibaba.innodb.java.reader.util.Pair;

import freemarker.template.TemplateException;

import java.io.IOException;
import java.util.Optional;


/**
 * @author xu.zx
 */
public class GenFillingRateHeatmapMainTest {

  public static void main(String[] args) throws IOException, TemplateException {
    //String ibdFilePath = "/Users/xu/IdeaProjects/innodb-java-reader/innodb-java-reader/src/
    // test/resources/testsuite/mysql56/simple/tb01.ibd";
    String sourceIbdFilePath = "/Users/xu/IdeaProjects/innodb-java-reader/innodb-java-reader"
        + "/src/test/resources/testsuite/mysql56/multiple/level/tb11.ibd";
    //String ibdFilePath = "/usr/local/mysql/data/test/tb_pk_only.ibd";
    //String ibdFilePath = "/usr/local/mysql/data/test/tb_secondary_index.ibd";
    String createTableSql = "CREATE TABLE `tb11`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "`b` varchar(64) NOT NULL,\n"
        + "`c` varchar(1024) NOT NULL,\n"
        + "PRIMARY KEY (`id`))\n"
        + "ENGINE=InnoDB;";
    String destHtmlFilePath = "/tmp/filling-rate-heatmap.html";
    int pageWrapNum = 64;

    GenFillingRateHeatmapUtil.dump(sourceIbdFilePath, destHtmlFilePath, createTableSql,
        pageWrapNum, Optional.of(new Pair<>("800", "500")));
  }

}
