package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SimpleTableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlTableDefProvider;

import java.util.List;

/**
 * @author xu.zx
 */
public class TableFactorySqlTableDefProviderMain {

  public static void main(String[] args) {
    String createTableSql = "CREATE TABLE `tb11`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "`b` varchar(64) NOT NULL,\n"
        + "PRIMARY KEY (`id`),\n"
        + "KEY `key_a` (`a`))\n"
        + "ENGINE=InnoDB;";

    TableDefProvider tableDefProvider = new SqlTableDefProvider(createTableSql);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath("/usr/local/mysql/data/test/")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb11");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assert recordList.size() == 10;
    } finally {
      reader.close();
    }
  }

}
