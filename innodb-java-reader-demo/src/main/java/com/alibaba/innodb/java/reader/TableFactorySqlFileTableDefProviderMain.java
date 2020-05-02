package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlFileTableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlTableDefProvider;

import java.util.List;

/**
 * @author xu.zx
 */
public class TableFactorySqlFileTableDefProviderMain {

  public static void main(String[] args) {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "/path/sample_table.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath("/usr/local/mysql/data/test/")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("t");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assert recordList.size() == 10;
    } finally {
      reader.close();
    }
  }

}
