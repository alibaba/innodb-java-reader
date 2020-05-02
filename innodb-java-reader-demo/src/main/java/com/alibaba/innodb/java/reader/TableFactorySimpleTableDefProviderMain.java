package com.alibaba.innodb.java.reader;

import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SimpleTableDefProvider;

import java.util.List;

/**
 * @author xu.zx
 */
public class TableFactorySimpleTableDefProviderMain {

  public static void main(String[] args) {
    TableDef tableDef = new TableDef().setName("t")
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false));

    TableDefProvider tableDefProvider = new SimpleTableDefProvider(tableDef);
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
