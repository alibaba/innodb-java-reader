package com.alibaba.innodb.java.reader;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.exception.ReaderException;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.TableDef;
import com.alibaba.innodb.java.reader.schema.provider.TableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SimpleTableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlFileTableDefProvider;
import com.alibaba.innodb.java.reader.schema.provider.impl.SqlTableDefProvider;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class TableReaderFactoryTest extends AbstractTest {

  private String createSql = "CREATE TABLE `tb01`\n"
      + "(`id` int(11) NOT NULL ,\n"
      + "`a` bigint(20) NOT NULL,\n"
      + "`b` varchar(64) NOT NULL,\n"
      + "`c` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
      + "PRIMARY KEY (`id`))\n"
      + "ENGINE=InnoDB;";

  private String createSql2 = "CREATE TABLE `test`.`tb01`\n"
      + "(`id` int(11) NOT NULL ,\n"
      + "`a` bigint(20) NOT NULL,\n"
      + "`b` varchar(64) NOT NULL,\n"
      + "`c` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
      + "PRIMARY KEY (`id`))\n"
      + "ENGINE=InnoDB;";

  private String createSql3 = "CREATE TABLE `test`.`tb001`\n"
      + "(`id` int(11) NOT NULL ,\n"
      + "`a` bigint(20) NOT NULL,\n"
      + "`b` varchar(64) NOT NULL,\n"
      + "`c` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
      + "PRIMARY KEY (`id`))\n"
      + "ENGINE=InnoDB;";

  private String wrongCreateSql2 = "CREATE TABLE ttt)\n"
      + "ENGINE=InnoDB;";

  private TableDef tableDef = new TableDef().setName("tb01")
      .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
      .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
      .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
      .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));

  private TableDef tableDef2 = new TableDef().setFullyQualifiedName("db.`tb01`")
      .addColumn(new Column().setName("id").setType("int(11)").setNullable(false))
      .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
      .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
      .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true))
      .setPrimaryKeyColumns(ImmutableList.of("id"));

  @Test
  public void testSimpleTableDefProviderNoSetFullQualifiedName1() {
    TableDefProvider tableDefProvider = new SimpleTableDefProvider(tableDef);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL56 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testSqlTableDefProviderNoSetFullQualifiedName1() {
    TableDefProvider tableDefProvider = new SqlTableDefProvider(createSql);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL56 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = ReaderException.class)
  public void testSqlTableDefProviderNoSetFullQualifiedName2() {
    TableDefProvider tableDefProvider = new SqlTableDefProvider(createSql2);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL56 + "simple")
        .build();
    // you can not search by full qualified name
    TableReader reader = tableReaderFactory.createTableReader("test.tb01");
  }

  @Test
  public void testSqlFileTableDefProvider() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = ReaderException.class)
  public void testSqlFileTableDefProviderTableNotExist() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("not_exist");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleTableDefProviderTableNegate() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableDefProvider tableDefProvider2 = new SqlTableDefProvider(createSql2);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withProvider(tableDefProvider2)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    // tb01 exist in more than one TableDefProvider
    TableReader reader = tableReaderFactory.createTableReader("tb01");
  }

  @Test
  public void testMultipleTableDefProviderTable() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableDefProvider tableDefProvider2 = new SqlTableDefProvider(createSql3);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withProvider(tableDefProvider2)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMultipleTableDefProviderTable2() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableDefProvider tableDefProvider2 = new SqlTableDefProvider(createSql3);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProviders(Arrays.asList(tableDefProvider, tableDefProvider2))
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testTableDefProviderTableWithSuffix() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .withDataFileSuffix(".ibd")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testGetAllTableDefMapTableDefProviderTable() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
    assertThat(tableReaderFactory.getTableNameToDefMap().size(), is(42));
    assertThat(tableReaderFactory.getTableDef("tb03").getName(), is("tb03"));
    assertThat(tableReaderFactory.getTableDef("ad_campaign").getName(), is("ad_campaign"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoTableDefProviderTableWithSuffix() {
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .withDataFileSuffix(".ibd")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoDataFilePathTableDefProviderTable() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileSuffix(".ibd")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = ReaderException.class)
  public void testDataFilePathNotExistTableDefProviderTable() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "wrong")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }

  @Test(expected = ReaderException.class)
  public void testWrongSqlTableDefProviderTable() {
    TableDefProvider tableDefProvider = new SqlTableDefProvider(wrongCreateSql2);
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFileBasePath(IBD_FILE_BASE_PATH_MYSQL80 + "simple")
        .build();
  }

  @Test
  public void testParseSqlWithCollate() {
    String sql = "CREATE TABLE ttt(id int) DEFAULT CHARSET = utf8 COLLATE = utf8_bin";
    TableDefProvider tableDefProvider = new SqlTableDefProvider(sql);
    tableDefProvider.load();
  }

  @Test
  public void testTableDefProviderTableWithExactDataFilePath() {
    TableDefProvider tableDefProvider = new SqlFileTableDefProvider(
        "src/test/resources/test.sql");
    TableReaderFactory tableReaderFactory = TableReaderFactory.builder()
        .withProvider(tableDefProvider)
        .withDataFilePath(IBD_FILE_BASE_PATH_MYSQL80 + "/simple/tb01.ibd")
        .build();
    TableReader reader = tableReaderFactory.createTableReader("tb01");
    try {
      reader.open();
      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(10));
    } finally {
      reader.close();
    }
  }
}
