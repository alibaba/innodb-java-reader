package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.column.ColumnType;
import com.alibaba.innodb.java.reader.exception.SqlParseException;

import org.junit.Test;

import java.util.List;

import static com.alibaba.innodb.java.reader.Constants.PRIMARY_KEY_NAME;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class TableDefUtilTest {

  @Test
  public void testConvert() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) unsigneD NOT NULL,\n"
        + "b tinyint DEFAULT 0 COMMENT 'comment here你好，：呵呵',\n"
        + "c text NOT NULL,\n"
        + "\"d\" datetime NOT NULL DEFAULT '9999-00-00 00:00:00',\n"
        + "`e` varchar(64) NOT NULL,\n"
        + "`f` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
        + "`g` timestamp(3) DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\n"
        + "`H` DECIMAL(16,8),\n"
        + "`i` decimal(12) NOT NULL,\n"
        + "`j` ENUM('A', 'B', 'Hello', '0xE4') NOT NULL,\n"
        + "`k` varchar(500) NOT NULL,\n"
        + "PRIMARY KEY (`id`),\n"
        + "CONSTRAINT `tb01_ibfk_1` FOREIGN KEY (c) REFERENCES ra_user(id),\n"
        + "key `key_d` (`d`), \n"
        + "UNIQUE key `key_e` (e),\n" //TODO key support COMMENT
        + "INDeX `key_b_e` (`b`, `e`), \n"
        + "KEY `key_k` (`k`(255)),\n"
        + "FULLTEXT KEY `fulltext_f` (`f`)\n"
        + ")ENGINE=InnoDB DEFAULT CHARSET =utf8mb4;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    assertThat(tableDef.getName(), is("tb01"));
    assertThat(tableDef.getFullyQualifiedName(), is("tb01"));
    assertThat(tableDef.getDefaultCharset(), is("utf8mb4"));
    assertThat(tableDef.getDefaultJavaCharset(), is("UTF-8"));
    assertThat(tableDef.getCollation(), is("utf8mb4_general_ci"));
    assertThat(tableDef.isCollationCaseSensitive(), is(false));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(12));
    assertThat(tableDef.getColumnNum(), is(12));

    assertThat(columnList.get(0).getOrdinal(), is(0));
    assertThat(columnList.get(0).getName(), is("id"));
    assertThat(columnList.get(0).getType(), is(ColumnType.INT));
    assertThat(columnList.get(0).getFullType(), is("int(11)"));
    assertThat(columnList.get(0).getLength(), is(11));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(false));

    assertThat(columnList.get(1).getOrdinal(), is(1));
    assertThat(columnList.get(1).getName(), is("a"));
    assertThat(columnList.get(1).getType(), is(ColumnType.BIGINT + " UNSIGNED"));
    assertThat(columnList.get(1).getFullType(), is("bigint(20) UNSIGNED"));
    assertThat(columnList.get(1).getLength(), is(20));
    assertThat(columnList.get(1).isPrimaryKey(), is(false));
    assertThat(columnList.get(1).isNullable(), is(false));

    assertThat(tableDef.getField("b").getColumn().getName(), is("b"));
    assertThat(tableDef.getField("b").getColumn().getType(), is(ColumnType.TINYINT));
    assertThat(tableDef.getField("b").getColumn().getFullType(),
        is("tinyint"));
    assertThat(tableDef.getField("b").getColumn().getLength(), is(0));
    assertThat(tableDef.getField("b").getColumn().getPrecision(), is(0));
    assertThat(tableDef.getField("b").getColumn().getScale(), is(0));
    assertThat(tableDef.getField("b").getColumn().isPrimaryKey(), is(false));
    assertThat(tableDef.getField("b").getColumn().isNullable(), is(true));

    assertThat(columnList.get(3).getOrdinal(), is(3));
    assertThat(columnList.get(3).getName(), is("c"));
    assertThat(columnList.get(3).getType(), is(ColumnType.TEXT));
    assertThat(columnList.get(3).getFullType(), is("text"));
    assertThat(columnList.get(3).getLength(), is(0));
    assertThat(columnList.get(3).getPrecision(), is(0));
    assertThat(columnList.get(3).getScale(), is(0));
    assertThat(columnList.get(3).isPrimaryKey(), is(false));
    assertThat(columnList.get(3).isNullable(), is(false));
    assertThat(columnList.get(3).getCharset(), is("utf8mb4"));
    assertThat(columnList.get(3).getJavaCharset(), is("UTF-8"));
    assertThat(columnList.get(3).getCollation(), is("utf8mb4_general_ci"));
    assertThat(columnList.get(3).isCollationCaseSensitive(), is(false));

    assertThat(columnList.get(4).getOrdinal(), is(4));
    assertThat(columnList.get(4).getName(), is("d"));
    assertThat(columnList.get(4).getType(), is(ColumnType.DATETIME));
    assertThat(columnList.get(4).getFullType(), is("datetime"));
    assertThat(columnList.get(4).getLength(), is(0));
    assertThat(columnList.get(4).getPrecision(), is(0));
    assertThat(columnList.get(4).getScale(), is(0));
    assertThat(columnList.get(4).isPrimaryKey(), is(false));
    assertThat(columnList.get(4).isNullable(), is(false));

    assertThat(columnList.get(5).getOrdinal(), is(5));
    assertThat(columnList.get(5).getName(), is("e"));
    assertThat(columnList.get(5).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(5).getFullType(), is("varchar(64)"));
    assertThat(columnList.get(5).getLength(), is(64));
    assertThat(columnList.get(5).getPrecision(), is(0));
    assertThat(columnList.get(5).getScale(), is(0));
    assertThat(columnList.get(5).isPrimaryKey(), is(false));
    assertThat(columnList.get(5).isNullable(), is(false));
    assertThat(columnList.get(5).getCharset(), is("utf8mb4"));

    assertThat(columnList.get(6).getOrdinal(), is(6));
    assertThat(columnList.get(6).getName(), is("f"));
    assertThat(columnList.get(6).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(6).getFullType(), is("varchar(1024)"));
    assertThat(columnList.get(6).getLength(), is(1024));
    assertThat(columnList.get(6).getPrecision(), is(0));
    assertThat(columnList.get(6).getScale(), is(0));
    assertThat(columnList.get(6).isPrimaryKey(), is(false));
    assertThat(columnList.get(6).isNullable(), is(true));
    assertThat(columnList.get(6).getCharset(), is("utf8mb4"));

    assertThat(tableDef.getField("g").getColumn().getName(), is("g"));
    assertThat(tableDef.getField("g").getColumn().getType(), is(ColumnType.TIMESTAMP));
    assertThat(tableDef.getField("g").getColumn().getFullType(), is("timestamp(3)"));
    assertThat(tableDef.getField("g").getColumn().getLength(), is(0));
    assertThat(tableDef.getField("g").getColumn().getPrecision(), is(3));
    assertThat(tableDef.getField("g").getColumn().getScale(), is(0));
    assertThat(tableDef.getField("g").getColumn().isPrimaryKey(), is(false));
    assertThat(tableDef.getField("g").getColumn().isNullable(), is(true));

    assertThat(columnList.get(8).getOrdinal(), is(8));
    assertThat(columnList.get(8).getName(), is("H"));
    assertThat(columnList.get(8).getType(), is(ColumnType.DECIMAL));
    assertThat(columnList.get(8).getFullType(), is(ColumnType.DECIMAL + "(16,8)"));
    assertThat(columnList.get(8).getLength(), is(0));
    assertThat(columnList.get(8).getPrecision(), is(16));
    assertThat(columnList.get(8).getScale(), is(8));
    assertThat(columnList.get(8).isPrimaryKey(), is(false));
    assertThat(columnList.get(8).isNullable(), is(true));

    assertThat(columnList.get(9).getOrdinal(), is(9));
    assertThat(columnList.get(9).getName(), is("i"));
    assertThat(columnList.get(9).getType(), is(ColumnType.DECIMAL));
    assertThat(columnList.get(9).getFullType(), is("decimal(12)"));
    assertThat(columnList.get(9).getLength(), is(0));
    assertThat(columnList.get(9).getPrecision(), is(12));
    assertThat(columnList.get(9).getScale(), is(0));
    assertThat(columnList.get(9).isPrimaryKey(), is(false));
    assertThat(columnList.get(9).isNullable(), is(false));

    assertThat(columnList.get(10).getOrdinal(), is(10));
    assertThat(columnList.get(10).getName(), is("j"));
    assertThat(columnList.get(10).getType(), is(ColumnType.ENUM));
    assertThat(columnList.get(10).getFullType(), is("ENUM('A','B','Hello','0xE4')"));
    assertThat(columnList.get(10).getLength(), is(0));
    assertThat(columnList.get(10).getPrecision(), is(0));
    assertThat(columnList.get(10).getScale(), is(0));
    assertThat(columnList.get(10).isPrimaryKey(), is(false));
    assertThat(columnList.get(10).isNullable(), is(false));

    assertThat(columnList.get(11).getOrdinal(), is(11));
    assertThat(columnList.get(11).getName(), is("k"));
    assertThat(columnList.get(11).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(11).getFullType(), is("varchar(500)"));
    assertThat(columnList.get(11).getLength(), is(500));
    assertThat(columnList.get(11).getPrecision(), is(0));
    assertThat(columnList.get(11).getScale(), is(0));
    assertThat(columnList.get(11).isPrimaryKey(), is(false));
    assertThat(columnList.get(11).isNullable(), is(false));

    assertThat(tableDef.getField("a").getColumn(), is(columnList.get(1)));
    assertThat(tableDef.getField("a").getOrdinal(), is(1));
    assertThat(tableDef.getField("H").getColumn(), is(columnList.get(8)));
    assertThat(tableDef.getField("H").getOrdinal(), is(8));

    assertThat(tableDef.getPrimaryKeyMeta().getName(), is(PRIMARY_KEY_NAME));
    assertThat(tableDef.getPrimaryKeyMeta().getType(), is(KeyMeta.Type.PRIMARY_KEY));
    assertThat(tableDef.getPrimaryKeyMeta().isSecondaryKey(), is(false));
    assertThat(tableDef.getPrimaryKeyColumns(), is(ImmutableList.of(columnList.get(0))));
    assertThat(tableDef.getPrimaryKeyColumnNum(), is(1));
    assertThat(tableDef.getPrimaryKeyColumnNames(), is(ImmutableList.of("id")));
    assertThat(tableDef.getPrimaryKeyVarLenColumns(), is(ImmutableList.of()));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames(), is(ImmutableList.of()));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(0)), is(true));
    for (int i = 1; i < 10; i++) {
      assertThat(tableDef.isColumnPrimaryKey(columnList.get(1)), is(false));
    }

    assertThat(tableDef.getSecondaryKeyMetaList().size(), is(6));
    assertThat(tableDef.getSecondaryKeyMetaMap().size(), is(6));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getType(), is(KeyMeta.Type.FOREIGN_KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getName(), is("tb01_ibfk_1"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getNumOfColumns(), is(0));

    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getType(), is(KeyMeta.Type.KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).isSecondaryKey(), is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).isSecondaryKey(), is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getName(), is("key_d"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getNumOfColumns(), is(1));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyColumns(),
        is(ImmutableList.of(columnList.get(4))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyColumnNames(),
        is(ImmutableList.of("d")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyVarLenColumns(),
        is(ImmutableList.of()));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyVarLenColumnNames(),
        is(ImmutableList.of()));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyVarLen(),
        is(ImmutableList.of()));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).containsColumn("d"),
        is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).containsColumn("e"),
        is(false));
    tableDef.getSecondaryKeyMetaList().get(1).validate();

    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getType(), is(KeyMeta.Type.UNIQUE_KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).isSecondaryKey(), is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getName(), is("key_e"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getNumOfColumns(), is(1));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyColumnNames(),
        is(ImmutableList.of("e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyVarLenColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyVarLenColumnNames(),
        is(ImmutableList.of("e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyVarLen(),
        is(ImmutableList.of(0)));

    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getType(), is(KeyMeta.Type.INDEX));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).isSecondaryKey(), is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getName(), is("key_b_e"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getNumOfColumns(), is(2));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getKeyColumns(),
        is(ImmutableList.of(columnList.get(2), columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getKeyColumnNames(),
        is(ImmutableList.of("b", "e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getKeyVarLenColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getKeyVarLenColumnNames(),
        is(ImmutableList.of("e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(3).getKeyVarLen(),
        is(ImmutableList.of(0)));

    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getType(), is(KeyMeta.Type.KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).isSecondaryKey(), is(true));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getName(), is("key_k"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getNumOfColumns(), is(1));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getKeyColumns(),
        is(ImmutableList.of(columnList.get(11))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getKeyColumnNames(),
        is(ImmutableList.of("k")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getKeyVarLenColumns(),
        is(ImmutableList.of(columnList.get(11))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getKeyVarLenColumnNames(),
        is(ImmutableList.of("k")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(4).getKeyVarLen(),
        is(ImmutableList.of(255)));

    assertThat(tableDef.getSecondaryKeyMetaList().get(5).getType(), is(KeyMeta.Type.FULLTEXT_KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(5).getName(), is("fulltext_f"));
  }

  @Test
  public void testConvertPrimaryKeyOnColumn1() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL PRIMARY KEY,\n"
        + "`a` bigint(20) unsigneD NOT NULL)\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = latin1;";
    testConvertPrimaryKeyOnColumn(sql);
  }

  @Test
  public void testConvertPrimaryKeyOnColumn2() {
    String sql = "create table `tb01`\n"
        + "(`id` int(11) not null key,\n"
        + "`a` bigint(20) unsigneD not null)\n"
        + "ENGINE=InnoDB default charset = latin1;";
    testConvertPrimaryKeyOnColumn(sql);
  }

  public void testConvertPrimaryKeyOnColumn(String sql) {
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    assertThat(tableDef.getDefaultCharset(), is("latin1"));
    assertThat(tableDef.getDefaultJavaCharset(), is("Cp1252"));
    assertThat(tableDef.getCollation(), is("latin1_swedish_ci"));
    assertThat(tableDef.isCollationCaseSensitive(), is(false));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(2));

    assertThat(columnList.get(0).getName(), is("id"));
    assertThat(columnList.get(0).getType(), is(ColumnType.INT));
    assertThat(columnList.get(0).getLength(), is(11));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(true));
    assertThat(columnList.get(0).isNullable(), is(false));

    assertThat(tableDef.getPrimaryKeyColumns(), is(ImmutableList.of(columnList.get(0))));
    assertThat(tableDef.getPrimaryKeyColumnNum(), is(1));
    assertThat(tableDef.getPrimaryKeyColumnNames(), is(ImmutableList.of("id")));
    assertThat(tableDef.getPrimaryKeyVarLenColumns(), is(ImmutableList.of()));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames(), is(ImmutableList.of()));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(0)), is(true));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(1)), is(false));

    assertThat(tableDef.getSecondaryKeyMetaList().isEmpty(), is(true));
  }

  @Test
  public void testConvertCompositePrimaryKey() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` int(11) NOT NULL,"
        + "`b` bigint(20) NOT NULL,"
        + "`c` varchar(20) NOT NULL,"
        + "`d` bigint(20) NOT NULL,"
        + "`e` int(11) NOT NULL,"
        + "PRIMARY KEY (`c`, b, e)\n"
        + ")\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = latin1;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(6));

    assertThat(tableDef.getPrimaryKeyColumns(), is(ImmutableList.of(
        tableDef.getField("c").getColumn(),
        tableDef.getField("b").getColumn(),
        tableDef.getField("e").getColumn()
    )));
    assertThat(tableDef.getPrimaryKeyColumnNum(), is(3));
    assertThat(tableDef.getPrimaryKeyColumnNames(), is(ImmutableList.of("c", "b", "e")));
    assertThat(tableDef.getPrimaryKeyVarLenColumns(),
        is(ImmutableList.of(tableDef.getField("c").getColumn())));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames(), is(ImmutableList.of("c")));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(0)), is(false));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(1)), is(false));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(2)), is(true));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(3)), is(true));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(4)), is(false));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(5)), is(true));
  }

  @Test
  public void testConvertNoPk() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) unsigneD NOT NULL,\n"
        + "b tinyint DEFAULT 0 COMMENT 'comment here',\n"
        + "c text NOT NULL,\n"
        + "d datetime NOT NULL,\n"
        + "`e` varchar(64) NOT NULL,\n"
        + "`f` varchar(1024) default 'THIS_IS_DEFAULT_VALUE')\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(7));

    assertThat(tableDef.getPrimaryKeyColumnNum(), is(0));
    assertThat(tableDef.getPrimaryKeyColumnNames().isEmpty(), is(true));
    assertThat(tableDef.getPrimaryKeyVarLenColumns().isEmpty(), is(true));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames().isEmpty(), is(true));
    for (int i = 0; i < 7; i++) {
      assertThat(tableDef.isColumnPrimaryKey(columnList.get(i)), is(false));
    }
  }

  @Test(expected = SqlParseException.class)
  public void testConvertPrimaryKeyNotExistColumn() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "PRIMARY KEY (b, a))\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8;";
    TableDefUtil.covertToTableDef(sql);
  }

  @Test(expected = SqlParseException.class)
  public void testConvertPrimaryKeyNoColumn() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "PRIMARY KEY ())\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8;";
    TableDefUtil.covertToTableDef(sql);
  }

  @Test
  public void testConvertPrimaryKeyWithName() {
    String sql = "CREATE TABLE `test2` (\n"
        + "  `key` int(11) NOT NULL, \n"
        + "  `value` varchar(96) DEFAULT NULL, \n"
        + "PRIMARY KEY `PRIMARY` (`key`)\n"
        + ") ENGINE=InnoDB;";
    TableDefUtil.covertToTableDef(sql);
  }

  @Test
  public void testConvertColumnCharsetAndCollate() {
    String sql = "CREATE TABLE t (c CHAR(20) CHARACTER SET utf8 COLLATE utf8_bin);";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    // by default set to utf8
    assertThat(tableDef.getDefaultCharset(), is("utf8"));
    assertThat(tableDef.getDefaultJavaCharset(), is("UTF-8"));
    assertThat(tableDef.getCollation(), is("utf8_general_ci"));
    assertThat(tableDef.isCollationCaseSensitive(), is(false));

    assertThat(tableDef.getPrimaryKeyColumns().isEmpty(), is(true));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(1));

    assertThat(columnList.get(0).getName(), is("c"));
    assertThat(columnList.get(0).getType(), is(ColumnType.CHAR));
    assertThat(columnList.get(0).getLength(), is(20));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(true));
    assertThat(columnList.get(0).getCharset(), is("utf8"));
    assertThat(columnList.get(0).getJavaCharset(), is("UTF-8"));
    assertThat(columnList.get(0).getCollation(), is("utf8_bin"));
    assertThat(columnList.get(0).isCollationCaseSensitive(), is(true));
  }

  @Test
  public void testConvertColumnCharsetAndCollate2() {
    String sql = "CREATE TABLE t (c varchar(20) CHARSET gbk COLLATE gb2312_chinese_ci) "
        + "ENGINE=InnoDB DEFAULT CHARSET=utf32 COLLATE utf32_bin;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    // by default set to utf8
    assertThat(tableDef.getDefaultCharset(), is("utf32"));
    assertThat(tableDef.getDefaultJavaCharset(), is("UTF-32"));
    assertThat(tableDef.getCollation(), is("utf32_bin"));
    assertThat(tableDef.isCollationCaseSensitive(), is(true));

    assertThat(tableDef.getPrimaryKeyColumns().isEmpty(), is(true));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(1));

    assertThat(columnList.get(0).getName(), is("c"));
    assertThat(columnList.get(0).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(0).getFullType(), is("varchar(20)"));
    assertThat(columnList.get(0).getLength(), is(20));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(true));
    assertThat(columnList.get(0).getCharset(), is("gbk"));
    assertThat(columnList.get(0).getJavaCharset(), is("GBK"));
    assertThat(columnList.get(0).getCollation(), is("gb2312_chinese_ci"));
    assertThat(columnList.get(0).isCollationCaseSensitive(), is(false));
  }

  @Test
  public void testConvertColumnCharsetAndCollate3() {
    String sql = "CREATE TABLE t (c varchar(20)) "
        + "ENGINE=InnoDB DEFAULT CHARSET=latin2 COLLATE latin2_bin;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    // by default set to utf8
    assertThat(tableDef.getDefaultCharset(), is("latin2"));
    assertThat(tableDef.getDefaultJavaCharset(), is("ISO8859_2"));
    assertThat(tableDef.getCollation(), is("latin2_bin"));
    assertThat(tableDef.isCollationCaseSensitive(), is(true));

    assertThat(tableDef.getPrimaryKeyColumns().isEmpty(), is(true));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(1));

    assertThat(columnList.get(0).getName(), is("c"));
    assertThat(columnList.get(0).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(0).getFullType(), is("varchar(20)"));
    assertThat(columnList.get(0).getLength(), is(20));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(true));
    assertThat(columnList.get(0).getCharset(), is("latin2"));
    assertThat(columnList.get(0).getJavaCharset(), is("ISO8859_2"));
    assertThat(columnList.get(0).getCollation(), is("latin2_bin"));
    // use table default collation
    assertThat(columnList.get(0).isCollationCaseSensitive(), is(true));
  }

  @Test(expected = SqlParseException.class)
  public void testConvertUnsupportedColumnType() {
    String sql = "CREATE TABLE employees (\n"
        + "    emp_no      INT             NOT NULL,  -- UNSIGNED AUTO_INCREMENT??\n"
        + "    birth_date  DATE            NOT NULL,\n"
        + "    first_name  VARCHAR(14)     NOT NULL,\n"
        + "    last_name   VARCHAR(16)     NOT NULL,\n"
        + "    gender      UNKNOWN ('M','F')  NOT NULL,  -- Enumeration of either 'M' or 'F'  \n"
        + "    hire_date   DATE            NOT NULL,\n"
        + "    PRIMARY KEY (emp_no)                   -- Index built automatically on primary-key column\n"
        + "                                           -- INDEX (first_name)\n"
        + "                                           -- INDEX (last_name)\n"
        + ");";
    TableDefUtil.covertToTableDef(sql);
  }

  @Test(expected = SqlParseException.class)
  public void testConvertNegativeNoColumn() {
    String sql = "CREATE TABLE `tb01`\n"
        + "()\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    TableDefUtil.covertToTableDef(sql);
  }

  @Test
  public void testTableName() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "PRIMARY KEY (a))\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    assertThat(tableDef.getName(), is("tb01"));
    assertThat(tableDef.getFullyQualifiedName(), is("tb01"));
  }

  @Test
  public void testTableFullyQualifiedName() {
    String sql = "CREATE TABLE `db`.`tb01`\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "PRIMARY KEY (a))\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    assertThat(tableDef.getName(), is("tb01"));
    assertThat(tableDef.getFullyQualifiedName(), is("db.tb01"));

    sql = "CREATE TABLE test.tb02\n"
        + "(`id` int(11) NOT NULL,\n"
        + "`a` bigint(20) NOT NULL,\n"
        + "PRIMARY KEY (a))\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8;";
    tableDef = TableDefUtil.covertToTableDef(sql);
    assertThat(tableDef.getName(), is("tb02"));
    assertThat(tableDef.getFullyQualifiedName(), is("test.tb02"));
  }

  @Test
  public void testMakeUniqueKeyAsPrimaryKey() {
    String sql = "CREATE TABLE `type_newdecimaltest59` (\n"
        + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"
        + "  `d` decimal(65,30) DEFAULT NULL,\n"
        + "  UNIQUE KEY `key_id` (`id`)\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    // have to prepare
    tableDef.prepare();
    System.out.println(tableDef);

    assertThat(tableDef.getName(), is("type_newdecimaltest59"));
    assertThat(tableDef.getFullyQualifiedName(), is("type_newdecimaltest59"));
    assertThat(tableDef.getDefaultCharset(), is("utf8"));
    assertThat(tableDef.getDefaultJavaCharset(), is("UTF-8"));
    assertThat(tableDef.getCollation(), is("utf8_general_ci"));
    assertThat(tableDef.isCollationCaseSensitive(), is(false));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(2));
    assertThat(tableDef.getColumnNum(), is(2));

    assertThat(columnList.get(0).getOrdinal(), is(0));
    assertThat(columnList.get(0).getName(), is("id"));
    assertThat(columnList.get(0).getType(), is(ColumnType.UNSIGNED_BIGINT));
    assertThat(columnList.get(0).getFullType(), is("bigint(20) UNSIGNED"));
    assertThat(columnList.get(0).getLength(), is(20));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(false));

    assertThat(columnList.get(1).getOrdinal(), is(1));
    assertThat(columnList.get(1).getName(), is("d"));
    assertThat(columnList.get(1).getType(), is(ColumnType.DECIMAL));
    assertThat(columnList.get(1).getFullType(), is("decimal(65,30)"));
    assertThat(columnList.get(1).getLength(), is(0));
    assertThat(columnList.get(1).getPrecision(), is(65));
    assertThat(columnList.get(1).getScale(), is(30));
    assertThat(columnList.get(1).isPrimaryKey(), is(false));
    assertThat(columnList.get(1).isNullable(), is(true));

    assertThat(tableDef.getPrimaryKeyMeta().getName(), is("key_id"));
    assertThat(tableDef.getPrimaryKeyMeta().getType(), is(KeyMeta.Type.PRIMARY_KEY));
    assertThat(tableDef.getPrimaryKeyMeta().isSecondaryKey(), is(false));
    assertThat(tableDef.getPrimaryKeyColumns(), is(ImmutableList.of(columnList.get(0))));
    assertThat(tableDef.getPrimaryKeyColumnNum(), is(1));
    assertThat(tableDef.getPrimaryKeyColumnNames(), is(ImmutableList.of("id")));
    assertThat(tableDef.getPrimaryKeyVarLenColumns(), is(ImmutableList.of()));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames(), is(ImmutableList.of()));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(0)), is(true));
    for (int i = 1; i < 10; i++) {
      assertThat(tableDef.isColumnPrimaryKey(columnList.get(1)), is(false));
    }

    assertThat(tableDef.getSecondaryKeyMetaList().size(), is(0));
    assertThat(tableDef.getSecondaryKeyMetaMap().size(), is(0));
  }

}
