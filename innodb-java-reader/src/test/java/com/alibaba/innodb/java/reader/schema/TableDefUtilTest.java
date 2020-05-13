package com.alibaba.innodb.java.reader.schema;

import com.google.common.collect.ImmutableList;

import com.alibaba.innodb.java.reader.column.ColumnType;
import com.alibaba.innodb.java.reader.exception.SqlParseException;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
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
        + "`h` decimal(16,8),\n"
        + "`i` decimal(12) NOT NULL,\n"
        + "PRIMARY KEY (`id`),\n"
        + "FOREIGN KEY (c) REFERENCES ra_user(id),\n"
        + "KEY `ddd` (`d`), \n"
        + "UNIQUE key (e) COMMENT 'feed',\n"
        + "INDEX `eee` (`b`, `e`) \n"
        + ")ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    assertThat(tableDef.getName(), is("tb01"));
    assertThat(tableDef.getFullyQualifiedName(), is("tb01"));
    assertThat(tableDef.getDefaultCharset(), is("utf8mb4"));

    List<Column> columnList = tableDef.getColumnList();
    assertThat(columnList.size(), is(10));
    assertThat(tableDef.getColumnNum(), is(10));

    assertThat(columnList.get(0).getName(), is("id"));
    assertThat(columnList.get(0).getType(), is(ColumnType.INT));
    assertThat(columnList.get(0).getLength(), is(11));
    assertThat(columnList.get(0).getPrecision(), is(0));
    assertThat(columnList.get(0).getScale(), is(0));
    assertThat(columnList.get(0).isPrimaryKey(), is(false));
    assertThat(columnList.get(0).isNullable(), is(false));

    assertThat(columnList.get(1).getName(), is("a"));
    assertThat(columnList.get(1).getType(), is(ColumnType.BIGINT + " UNSIGNED"));
    assertThat(columnList.get(1).getLength(), is(20));
    assertThat(columnList.get(1).isPrimaryKey(), is(false));
    assertThat(columnList.get(1).isNullable(), is(false));

    assertThat(tableDef.getField("b").getColumn().getName(), is("b"));
    assertThat(tableDef.getField("b").getColumn().getType(), is(ColumnType.TINYINT));
    assertThat(tableDef.getField("b").getColumn().getLength(), is(0));
    assertThat(tableDef.getField("b").getColumn().getPrecision(), is(0));
    assertThat(tableDef.getField("b").getColumn().getScale(), is(0));
    assertThat(tableDef.getField("b").getColumn().isPrimaryKey(), is(false));
    assertThat(tableDef.getField("b").getColumn().isNullable(), is(true));

    assertThat(columnList.get(3).getName(), is("c"));
    assertThat(columnList.get(3).getType(), is(ColumnType.TEXT));
    assertThat(columnList.get(3).getLength(), is(0));
    assertThat(columnList.get(3).getPrecision(), is(0));
    assertThat(columnList.get(3).getScale(), is(0));
    assertThat(columnList.get(3).isPrimaryKey(), is(false));
    assertThat(columnList.get(3).isNullable(), is(false));
    assertThat(columnList.get(3).getCharset(), is("utf8mb4"));

    assertThat(columnList.get(4).getName(), is("d"));
    assertThat(columnList.get(4).getType(), is(ColumnType.DATETIME));
    assertThat(columnList.get(4).getLength(), is(0));
    assertThat(columnList.get(4).getPrecision(), is(0));
    assertThat(columnList.get(4).getScale(), is(0));
    assertThat(columnList.get(4).isPrimaryKey(), is(false));
    assertThat(columnList.get(4).isNullable(), is(false));

    assertThat(columnList.get(5).getName(), is("e"));
    assertThat(columnList.get(5).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(5).getLength(), is(64));
    assertThat(columnList.get(5).getPrecision(), is(0));
    assertThat(columnList.get(5).getScale(), is(0));
    assertThat(columnList.get(5).isPrimaryKey(), is(false));
    assertThat(columnList.get(5).isNullable(), is(false));
    assertThat(columnList.get(5).getCharset(), is("utf8mb4"));

    assertThat(columnList.get(6).getName(), is("f"));
    assertThat(columnList.get(6).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(6).getLength(), is(1024));
    assertThat(columnList.get(6).getPrecision(), is(0));
    assertThat(columnList.get(6).getScale(), is(0));
    assertThat(columnList.get(6).isPrimaryKey(), is(false));
    assertThat(columnList.get(6).isNullable(), is(true));
    assertThat(columnList.get(6).getCharset(), is("utf8mb4"));

    assertThat(tableDef.getField("g").getColumn().getName(), is("g"));
    assertThat(tableDef.getField("g").getColumn().getType(), is(ColumnType.TIMESTAMP));
    assertThat(tableDef.getField("g").getColumn().getLength(), is(0));
    assertThat(tableDef.getField("g").getColumn().getPrecision(), is(3));
    assertThat(tableDef.getField("g").getColumn().getScale(), is(0));
    assertThat(tableDef.getField("g").getColumn().isPrimaryKey(), is(false));
    assertThat(tableDef.getField("g").getColumn().isNullable(), is(true));

    assertThat(columnList.get(8).getName(), is("h"));
    assertThat(columnList.get(8).getType(), is(ColumnType.DECIMAL));
    assertThat(columnList.get(8).getLength(), is(0));
    assertThat(columnList.get(8).getPrecision(), is(16));
    assertThat(columnList.get(8).getScale(), is(8));
    assertThat(columnList.get(8).isPrimaryKey(), is(false));
    assertThat(columnList.get(8).isNullable(), is(true));

    assertThat(columnList.get(9).getName(), is("i"));
    assertThat(columnList.get(9).getType(), is(ColumnType.DECIMAL));
    assertThat(columnList.get(9).getLength(), is(0));
    assertThat(columnList.get(9).getPrecision(), is(12));
    assertThat(columnList.get(9).getScale(), is(0));
    assertThat(columnList.get(9).isPrimaryKey(), is(false));
    assertThat(columnList.get(9).isNullable(), is(false));

    assertThat(tableDef.getField("a").getColumn(), is(columnList.get(1)));
    assertThat(tableDef.getField("a").getOrdinal(), is(1));

    assertThat(tableDef.getPrimaryKeyColumns(), is(ImmutableList.of(columnList.get(0))));
    assertThat(tableDef.getPrimaryKeyColumnNum(), is(1));
    assertThat(tableDef.getPrimaryKeyColumnNames(), is(ImmutableList.of("id")));
    assertThat(tableDef.getPrimaryKeyVarLenColumns(), is(ImmutableList.of()));
    assertThat(tableDef.getPrimaryKeyVarLenColumnNames(), is(ImmutableList.of()));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(0)), is(true));
    assertThat(tableDef.isColumnPrimaryKey(columnList.get(1)), is(false));

    assertThat(tableDef.getSecondaryKeyMetaList().size(), is(3));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getType(), is(KeyMeta.Type.KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getName(), is("ddd"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getNumOfColumns(), is(1));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getKeyColumns(),
        is(ImmutableList.of(columnList.get(4))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getKeyColumnNames(),
        is(ImmutableList.of("d")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getKeyVarLenColumns(),
        is(ImmutableList.of()));
    assertThat(tableDef.getSecondaryKeyMetaList().get(0).getKeyVarLenColumnNames(),
        is(ImmutableList.of()));

    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getType(), is(KeyMeta.Type.UNIQUE_KEY));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getName(), nullValue());
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getNumOfColumns(), is(1));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyColumnNames(),
        is(ImmutableList.of("e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyVarLenColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(1).getKeyVarLenColumnNames(),
        is(ImmutableList.of("e")));

    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getType(), is(KeyMeta.Type.INDEX));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getName(), is("eee"));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getNumOfColumns(), is(2));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyColumns(),
        is(ImmutableList.of(columnList.get(2), columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyColumnNames(),
        is(ImmutableList.of("b", "e")));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyVarLenColumns(),
        is(ImmutableList.of(columnList.get(5))));
    assertThat(tableDef.getSecondaryKeyMetaList().get(2).getKeyVarLenColumnNames(),
        is(ImmutableList.of("e")));
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

    assertThat(tableDef.getSecondaryKeyMetaList(), nullValue());
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
  public void testConvertColumnCharset() {
    String sql = "CREATE TABLE t (c CHAR(20) CHARACTER SET utf8 COLLATE utf8_bin);";
    TableDef tableDef = TableDefUtil.covertToTableDef(sql);
    System.out.println(tableDef);
    // by default set to utf8
    assertThat(tableDef.getDefaultCharset(), is("utf8"));

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
  }

  @Test(expected = SqlParseException.class)
  public void testConvertUnsupportedColumnType() {
    String sql = "CREATE TABLE employees (\n"
        + "    emp_no      INT             NOT NULL,  -- UNSIGNED AUTO_INCREMENT??\n"
        + "    birth_date  DATE            NOT NULL,\n"
        + "    first_name  VARCHAR(14)     NOT NULL,\n"
        + "    last_name   VARCHAR(16)     NOT NULL,\n"
        + "    gender      ENUM ('M','F')  NOT NULL,  -- Enumeration of either 'M' or 'F'  \n"
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

}
