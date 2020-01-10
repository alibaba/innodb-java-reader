package com.alibaba.innodb.java.reader.schema;

import com.alibaba.innodb.java.reader.column.ColumnType;
import com.alibaba.innodb.java.reader.exception.SqlParseException;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class SchemaUtilTest {

  @Test
  public void testConvert() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) unsigneD NOT NULL,\n"
        + "b tinyint DEFAULT 0 COMMENT 'comment here你好，：呵呵',\n"
        + "c text NOT NULL,\n"
        + "d datetime NOT NULL DEFAULT '9999-00-00 00:00:00',\n"
        + "`e` varchar(64) NOT NULL,\n"
        + "`f` varchar(1024) default 'THIS_IS_DEFAULT_VALUE',\n"
        + "PRIMARY KEY (`id`),\n"
        + "KEY `ddd` (`a`))\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    Schema schema = SchemaUtil.covertFromSqlToSchema(sql);
    System.out.println(schema);
    assertThat(schema.getTableCharset(), is("utf8mb4"));

    assertThat(schema.getPrimaryKeyColumn().getName(), is("id"));

    List<Column> columnList = schema.getColumnList();
    assertThat(columnList.size(), is(7));

    assertThat(columnList.get(0).getName(), is("id"));
    assertThat(columnList.get(0).getType(), is(ColumnType.INT));
    assertThat(columnList.get(0).isPrimaryKey(), is(true));
    assertThat(columnList.get(0).isNullable(), is(false));

    assertThat(columnList.get(1).getName(), is("a"));
    assertThat(columnList.get(1).getType(), is(ColumnType.BIGINT + " UNSIGNED"));
    assertThat(columnList.get(1).isPrimaryKey(), is(false));
    assertThat(columnList.get(1).isNullable(), is(false));

    assertThat(schema.getField("b").getColumn().getName(), is("b"));
    assertThat(schema.getField("b").getColumn().getType(), is(ColumnType.TINYINT));
    assertThat(schema.getField("b").getColumn().isPrimaryKey(), is(false));
    assertThat(schema.getField("b").getColumn().isNullable(), is(true));

    assertThat(columnList.get(3).getName(), is("c"));
    assertThat(columnList.get(3).getType(), is(ColumnType.TEXT));
    assertThat(columnList.get(3).isPrimaryKey(), is(false));
    assertThat(columnList.get(3).isNullable(), is(false));

    assertThat(columnList.get(4).getName(), is("d"));
    assertThat(columnList.get(4).getType(), is(ColumnType.DATETIME));
    assertThat(columnList.get(4).isPrimaryKey(), is(false));
    assertThat(columnList.get(4).isNullable(), is(false));

    assertThat(columnList.get(5).getName(), is("e"));
    assertThat(columnList.get(5).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(5).isPrimaryKey(), is(false));
    assertThat(columnList.get(5).isNullable(), is(false));

    assertThat(columnList.get(6).getName(), is("f"));
    assertThat(columnList.get(6).getType(), is(ColumnType.VARCHAR));
    assertThat(columnList.get(6).isPrimaryKey(), is(false));
    assertThat(columnList.get(6).isNullable(), is(true));
  }

  @Test(expected = SqlParseException.class)
  public void testConvertNegativeNoColumn() {
    String sql = "CREATE TABLE `tb01`\n"
        + "()\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    SchemaUtil.covertFromSqlToSchema(sql);
  }

  @Test(expected = SqlParseException.class)
  public void testConvertNegativeNoPk() {
    String sql = "CREATE TABLE `tb01`\n"
        + "(`id` int(11) NOT NULL ,\n"
        + "`a` bigint(20) unsigneD NOT NULL,\n"
        + "b tinyint DEFAULT 0 COMMENT 'comment here',\n"
        + "c text NOT NULL,\n"
        + "d datetime NOT NULL,\n"
        + "`e` varchar(64) NOT NULL,\n"
        + "`f` varchar(1024) default 'THIS_IS_DEFAULT_VALUE')\n"
        + "ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;";
    SchemaUtil.covertFromSqlToSchema(sql);
  }

}
