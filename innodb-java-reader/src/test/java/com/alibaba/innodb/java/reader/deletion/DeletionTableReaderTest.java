package com.alibaba.innodb.java.reader.deletion;

import com.alibaba.innodb.java.reader.AbstractTest;
import com.alibaba.innodb.java.reader.TableReader;
import com.alibaba.innodb.java.reader.page.index.GenericRecord;
import com.alibaba.innodb.java.reader.schema.Column;
import com.alibaba.innodb.java.reader.schema.Schema;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author xu.zx
 */
public class DeletionTableReaderTest extends AbstractTest {

  public Schema getSchema() {
    return new Schema()
        .addColumn(new Column().setName("id").setType("int(11)").setNullable(false).setPrimaryKey(true))
        .addColumn(new Column().setName("a").setType("bigint(20)").setNullable(false))
        .addColumn(new Column().setName("b").setType("varchar(64)").setNullable(false))
        .addColumn(new Column().setName("c").setType("varchar(1024)").setNullable(true));
  }

  @Test
  public void testDeletionTableQueryAllMysql56() {
    testDeletionTableQueryAll(IBD_FILE_BASE_PATH_MYSQL56 + "deletion/tb13.ibd");
  }

  @Test
  public void testDeletionTableQueryAllMysql57() {
    testDeletionTableQueryAll(IBD_FILE_BASE_PATH_MYSQL57 + "deletion/tb13.ibd");
  }

  @Test
  public void testDeletionTableQueryAllMysql80() {
    testDeletionTableQueryAll(IBD_FILE_BASE_PATH_MYSQL80 + "deletion/tb13.ibd");
  }

  public void testDeletionTableQueryAll(String path) {
    try (TableReader reader = new TableReader(path, getSchema())) {
      reader.open();

      List<GenericRecord> recordList = reader.queryAll();
      assertThat(recordList.size(), is(1000));
    }
  }

}
