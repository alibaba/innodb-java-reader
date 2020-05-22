/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.schema;

import com.alibaba.innodb.java.reader.page.index.Index;

import java.util.Optional;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.innodb.java.reader.Constants.ROOT_PAGE_NUMBER;
import static java.util.stream.Collectors.toList;

/**
 * Workaround for features that supported in a non-standard way.
 *
 * @author xu.zx
 */
@Slf4j
public class Workaround {

  /**
   * Get secondary key root page number.
   * <p>
   * Find clustered index root page and calculate sk page number based on it.
   * Note that if table has ever been altered to add or remove indices,
   * the secondary key root page number may be incorrect, and cause error.
   * <p>
   * MySQL will set FULLTEXT KEY, UNIQUE KEY ahead of normal KEY, but in
   * <code>SHOW CREATE TABLE</code> command, the FULLTEXT KEY goes to the end,
   * so here the workaround is to add the delta of fulltext key count.
   * <p>
   * More standard way would be to look up root page number by:
   * <code>SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES;</code> before 5.7
   * or
   * <code>SELECT * FROM INFORMATION_SCHEMA.INNODB_INDEXES;</code> after 8.0
   * <p>
   * For example, for key "age", the page no. will be page#8.
   * <pre>
   * SELECT * FROM INFORMATION_SCHEMA.INNODB_SYS_INDEXES WHERE TABLE_ID = 3399;
   * +----------+------------------+----------+------+----------+---------+-------+
   * | INDEX_ID | NAME             | TABLE_ID | TYPE | N_FIELDS | PAGE_NO | SPACE |
   * +----------+------------------+----------+------+----------+---------+-------+
   * |     5969 | PRIMARY          |     3399 |    3 |        1 |       3 |  3385 |
   * |     5975 | FTS_DOC_ID_INDEX |     3399 |    2 |        1 |       4 |  3385 |
   * |     5976 | empno            |     3399 |    2 |        1 |       5 |  3385 |
   * |     5977 | name             |     3399 |    0 |        1 |       6 |  3385 |
   * |     5978 | idx_city         |     3399 |    0 |        1 |       7 |  3385 |
   * |     5979 | age              |     3399 |    0 |        1 |       8 |  3385 |
   * |     5980 | age_2            |     3399 |    0 |        2 |       9 |  3385 |
   * |     5981 | key_join_date    |     3399 |    0 |        1 |      10 |  3385 |
   * |     5982 | deptno           |     3399 |    0 |        3 |      11 |  3385 |
   * |     5983 | deptno_2         |     3399 |    0 |        3 |      12 |  3385 |
   * |     5984 | address          |     3399 |    0 |        1 |      13 |  3385 |
   * |     5985 | profile          |     3399 |   32 |        1 |      -1 |  3385 |
   * |     5992 | key_level        |     3399 |    0 |        1 |      20 |  3385 |
   * +----------+------------------+----------+------+----------+---------+-------+
   * </pre>
   *
   * @param tableDef  table definition
   * @param skName    secondary key name
   * @param skOrdinal secondary key ordinal in <code>SHOW CREATE TABLE</code> command
   * @param func      function to apply on page number and return the loaded index page
   * @return secondary key root page number
   */
  public static long getSkRootPageNumber(TableDef tableDef, String skName, Optional<Integer> skOrdinal,
                                         Function<Long, Index> func) {
    Index rootIndex = func.apply((long) ROOT_PAGE_NUMBER);
    long fulltextKeyCount = tableDef.getSecondaryKeyMetaList().stream()
        .filter(k -> k.getType() == KeyMeta.Type.FULLTEXT_KEY).count();
    int ordinal = skOrdinal.isPresent() ? skOrdinal.get()
        : tableDef.getSecondaryKeyMetaList().stream().map(KeyMeta::getName).collect(toList()).indexOf(skName);
    long skRootPageNumber = rootIndex.getPageNumber() + fulltextKeyCount + ordinal + 1;
    log.debug("Secondary key ({}) root page number is {}, pkRootPage={}, fulltextKeyCount={}, ordinal={}",
        skName, skRootPageNumber, rootIndex.getPageNumber(), fulltextKeyCount, ordinal);
    return skRootPageNumber;
  }

}
