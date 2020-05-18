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
