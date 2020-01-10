/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.cli;

/**
 * CommandType
 *
 * @author xu.zx
 */
public enum CommandType {

  /**
   * Command type enums
   */

  SHOW_ALL_PAGES("show-all-pages", "show all pages"),
  SHOW_PAGES("show-pages", "show specific pages"),
  QUERY_BY_PAGE_NUMBER("query-by-page-number", "query records by page number"),
  QUERY_BY_PK("query-by-pk", "query records by primary key"),
  QUERY_ALL("query-all", "query all records"),
  RANGE_QUERY_BY_PK("range-query-by-pk", "range query records"),
  GEN_LSN_HEATMAP("gen-lsn-heatmap", "generate lsn heatmap"),
  GEN_FILLING_RATE_HEATMAP("gen-filling-rate-heatmap", "generate filling rate heatmap"),
  GET_ALL_INDEX_PAGE_FILLING_RATE("get-all-index-page-filling-rate", "get all index page filling rate");

  CommandType(final String type, final String desc) {
    this.type = type;
    this.desc = desc;
  }

  private String type;

  private String desc;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }
}
