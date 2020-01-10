/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.heatmap;

import java.util.ArrayList;
import java.util.List;

/**
 * One line in heatmap
 *
 * @author xu.zx
 */
public class Line<T extends Number> {

  private List<T> list = new ArrayList<>();

  public List<T> getList() {
    return list;
  }

  public void setList(List<T> list) {
    this.list = list;
  }
}
