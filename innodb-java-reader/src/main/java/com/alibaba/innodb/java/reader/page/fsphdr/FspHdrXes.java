/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.fsphdr;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

/**
 * File space header.
 *
 * @author xu.zx
 */
@Data
public class FspHdrXes extends AbstractPage {

  /**
   * One FSP_HDR, a.k.a XDES, has maximum of 256 extents (or 16,384 pages, 256 MB)，
   * Every extent is managed by a XDES Entry.
   */
  public static final int MAX_SEGMENT_SIZE = 256;

  private FspHeader fspHeader;

  private List<Xdes> xdesList = new ArrayList<>();

  public FspHdrXes(InnerPage innerPage) {
    super(innerPage);
    this.fspHeader = FspHeader.fromSlice(sliceInput);
    for (int i = 0; i < MAX_SEGMENT_SIZE; i++) {
      Xdes curr = Xdes.fromSlice(sliceInput);
      // TODO When state is null stops, but not sure if this is what Mysql does.
      if (curr.getState() == null) {
        break;
      }
      this.xdesList.add(curr);
    }
  }

}
