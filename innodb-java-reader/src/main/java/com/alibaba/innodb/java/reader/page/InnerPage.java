/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page;

import com.alibaba.innodb.java.reader.util.Slice;
import com.alibaba.innodb.java.reader.util.SliceInput;

import org.codehaus.jackson.annotate.JsonIgnore;

import lombok.Getter;

import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_HEADER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_FIL_TRAILER;
import static com.alibaba.innodb.java.reader.SizeOf.SIZE_OF_PAGE;

/**
 * page = FIL HEADER (38) + body + FIL TRAILER(8).
 *
 * @author xu.zx
 */
public class InnerPage {

  @Getter
  protected FilHeader filHeader;

  @Getter
  protected FilTrailer filTrailer;

  /**
   * page 0 is located at file offset 0, page 1 at file offset 16384.
   */
  @Getter
  protected long pageNumber;

  /**
   * 16k page byte buffer with fil header and fil trailer.
   */
  @JsonIgnore
  @Getter
  protected SliceInput sliceInput;

  public InnerPage(long pageNumber, Slice slice) {
    this.pageNumber = pageNumber;
    this.sliceInput = slice.input();
    this.filHeader = FilHeader.fromSlice(sliceInput);
    sliceInput.setPosition(SIZE_OF_PAGE - SIZE_OF_FIL_TRAILER);
    this.filTrailer = FilTrailer.fromSlice(sliceInput);
    checkState(this.filHeader.getLow32Lsn() == this.filTrailer.getLow32lsn(), "low32 lsn not match");

    // reset to end of fil header
    this.sliceInput.setPosition(SIZE_OF_FIL_HEADER);
  }

  public PageType pageType() {
    return filHeader.getPageType();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Page(");
    sb.append("header=").append(filHeader);
    sb.append(')');
    return sb.toString();
  }

}
