/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.blob;

import com.alibaba.innodb.java.reader.page.AbstractPage;
import com.alibaba.innodb.java.reader.page.InnerPage;

import lombok.Data;

import static com.alibaba.innodb.java.reader.util.Utils.maybeUndefined;

/**
 * Overflow page and BLOB,TEXT page.
 *
 * @author xu.zx
 */
@Data
public class Blob extends AbstractPage {

  private long length;

  private Long nextPageNumber;

  private long offset;

  public Blob(InnerPage innerPage) {
    super(innerPage);
    this.length = sliceInput.readUnsignedInt();
    this.nextPageNumber = maybeUndefined(sliceInput.readUnsignedInt());
  }

  public Blob(InnerPage innerPage, long offset) {
    super(innerPage);
    this.offset = offset;
    sliceInput.setPosition((int) offset);
    this.length = sliceInput.readUnsignedInt();
    this.nextPageNumber = maybeUndefined(sliceInput.readUnsignedInt());
  }

  public byte[] read() {
    return sliceInput.readByteArray((int) length);
  }

  public boolean hasNext() {
    return nextPageNumber != null;
  }
}
