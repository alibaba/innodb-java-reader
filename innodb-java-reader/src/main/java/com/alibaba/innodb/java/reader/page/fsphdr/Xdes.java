/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.fsphdr;

import com.alibaba.innodb.java.reader.ListNode;
import com.alibaba.innodb.java.reader.util.EnumUtil;
import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * Extent descriptor
 *
 * @author xu.zx
 */
@Data
public class Xdes {

  private long segmentId;

  /**
   * List node for XDES list: Pointers to previous and next extents in a doubly-linked
   * extent descriptor list.
   * 指向前后extent的指针，所有extent双向链接。
   */
  private ListNode xdesListNode;

  /**
   * State: extent的空闲状态。The current state of the extent, for which only four values
   * are currently defined: FREE, FREE_FRAG, and FULL_FRAG, meaning this extent belongs to
   * the space’s list with the same name; and FSEG, meaning this extent belongs to the file
   * segment with the ID stored in the File Segment ID field.
   */
  private XdesState state;

  /**
   * Page State Bitmap: page空闲情况。A bitmap of 2 bits per page in the extent (64 x 2 = 128 bits,
   * or 16 bytes). The first bit indicates whether the page is free. The second bit is reserved
   * to indicate whether the page is clean (has no un-flushed data), but this bit is currently
   * unused and is always set to 1.
   */
  private byte[] pageStateBitmap;

  public static Xdes fromSlice(SliceInput input) {
    Xdes xdes = new Xdes();
    xdes.setSegmentId(input.readLong());
    xdes.setXdesListNode(ListNode.fromSlice(input));
    xdes.setState(EnumUtil.find(XdesState.class, input.readInt()));
    xdes.setPageStateBitmap(input.readByteArray(16));
    return xdes;
  }

}
