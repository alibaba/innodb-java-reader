/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.inode;

import com.alibaba.innodb.java.reader.ListBaseNode;
import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.innodb.java.reader.util.Utils.maybeUndefined;

/**
 * InodeEntry
 *
 * @author xu.zx
 */
@Data
public class InodeEntry {

  public static final int FRAG_ARRAY_SIZE = 32;

  /**
   * 该Inode归属的Segment ID，若值为0表示该slot未被使用
   */
  private long segmentId;

  /**
   * FSEG_NOT_FULL链表上被使用的Page数量
   */
  private int numberOfPagesUsedInNotFullList;

  /**
   * 完全没有被使用并分配给该Segment的Extent链表
   */
  private ListBaseNode free;

  /**
   * 至少有一个page分配给当前Segment的Extent链表，全部用完时，转移到FSEG_FULL上，全部释放时，则归还给当前表空间FSP_FREE链表
   */
  private ListBaseNode notFull;

  /**
   * 分配给当前segment且Page完全使用完的Extent链表
   */
  private ListBaseNode full;

  /**
   * The value 97937874 is stored as a marker that this file segment INODE entry has been properly initialized.
   */
  private int magicNumber;

  /**
   * 属于该Segment的独立Page。总是先从全局分配独立的Page，
   * 当填满32个数组项时，就在每次分配时都分配一个完整的Extent，并在XDES PAGE中将其Segment ID设置为当前值.
   * 总共存储32个记录项
   */
  private Long[] fragArrayEntries = new Long[32];

  public static InodeEntry fromSlice(SliceInput input) {
    InodeEntry inodeEntry = new InodeEntry();
    inodeEntry.setSegmentId(input.readLong());
    inodeEntry.setNumberOfPagesUsedInNotFullList(input.readInt());
    inodeEntry.setFree(ListBaseNode.fromSlice(input));
    inodeEntry.setNotFull(ListBaseNode.fromSlice(input));
    inodeEntry.setFull(ListBaseNode.fromSlice(input));
    // Magic Number
    inodeEntry.setMagicNumber(input.readInt());
    if (inodeEntry.getSegmentId() > 0) {
      checkState(inodeEntry.getMagicNumber() == 97937874);
    }
    for (int i = 0; i < FRAG_ARRAY_SIZE; i++) {
      inodeEntry.getFragArrayEntries()[i] = maybeUndefined(input.readUnsignedInt());
    }
    return inodeEntry;
  }

}
