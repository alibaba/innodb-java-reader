/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.page.fsphdr;

import com.alibaba.innodb.java.reader.ListBaseNode;
import com.alibaba.innodb.java.reader.util.SliceInput;

import lombok.Data;

/**
 * InnoDB allocates FSP_HDR and XDES pages at fixed locations within the space
 *
 * @author xu.zx
 */
@Data
public class FspHeader {

  /** 该文件对应的space id */
  private long space;

  /**
   * Highest page number in file (size)
   * <p>
   * 当前表空间总的PAGE个数，扩展文件时需要更新该值fsp_try_extend_data_file_with_pages
   */
  private long size;

  /**
   * Highest page number initialized (free limit)
   * <p>
   * 当前尚未初始化的最小Page No。从该Page往后的都尚未加入到表空间的FREE LIST上。
   */
  private long freeLimit;

  /**
   * 当前表空间的FLAG信息
   * <pre>
   * Macro Desc
   * FSP_FLAGS_POS_ZIP_SSIZE 压缩页的block size，如果为0表示非压缩表
   * FSP_FLAGS_POS_ATOMIC_BLOBS 使用的是compressed或者dynamic的行格式
   * FSP_FLAGS_POS_PAGE_SSIZE InnerPage Size
   * FSP_FLAGS_POS_DATA_DIR 如果该表空间显式指定了data_dir，则设置该flag
   * FSP_FLAGS_POS_SHARED 是否是共享的表空间，如5.7引入的General Tablespace，可以在一个表空间中创建多个表
   * FSP_FLAGS_POS_TEMPORARY 是否是临时表空间
   * FSP_FLAGS_POS_ENCRYPTION 是否是加密的表空间，MySQL 5.7.11引入
   * FSP_FLAGS_POS_UNUSED 未使用的位
   * </pre>
   */
  private long flags;

  /** FSP_FREE_FRAG链表上已被使用的Page数，用于快速计算该链表上可用空闲Page数 */
  private long numberOfPagesUsed;

  /** 当一个Extent中所有page都未被使用时，放到该链表上，可以用于随后的分配 */
  private ListBaseNode free;

  /** FREE_FRAG链表的Base Node，通常这样的Extent中的Page可能归属于不同的segment，用于segment frag array page的分配 */
  private ListBaseNode freeFrag;

  /** Extent中所有的page都被使用掉时，会放到该链表上，当有Page从该Extent释放时，则移回FREE_FRAG链表 */
  private ListBaseNode fullFrag;

  /** 当前文件中最大Segment ID + 1，用于段分配时的seg id计数器 */
  private long nextUsedSegmentId;

  /** 已被完全用满的Inode Page链表 */
  private ListBaseNode fullInodes;

  /** 至少存在一个空闲Inode Entry的Inode Page被放到该链表上 */
  private ListBaseNode freeInodes;

  public static FspHeader fromSlice(SliceInput input) {
    FspHeader fspHeader = new FspHeader();
    fspHeader.setSpace(input.readUnsignedInt());
    // FSP_NOT_USED 4 保留字节
    input.skipBytes(4);
    fspHeader.setSize(input.readUnsignedInt());
    fspHeader.setFreeLimit(input.readUnsignedInt());
    fspHeader.setFlags(input.readUnsignedInt());
    fspHeader.setNumberOfPagesUsed(input.readUnsignedInt());
    fspHeader.setFree(ListBaseNode.fromSlice(input));
    fspHeader.setFreeFrag(ListBaseNode.fromSlice(input));
    fspHeader.setFullFrag(ListBaseNode.fromSlice(input));
    fspHeader.setNextUsedSegmentId(input.readLong());
    fspHeader.setFullInodes(ListBaseNode.fromSlice(input));
    fspHeader.setFreeInodes(ListBaseNode.fromSlice(input));
    return fspHeader;
  }
}
