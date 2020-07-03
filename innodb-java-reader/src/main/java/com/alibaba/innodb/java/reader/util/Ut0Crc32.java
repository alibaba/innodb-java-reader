/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited.
 * Copyright (c) 2009, 2010 Facebook, Inc. All Rights Reserved.
 * Copyright (c) 2011, 2015, Oracle and/or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA
 */
package com.alibaba.innodb.java.reader.util;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * InnoDB page checksum utility.
 * <p>
 * CRC32 implementation from Facebook, based on the zlib implementation.
 * <p>
 * This file is translated based on {@code ut/ut0crc32.cc} in MySQL, and I
 * make it from {@code C} language to {@code Java} language and reserve the
 * original naming convention, so checkstyle should suppress the warnings.
 * <p>
 * Most of the code was taken from MySQL codebase. Here to acknowledge the
 * copyright and author.
 *
 * @author xu.zx
 */
public class Ut0Crc32 {

  /**
   * bit-reversed poly 0x1EDC6F41 (from SSE42 crc32 instruction)
   */
  private static final long POLY = 0x82f63b78L;

  private static final long[][] UT_CRC_32_SLICE_8_TABLE = new long[8][256];

  private static ThreadLocal<long[][]> UT_CRC_32_SLICE_8_TABLE_THREAD_LOCAL
      = ThreadLocal.withInitial(() -> new long[8][256]);

  // private static boolean utCrc32Slice8TableInitialized = false;

  /**
   * Initializes the table that is used to generate the CRC32 if the CPU does
   * not have support for it.
   * <p>
   * ut_crc32_slice8_table_init
   */
  static {
    long n;
    long k;
    long c;
    for (n = 0; n < 256; n++) {
      c = n;
      for (k = 0; k < 8; k++) {
        c = ((c & 1) == 1) ? (POLY ^ (c >> 1)) : (c >> 1);
      }
      UT_CRC_32_SLICE_8_TABLE[0][(int) (n & 0xFFL)] = c;
    }

    for (n = 0; n < 256; n++) {
      c = UT_CRC_32_SLICE_8_TABLE[0][(int) n];
      for (k = 1; k < 8; k++) {
        c = UT_CRC_32_SLICE_8_TABLE[0][(int) (c & 0xFFL)] ^ (c >> 8);
        UT_CRC_32_SLICE_8_TABLE[(int) (k & 0xFFL)][(int) (n & 0xFFL)] = c;
      }
    }

    //ut_crc32_slice8_table_initialized = true;
  }

  public static long crc32(byte[] data, int offset, int len) {
    long crc = 0xFFFFFFFFL;

    long[][] table = init(UT_CRC_32_SLICE_8_TABLE_THREAD_LOCAL.get());

    /* Calculate byte-by-byte up to an 8-byte aligned address. After
    this consume the input 8-bytes at a time. */
    // In C code, it needs to align to be CPU friendly, but in Java,
    // this is meaningless, so comment this snippet.
    // while (len > 0 && (reinterpret_cast<uintptr_t>(buf) & 7) != 0) {
    //  ut_crc32_8_sw(&crc, &buf, &len);
    //}

    while (len >= 128) {
      /* This call is repeated 16 times. 16 * 8 = 128. */
      for (int i = 0; i < 16; i++) {
        long dataInt = fromByteArray(data, offset);
        crc = utCrc3264LowSw(crc, dataInt, table);
        offset += 8;
        len -= 8;
      }
    }

    while (len >= 8) {
      long dataInt = fromByteArray(data, offset);
      crc = utCrc3264LowSw(crc, dataInt, table);
      offset += 8;
      len -= 8;
    }

    while (len > 0) {
      crc = utCrc328Sw(crc, data[offset] & 0xFFL, table);
      offset++;
      len--;
    }

    return (~crc) & 0xFFFFFFFFL;
  }

  private static long[][] init(long[][] table) {
    for (int i = 0; i < 8; i++) {
      for (int j = 0; j < 256; j++) {
        table[i][j] = UT_CRC_32_SLICE_8_TABLE[i][j];
      }
    }
    return table;
  }

  public static long crc32ByteByByte(byte[] data, int offset, int len) {
    long crc = 0xFFFFFFFFL;

    long[][] table = init(UT_CRC_32_SLICE_8_TABLE_THREAD_LOCAL.get());

    while (len > 0) {
      crc = utCrc328Sw(crc, data[offset] & 0xFFL, table);
      offset++;
      len--;
    }

    return (~crc) & 0xFFFFFFFFL;
  }

  /**
   * Calculate CRC32 over a 64-bit integer using a software implementation.
   *
   * @param crc   crc32 checksum so far
   * @param data  data to be checksummed
   * @param table base table to calculate
   * @return resulting checksum of crc + crc(data)
   */
  public static long utCrc3264LowSw(long crc, long data, long[][] table) {
    long i = crc ^ data;
    return table[7][(int) ((i) & 0xFFL)] ^
        table[6][(int) ((i >> 8) & 0xFFL)] ^
        table[5][(int) ((i >> 16) & 0xFFL)] ^
        table[4][(int) ((i >> 24) & 0xFFL)] ^
        table[3][(int) ((i >> 32) & 0xFFL)] ^
        table[2][(int) ((i >> 40) & 0xFFL)] ^
        table[1][(int) ((i >> 48) & 0xFFL)] ^
        table[0][(int) ((i >> 56) & 0xFFL)];
  }

  public static long utCrc328Sw(long crc, long data, long[][] table) {
    int i = (int) ((crc ^ data) & 0xFFL);
    return ((crc >> 8) ^ table[0][i]);
  }

  public static long utCrc32SwapByteorder(long i) {
    return (i << 56
        | (i & 0x000000000000FF00L) << 40
        | (i & 0x0000000000FF0000L) << 24
        | (i & 0x00000000FF000000L) << 8
        | (i & 0x000000FF00000000L) >> 8
        | (i & 0x0000FF0000000000L) >> 24
        | (i & 0x00FF000000000000L) >> 40
        | i >> 56);
  }

  /**
   * Returns the {@code long} value whose little-endian representation is stored
   * in the 8 bytes of {@code bytes} from {@code offset}.
   */
  public static long fromByteArray(byte[] bytes, int offset) {
    checkArgument(offset + 7 <= bytes.length,
        "array too small, length=%s ,offset=%s", bytes.length, offset);
    return (bytes[offset + 7] & 0xFFL) << 56
        | (bytes[offset + 6] & 0xFFL) << 48
        | (bytes[offset + 5] & 0xFFL) << 40
        | (bytes[offset + 4] & 0xFFL) << 32
        | (bytes[offset + 3] & 0xFFL) << 24
        | (bytes[offset + 2] & 0xFFL) << 16
        | (bytes[offset + 1] & 0xFFL) << 8
        | (bytes[offset] & 0xFFL);
  }

}
