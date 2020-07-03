/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited.
 * Copyright (c) 1994, 2009, Oracle and/or its affiliates. All Rights Reserved.
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

/**
 * Page level checksum algorithm, see <code>innodb_utility.c</code>
 * in innodb source code.
 *
 * @author xu.zx
 */
public class Checksum {

  public static final long UT_HASH_RANDOM_MASK = 1463735687L;

  public static final long UT_HASH_RANDOM_MASK2 = 1653893711L;

  private static long fold(long n1, long n2) {
    return (((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1) ^ UT_HASH_RANDOM_MASK) + n2);
  }

  public static long getValue(byte[] bytes, int offset, int limit) {
    long fold = 0L;
    for (int i = offset; i < limit; i++) {
      fold = fold(fold, bytes[i] & 0xFFL);
    }
    return fold;
  }

}
