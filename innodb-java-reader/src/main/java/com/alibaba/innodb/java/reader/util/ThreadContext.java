/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.util.HashMap;
import java.util.Map;

/**
 * ThreadLocal helper.
 *
 * @author xu.zx
 */
public class ThreadContext {

  public static final String SK_ORDINAL_KEY = "sk_ordinal";

  public static final String SK_ROOT_PAGE_NUMBER = "sk_root_page_number";

  private static final ThreadLocal<Map<String, Object>> CTX_HOLDER = new ThreadLocal<Map<String, Object>>();

  static {
    CTX_HOLDER.set(new HashMap<String, Object>());
  }

  public static void putContext(String key, Object value) {
    Map<String, Object> ctx = CTX_HOLDER.get();
    if (ctx == null) {
      ctx = new HashMap<String, Object>(8);
      CTX_HOLDER.set(ctx);
    }
    ctx.put(key, value);
  }

  public static <T extends Object> T getContext(String key) {
    Map<String, Object> ctx = CTX_HOLDER.get();
    if (ctx == null) {
      return null;
    }
    return (T) ctx.get(key);
  }

  public static Map<String, Object> getContext() {
    Map<String, Object> ctx = CTX_HOLDER.get();
    if (ctx == null) {
      return null;
    }
    return ctx;
  }

  public static void remove(String key) {
    Map<String, Object> ctx = CTX_HOLDER.get();
    if (ctx != null) {
      ctx.remove(key);
    }
  }

  public static boolean contains(String key) {
    Map<String, Object> ctx = CTX_HOLDER.get();
    if (ctx != null) {
      return ctx.containsKey(key);
    }
    return false;
  }

  public static void clean() {
    CTX_HOLDER.remove();
    // CTX_HOLDER.set(null);
  }

  public static void init() {
    CTX_HOLDER.set(new HashMap<String, Object>(8));
  }

  public static Integer getSkOrdinal() {
    return (Integer) getContext(SK_ORDINAL_KEY);
  }

  /**
   * Starts from 0.
   *
   * @param skOrdinal secondary key ordinal in DDL
   */
  public static void putSkOrdinal(int skOrdinal) {
    putContext(SK_ORDINAL_KEY, skOrdinal);
  }

  public static Long getSkRootPageNumber() {
    return (Long) getContext(SK_ROOT_PAGE_NUMBER);
  }

  public static void putSkRootPageNumber(Long skRootPageNumber) {
    putContext(SK_ROOT_PAGE_NUMBER, skRootPageNumber);
  }

}
