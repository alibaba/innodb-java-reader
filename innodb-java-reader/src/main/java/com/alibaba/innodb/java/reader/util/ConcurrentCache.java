/*
 * Copyright (C) 1999-2019 Alibaba Group Holding Limited
 */
package com.alibaba.innodb.java.reader.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import lombok.extern.slf4j.Slf4j;

/**
 * ConcurrentCache.
 *
 * @author xu.zx
 */
@Slf4j
public class ConcurrentCache<K, V> implements Computable<K, V> {

  protected final ConcurrentMap<K, Future<V>> concurrentMap;

  public ConcurrentCache() {
    concurrentMap = new ConcurrentHashMap<>();
  }

  public static <K, V> Computable<K, V> createComputable() {
    return new ConcurrentCache<K, V>();
  }

  @Override
  public V get(K key, Callable<V> callable) {
    Future<V> future = concurrentMap.get(key);
    if (future == null) {
      FutureTask<V> futureTask = new FutureTask<V>(callable);
      future = concurrentMap.putIfAbsent(key, futureTask);
      if (future == null) {
        future = futureTask;
        futureTask.run();
      }
    }
    try {
      // 此时阻塞
      return future.get();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      concurrentMap.remove(key);
      return null;
    }
  }

  @Override
  public void remove(K key) {
    concurrentMap.remove(key);
  }

  @Override
  public void clear() {
    concurrentMap.clear();
  }

}

