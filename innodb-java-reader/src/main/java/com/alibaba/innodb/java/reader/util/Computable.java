package com.alibaba.innodb.java.reader.util;

import java.util.concurrent.Callable;

/**
 * Computable
 *
 * @author xu.zx
 */
public interface Computable<K, V> {

  /**
   * Compute based on key
   *
   * @param key      key
   * @param callable # @see Callable
   * @return result value
   */
  V get(K key, Callable<V> callable);

  /**
   * Get result
   *
   * @param key key
   */
  void remove(K key);

  /**
   * Clear
   */
  void clear();

}
