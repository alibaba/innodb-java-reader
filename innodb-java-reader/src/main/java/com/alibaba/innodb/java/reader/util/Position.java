/**
 * Apache License Version 2.0.
 *
 * Copy from https://github.com/rolandhe/hiriver
 */
package com.alibaba.innodb.java.reader.util;

/**
 * Position.
 *
 * @author hexiufeng
 */
public final class Position {

  private int pos;

  private Position(int pos) {
    this.pos = pos;
  }

  public static Position factory() {
    return factory(0);
  }

  public static Position factory(int pos) {
    return new Position(pos);
  }

  public int getPos() {
    return this.pos;
  }

  public int getAndForwordPos() {
    return this.pos++;
  }

  public int getAndForwordPos(int step) {
    int cur = this.pos;
    this.pos += step;
    return cur;
  }

  public int forwardPos() {
    return ++this.pos;
  }

  public int forwardPos(int step) {
    this.pos += step;
    return this.pos;
  }

  public void reset() {
    this.pos = 0;
  }

  public void reset(int newPos) {
    this.pos = newPos;
  }

}
