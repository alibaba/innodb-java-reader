package com.alibaba.innodb.java.reader;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xu.zx
 */
public class SysoutInterceptor extends PrintStream {

  private List<String> output = new ArrayList<>();

  private boolean enableConsoleOutput;

  public SysoutInterceptor(OutputStream out, boolean enableConsoleOutput) {
    super(out, true);
    this.enableConsoleOutput = enableConsoleOutput;
  }

  @Override
  public void print(String s) {
    if (enableConsoleOutput) {
      super.print(s);
    }
    output.add(s);
  }

  public void clear() {
    output.clear();
  }

  public List<String> getOutput() {
    return output;
  }
}
