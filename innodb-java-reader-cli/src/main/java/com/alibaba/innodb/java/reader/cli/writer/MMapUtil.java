package com.alibaba.innodb.java.reader.cli.writer;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Utility class which provides a method for attempting to directly unmap a MappedByteBuffer rather than
 * waiting for the JVM and OS eventually unmap.
 */
public class MMapUtil {

  /**
   * No instances
   */
  private MMapUtil() {
    throw new AssertionError();
  }

  /**
   * <a href="http://svn.apache.org/repos/asf/lucene/dev/trunk/lucene/src/java/org/apache/lucene/store/MMapDirectory.java">MMapDirectory.java</a>
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;

  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;

    if (!UNMAP_SUPPORTED) {
      System.out.println("JVM does not support unmapping memory-mapped files.");
    }
  }

  /**
   * Try to unmap the given {@link MappedByteBuffer}. This method enables the workaround for unmapping the buffers from
   * address space after closing IndexInput, that is mentioned in the bug report. This hack may fail on
   * non-Sun JVMs. It forcefully unmaps the buffer on close by using an undocumented internal cleanup functionality.
   *
   * @param theBuffer buffer
   * @return <code>true</code> if unmap was successful, <code>false</code> if unmap is not supported by the JVM or if
   * there was an exception while trying to unmap.
   */
  public static final boolean unmap(final MappedByteBuffer theBuffer) {
    if (UNMAP_SUPPORTED) {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
          public Object run() throws Exception {
            final Method getCleanerMethod = theBuffer.getClass().getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            final Object cleaner = getCleanerMethod.invoke(theBuffer);
            if (cleaner != null) {
              cleaner.getClass().getMethod("clean").invoke(cleaner);
            }
            return null;
          }
        });
        return true;
      } catch (PrivilegedActionException e) {
        System.err.println("Cleaning memory mapped byte buffer failed: " + e.getMessage());
      }
    }

    return false;
  }
}


