package com.alibaba.innodb.java.reader.util;

import java.lang.reflect.Method;

/**
 * EnumUtil
 * <p>
 * This util class is useful to find enum from an identifier. For example,
 * <code>EnumUtil.find(PageType.class, 3)</code>.
 * For hot code, the performance might be a problem, especially for cases when null is returned.
 *
 * @author xu.zx
 */
public class EnumUtil {

  private static final Computable<String, Enum<?>> COMPUTABLE = ConcurrentCache.createComputable();

  public static <E extends Enum<E>> E find(final Class<E> enumType, final Object value) {
    if (enumType == null || value == null) {
      return null;
    }

    if (enumType.isAssignableFrom(IdAble.class)) {
      return null;
    }

    String key = enumType.getName() + ":(" + value.getClass().getName() + ":" + value + ")";
    @SuppressWarnings("unchecked")
    E result = (E) COMPUTABLE.get(key, () -> {
      IdAble<?>[] values = invokeStaticMethod(enumType, "values");

      for (IdAble<?> e : values) {
        if (value.equals(e.id())) {
          @SuppressWarnings("unchecked")
          E result1 = (E) e;
          return result1;
        }
      }

      return null;
    });

    return result;
  }

  public static <T> T invokeStaticMethod(Class<?> clazz, String methodName) {
    Method method;
    try {
      method = clazz.getDeclaredMethod(methodName);
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
    if (method == null) {
      return null;
    }

    return invokeMethod(method, (Object) null, null);
  }

  public static <T> T invokeMethod(Method method, Object target, Object... args) {
    if (method == null) {
      return null;
    }
    method.setAccessible(true);
    try {
      T result = (T) method.invoke(target, args);
      return result;
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

}
