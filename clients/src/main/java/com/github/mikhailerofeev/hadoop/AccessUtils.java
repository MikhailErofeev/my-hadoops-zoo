package com.github.mikhailerofeev.hadoop;

import com.github.mikhailerofeev.hadoop.fun.CheckedProcedure;
import com.github.mikhailerofeev.hadoop.fun.UncheckedProcedure;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.function.Supplier;

/**
 * @author m-erofeev
 * @since 28.03.15
 */
class AccessUtils {

  public static final UserGroupInformation ROOT_USER_SIMPLE_AUTH = UserGroupInformation.createRemoteUser("root");

  public static <R> R execAsRoot(final Supplier<R> function) {
    try {
      return ROOT_USER_SIMPLE_AUTH.doAs((PrivilegedExceptionAction<R>) function::get);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void execAsRootSafe(final CheckedProcedure function) {
    ROOT_USER_SIMPLE_AUTH.doAs((PrivilegedAction<Void>) () -> {
      function.call();
      return null;
    });
  }

  public static void execAsRootUnsafe(final UncheckedProcedure function) {
    ROOT_USER_SIMPLE_AUTH.doAs((PrivilegedAction<Void>) () -> {
      try {
        function.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }
}
