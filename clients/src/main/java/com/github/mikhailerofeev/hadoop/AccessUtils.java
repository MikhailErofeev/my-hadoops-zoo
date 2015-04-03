package com.github.mikhailerofeev.hadoop;

import com.github.mikhailerofeev.hadoop.fun.CheckedProcedure;
import com.github.mikhailerofeev.hadoop.fun.UncheckedProcedure;
import com.google.common.base.Supplier;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;

/**
 * @author m-erofeev
 * @since 28.03.15
 */
class AccessUtils {

  public static final UserGroupInformation ROOT_USER_SIMPLE_AUTH = UserGroupInformation.createRemoteUser("root");

  public static <R> R execAsRoot(final Supplier<R> function) {
    try {
      return ROOT_USER_SIMPLE_AUTH.doAs(new PrivilegedAction<R>() {
        @Override
        public R run() {
          return function.get();
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void execAsRootSafe(final CheckedProcedure function) {
    ROOT_USER_SIMPLE_AUTH.doAs((PrivilegedAction<Void>) new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        function.call();
        return null;
      }
    });
  }

  public static void execAsRootUnsafe(final UncheckedProcedure function) {
    ROOT_USER_SIMPLE_AUTH.doAs((PrivilegedAction<Void>) new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        try {
          function.call();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    });
  }
}
