/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

/**
 * These asserts act like the <i>assert</i> in C/C++.  They will throw an AssertionError whether the
 * JVM -ea is set or not.
 *
 * @author Lee Rhodes
 */
final class RuntimeAsserts {

  static void rtAssert(final boolean b) {
    if (!b) { error("False, expected True."); }
  }

  static void rtAssertFalse(final boolean b) {
    if (b) { error("True, expected False."); }
  }

  static void rtAssertEquals(final long a, final long b) {
    if (a != b) { error(a + " != " + b); }
  }

  static void rtAssertEquals(final double a, final double b, final double eps) {
    if (Math.abs(a - b) > eps) { error("abs(" + a + " - " + b + ") > " + eps); }
  }

  static void rtAssertEquals(final boolean a, final boolean b) {
    if (a != b) { error(a + " != " + b); }
  }

  static void rtAssertEquals(final byte[] a, final byte[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + a.length + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) { error(a[i] + " != " + b[i] + " at index " + i); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  static void rtAssertEquals(final short[] a, final short[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + a.length + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) { error(a[i] + " != " + b[i] + " at index " + i); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  static void rtAssertEquals(final int[] a, final int[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + a.length + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) { error(a[i] + " != " + b[i] + " at index " + i); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  static void rtAssertEquals(final long[] a, final long[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + a.length + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) { error(a[i] + " != " + b[i] + " at index " + i); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  static void rtAssertEquals(final float[] a, final float[] b, final float eps) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + a.length + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (Math.abs(a[i] - b[i]) > eps) { error("abs(" + a[i] + " - " + b[i] + ") > " + eps); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  static void rtAssertEquals(final double[] a, final double[] b, final double eps) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) { error("Array lengths not equal: " + alen + ", " + b.length); }
      for (int i = 0; i < alen; i++) {
        if (Math.abs(a[i] - b[i]) > eps) { error("abs(" + a[i] + " - " + b[i] + ") > " + eps); }
      }
    } else { error("Array " + ((a == null) ? "a" : "b") + " is null"); }
  }

  private static void error(final String message) {
    throw new AssertionError(message);
  }
}
