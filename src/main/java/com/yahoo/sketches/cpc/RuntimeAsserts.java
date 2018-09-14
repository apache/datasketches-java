/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public final class RuntimeAsserts {

  static void rtAssertTrue(final boolean b) {
    if (!b) {
      throw new SketchesArgumentException("False, expected True.");
    }
  }

  static void rtAssertFalse(final boolean b) {
    if (b) {
      throw new SketchesArgumentException("True, expected False.");
    }
  }

  static void rtAssertEquals(final long a, final long b) {
    if (a != b) {
      throw new SketchesArgumentException(a + " != " + b);
    }
  }

  static void rtAssertEquals(final double a, final double b, final double eps) {
    if (Math.abs(a - b) > eps) {
      throw new SketchesArgumentException("abs(" + a + " - " + b + ") > " + eps);
    }
  }

  static void rtAssertEquals(final boolean a, final boolean b) {
    if (a != b) {
      throw new SketchesArgumentException(a + " != " + b);
    }
  }

  static void rtAssertEquals(final byte[] a, final byte[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + a.length + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) {
          throw new SketchesArgumentException(a[i] + " != " + b[i] + " at index " + i);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

  static void rtAssertEquals(final short[] a, final short[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + a.length + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) {
          throw new SketchesArgumentException(a[i] + " != " + b[i] + " at index " + i);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

  static void rtAssertEquals(final int[] a, final int[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + a.length + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) {
          throw new SketchesArgumentException(a[i] + " != " + b[i] + " at index " + i);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

  static void rtAssertEquals(final long[] a, final long[] b) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + a.length + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (a[i] != b[i]) {
          throw new SketchesArgumentException(a[i] + " != " + b[i] + " at index " + i);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

  static void rtAssertEquals(final float[] a, final float[] b, final float eps) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + a.length + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (Math.abs(a[i] - b[i]) > eps) {
          throw new SketchesArgumentException("abs(" + a[i] + " - " + b[i] + ") > " + eps);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

  static void rtAssertEquals(final double[] a, final double[] b, final double eps) {
    if ((a == null) && (b == null)) { return; }
    if ((a != null) && (b != null)) {
      final int alen = a.length;
      if (alen != b.length) {
        throw new SketchesArgumentException("Array lengths not equal: " + alen + ", " + b.length);
      }
      for (int i = 0; i < alen; i++) {
        if (Math.abs(a[i] - b[i]) > eps) {
          throw new SketchesArgumentException("abs(" + a[i] + " - " + b[i] + ") > " + eps);
        }
      }
    } else {
      throw new SketchesArgumentException("Array " + ((a == null) ? "a" : "b") + " is null");
    }
  }

}
