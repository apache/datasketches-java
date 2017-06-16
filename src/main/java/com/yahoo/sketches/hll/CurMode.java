/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * Represents the three fundamental modes of the HLL Sketch.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
enum CurMode { LIST, SET, HLL; //do not change the order.

  public static final CurMode values[] = values();

  /**
   * Returns the CurMode given its ordinal
   * @param ordinal the order of appearance in the enum definition.
   * @return the CurMode given its ordinal
   */
  public static CurMode fromOrdinal(final int ordinal) {
    return values[ordinal];
  }


}
