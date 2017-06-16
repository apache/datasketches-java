/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

/**
 * @author Jon Malkin
 */
abstract class DoublesBufferAccessor {
  abstract double get(final int index);

  abstract double set(final int index, final double value);

  abstract int numItems();

  abstract double[] getArray(int fromIdx, int numItems);

  abstract void putArray(double[] srcArray, int srcIndex,
                         int dstIndex, int numItems);
}
