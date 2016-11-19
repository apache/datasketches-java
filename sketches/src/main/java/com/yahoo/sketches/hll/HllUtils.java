/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.invPow2;

/**
 * Utility functions for the HLL package
 *
 * @author Kevin Lang
 */
final class HllUtils {

  private HllUtils() {}

  static double computeInvPow2Sum(int numBuckets, final BucketIterator iter) {
    double retVal = 0;
    while (iter.next()) {
      retVal += invPow2(iter.getValue());
      --numBuckets;
    }
    retVal += numBuckets;
    return retVal;
  }

  static Fields unionBucketIterator(
      Fields fields, final BucketIterator iter, final Fields.UpdateCallback updateCallback) {
    while (iter.next()) {
      fields = fields.updateBucket(iter.getKey(), iter.getValue(), updateCallback);
    }
    return fields;
  }

}
