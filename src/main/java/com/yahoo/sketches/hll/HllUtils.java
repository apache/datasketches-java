/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

/**
 * Utility functions for the HLL package
 * 
 * @author Kevin Lang
 */
class HllUtils {

  static double computeInvPow2Sum(int numBuckets, BucketIterator iter) {
    double retVal = 0;
    while (iter.next()) {
      retVal += invPow2(iter.getValue());
      --numBuckets;
    }
    retVal += numBuckets;
    return retVal;
  }

  static Fields unionBucketIterator(Fields fields, BucketIterator iter, Fields.UpdateCallback updateCallback) {
    while (iter.next()) {
      fields = fields.updateBucket(iter.getKey(), iter.getValue(), updateCallback);
    }
    return fields;
  }

  /**
   * Computes the inverse integer power of 2: 1/(2^e) == 2^(-e). 
   * @param e a positive value between 0 and 1023 inclusive
   * @return  the inverse integer power of 2: 1/(2^e) == 2^(-e)
   */
  static final double invPow2(int e) {
    assert (e | (1024 - e -1)) >= 0 : "e cannot be negative or greater than 1023: "+ e;
    return Double.longBitsToDouble((0x3ffL - e) << 52); //suggested by Otmar Ertl
  }
  
}
