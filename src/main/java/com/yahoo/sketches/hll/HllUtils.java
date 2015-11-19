package com.yahoo.sketches.hll;

/**
 */
class HllUtils
{
  static double computeInvPow2Sum(int numBuckets, BucketIterator iter) {
    double retVal = 0;
    while (iter.next()) {
      retVal += Math.scalb(1d, -iter.getValue());
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

}
