package com.yahoo.sketches.hll;

/**
 */
class HllUtils
{
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

  static double invPow2(int e) {
      return Double.longBitsToDouble((0x3ffL - e) << 52);
  }

}
