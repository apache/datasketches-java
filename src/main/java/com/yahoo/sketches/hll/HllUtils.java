package com.yahoo.sketches.hll;

/**
 */
public class HllUtils
{
  static double[] invPow2Table = new double[256];
  static {
    for (int i = 0; i < 256; i++) invPow2Table[i] = Math.pow(2.0, -1.0 * i);
  }

  public static double computeInvPow2Sum(int numBuckets, BucketIterator iter) {
    double retVal = 0;
    while (iter.next()) {
      retVal += invPow2Table[iter.getValue()];
      --numBuckets;
    }
    retVal += numBuckets * invPow2Table[0];
    return retVal;
  }

}
