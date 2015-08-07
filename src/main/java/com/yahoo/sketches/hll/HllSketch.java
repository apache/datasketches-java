package com.yahoo.sketches.hll;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.hash.MurmurHash3;

public class HllSketch
{
  // derived using some formulas in Ting's paper (link?)
  private static final double HIP_REL_ERROR_NUMER = 0.836083874576235;

  public static HllSketchBuilder builder() {
    return new HllSketchBuilder();
  }

  private Fields.UpdateCallback updateCallback;
  private final Preamble preamble;

  private Fields fields;

  public HllSketch(Fields fields) {
    this.fields = fields;
    this.updateCallback = new NoopUpdateCallback();
    this.preamble = fields.getPreamble();
  }

  public void update(byte[] key) {
    updateWithHash(MurmurHash3.hash(key, Util.DEFAULT_UPDATE_SEED));
  }

  public void update(int[] key) {
    updateWithHash(MurmurHash3.hash(key, Util.DEFAULT_UPDATE_SEED));
  }

  public void update(long[] key) {
    updateWithHash(MurmurHash3.hash(key, Util.DEFAULT_UPDATE_SEED));
  }

  public double getEstimate() {
    double rawEst = getRawEstimate();
    int logK = preamble.getLogConfigK();

    double[] x_arr = Interpolation.interpolation_x_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];
    double[] y_arr = Interpolation.interpolation_y_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];

    if (rawEst < x_arr[0]) {
      return 0;
    }
    if (rawEst > x_arr[x_arr.length - 1]) {
      return rawEst;
    }

    double adjEst = Interpolation.cubicInterpolateUsingTable(x_arr, y_arr, rawEst);
    int configK = preamble.getConfigK();

    if (adjEst > 3.0 * configK) {
      return adjEst;
    }

    double linEst = getLinearEstimate();
    double avgEst = (adjEst + linEst) / 2.0;

    /* the following constant 0.64 comes from empirical measurements (see below) of the
       crossover point between the average error of the linear estimator and the adjusted hll estimator */
    if (avgEst > 0.64 * configK) {
      return adjEst;
    }
    return linEst;
  }

  public double getUpperBound(double numStdDevs) {
    return getEstimate() / (1.0 - eps(numStdDevs));
  }

  public double getLowerBound(double numStdDevs) {
    double lowerBound = getEstimate() / (1.0 + eps(numStdDevs));
    double numNonZeros = (double) preamble.getConfigK();
    numNonZeros -= numBucketsAtZero();
    if (lowerBound < numNonZeros) {
      return numNonZeros;
    }
    return lowerBound;
  }

  private double getRawEstimate() {
    int numBuckets = preamble.getConfigK();
    double correctionFactor = 0.7213 / (1.0 + 1.079 / (double) numBuckets);
    correctionFactor *= numBuckets * numBuckets;
    correctionFactor /= inversePowerOf2Sum();
    return correctionFactor;
  }

  private double getLinearEstimate() {
    int configK = preamble.getConfigK();
    long longV = numBucketsAtZero();
    if (longV == 0) {
      return configK * Math.log(configK / 0.5);
    }
    return (configK * (HarmonicNumbers.harmonicNumber(configK) - HarmonicNumbers.harmonicNumber(longV)));
  }

  public HllSketch union(HllSketch that) {
    BucketIterator iter = that.fields.getBucketIterator();
    while (iter.next()) {
      fields = fields.updateBucket(iter.getKey(), iter.getValue(), updateCallback);
    }
    return this;
  }

  private void updateWithHash(long[] hash) {
    byte newValue = (byte) (Long.numberOfLeadingZeros(hash[1]) + 1);
    int slotno = (int) hash[0] & (preamble.getConfigK() - 1);
    fields = fields.updateBucket(slotno, newValue, updateCallback);
  }

  private double eps(double numStdDevs) {
    return numStdDevs * HIP_REL_ERROR_NUMER / Math.sqrt(preamble.getConfigK());
  }

  public byte[] toByteArray() {
    int numBytes = (preamble.getPreambleSize() << 3) + fields.numBytesToSerialize();
    byte[] retVal = new byte[numBytes];

    fields.intoByteArray(retVal, preamble.intoByteArray(retVal, 0));

    return retVal;
  }

  public byte[] toByteArrayNoPreamble() {
    byte[] retVal = new byte[fields.numBytesToSerialize()];
    fields.intoByteArray(retVal, 0);
    return retVal;
  }

  public HllSketch asCompact() {
    return new HllSketch(fields.toCompact());
  }

  public int numBuckets() {
    return preamble.getConfigK();
  }

  /**
   * Get the update Callback.  This is protected because it is intended that only children might *call*
   * the method.  It is not expected that this would be overridden by a child class.  If someone overrides
   * it and weird things happen, the bug lies in the fact that it was overridden.
   *
   * @return The updateCallback registered with the HllSketch
   */
  protected Fields.UpdateCallback getUpdateCallback()
  {
    return updateCallback;
  }

  /**
   * Set the update callback.  This is protected because it is intended that only children might *call*
   * the method.  It is not expected that this would be overridden by a child class.  If someone overrides
   * it and weird things happen, the bug lies in the fact that it was overridden.
   *
   * @param updateCallback the update callback for the HllSketch to use when talking with its Fields
   */
  protected void setUpdateCallback(Fields.UpdateCallback updateCallback)
  {
    this.updateCallback = updateCallback;
  }

  /** Helper methods that are potential extension points for children **/

  protected double inversePowerOf2Sum() {
    return HllUtils.computeInvPow2Sum(numBuckets(), fields.getBucketIterator());
  }

  protected int numBucketsAtZero() {
    int retVal = 0;
    int count = 0;

    BucketIterator bucketIter = fields.getBucketIterator();
    while (bucketIter.next()) {
      if (bucketIter.getValue() == 0) {
        ++retVal;
      }
      ++count;
    }

    // All skipped buckets are 0.
    retVal += fields.getPreamble().getConfigK() - count;

    return retVal;
  }
}
