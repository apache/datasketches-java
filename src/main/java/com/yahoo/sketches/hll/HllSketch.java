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

  private final Fields.UpdateCallback updateCallback;
  private final Preamble preamble;

  private Fields fields;

  public HllSketch(Fields fields) {
    this(fields, new NoopUpdateCallback());
  }

  public HllSketch(Fields fields, Fields.UpdateCallback updateCallback) {
    this.fields = fields;
    this.updateCallback = updateCallback;
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
    return getUserVisibleEstimate();
  }

  public double getUpperBound(double numStdDevs) {
    return getCompositeEstimate() / (1.0 - eps(numStdDevs));
  }

  public double getLowerBound(double numStdDevs) {
    double lowerBound = getCompositeEstimate() / (1.0 + eps(numStdDevs));
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
    correctionFactor /= HllUtils.computeInvPow2Sum(numBuckets, fields.getBucketIterator());
    return correctionFactor;
  }

  public double getLinearEstimate() {
    int configK = preamble.getConfigK();
    long longV = numBucketsAtZero();
    if (longV == 0) {
      return configK * Math.log(configK / 0.5);
    }
    return (configK * (HarmonicNumbers.harmonicNumber(configK) - HarmonicNumbers.harmonicNumber(longV)));
  }

  /**
   * This is the (non-HIP) estimator that is exported to users
   * It is called "composite" because multiple estimators are pasted together
   *
   * @return
   */
  public double getCompositeEstimate() {
    double rawEst = getRawEstimate();
    int configK = preamble.getConfigK();
    int logK = Util.simpleIntlog2(configK);
    assert (logK >= Interpolation.INTERPOLATION_MIN_LOG_K && logK <= Interpolation.INTERPOLATION_MAX_LOG_K);
    double[] x_arr = Interpolation.interpolation_x_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];
    double[] y_arr = Interpolation.interpolation_y_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];
    if (rawEst < x_arr[0]) return 0;
    if (rawEst > x_arr[x_arr.length - 1]) return rawEst;
    double adjEst = Interpolation.cubicInterpolateUsingTable(x_arr, y_arr, rawEst);
    if (adjEst > 3.0 * configK) return adjEst;
    double linEst = getLinearEstimate();
    double avgEst = (adjEst + linEst) / 2.0;
    /* the following constant 0.64 comes from empirical measurements (see below) of the 
       crossover point between the average error of the linear estimator and the adjusted hll estimator */
    if (avgEst > 0.64 * configK) return adjEst;
    return linEst;
  }

  public double getUserVisibleEstimate() {
    return getCompositeEstimate();
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

  int numBucketsAtZero() {
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
