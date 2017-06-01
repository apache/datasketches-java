/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.WritableMemory;

/**
 * Top-level class for the HLL family of sketches.
 * Use the HllSketchBuilder to construct this class.
 *
 */
public class HllSketch {
  private static final double HLL_REL_ERROR_NUMER = 1.04;
  private Fields.UpdateCallback updateCallback;
  private final Preamble preamble;
  private Fields fields;
  static final String LS = System.getProperty("line.separator");

  /**
   * Construct this class with the given Fields
   * @param fields the given Fields
   */
  public HllSketch(final Fields fields) {
    this.fields = fields;
    updateCallback = new Fields.UpdateCallback() {
      @Override
      public void bucketUpdated(final int bucket, final byte oldVal, final byte newVal) {
        //intentionally empty
      }
    };
    preamble = fields.getPreamble();
  }

  /**
   * Present this sketch with a long.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given double (or float) datum.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   */
  public void update(final double datum) {
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given String.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * @param datum The given String.
   */
  public void update(final String datum) {
    if ((datum == null) || datum.isEmpty()) {
      return;
    }
    final byte[] data = datum.getBytes(UTF_8);
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given byte array.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   */
  public void update(final byte[] data) {
    if ((data == null) || (data.length == 0)) {
      return;
    }
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given char array.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given char array.
   */
  public void update(final char[] data) {
    if ((data == null) || (data.length == 0)) {
      return;
    }
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given integer array.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   */
  public void update(final int[] data) {
    if ((data == null) || (data.length == 0)) {
      return;
    }
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Present this sketch with the given long array.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   */
  public void update(final long[] data) {
    if ((data == null) || (data.length == 0)) {
      return;
    }
    updateWithHash(hash(data, DEFAULT_UPDATE_SEED));
  }

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public double getEstimate() {
    final double rawEst = getRawEstimate();
    final int logK = preamble.getLogConfigK();

    final double[] x_arr =
        Interpolation.interpolation_x_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];
    final double[] y_arr =
        Interpolation.interpolation_y_arrs[logK - Interpolation.INTERPOLATION_MIN_LOG_K];

    if (rawEst < x_arr[0]) {
      return 0;
    }
    if (rawEst > x_arr[x_arr.length - 1]) {
      return rawEst;
    }

    final double adjEst = Interpolation.cubicInterpolateUsingTable(x_arr, y_arr, rawEst);
    final int configK = preamble.getConfigK();

    if (adjEst > (3.0 * configK)) {
      return adjEst;
    }

    final double linEst = getLinearEstimate();
    final double avgEst = (adjEst + linEst) / 2.0;

    // The following constant 0.64 comes from empirical measurements (see below) of the crossover
    //   point between the average error of the linear estimator and the adjusted hll estimator
    if (avgEst > (0.64 * configK)) {
      return adjEst;
    }
    return linEst;
  }

  /**
   * Gets the upper bound with respect to the Estimate
   * @param numStdDevs the number of standard deviations from the Estimate
   * @return the upper bound
   */
  public double getUpperBound(final double numStdDevs) {
    return getEstimate() / (1.0 - eps(numStdDevs));
  }

  /**
   * Gets the lower bound with respect to the Estimate
   * @param numStdDevs the number of standard deviations from the Estimate
   * @return the lower bound
   */
  public double getLowerBound(final double numStdDevs) {
    final double lowerBound = getEstimate() / (1.0 + eps(numStdDevs));
    double numNonZeros = preamble.getConfigK();
    numNonZeros -= numBucketsAtZero();
    if (lowerBound < numNonZeros) {
      return numNonZeros;
    }
    return lowerBound;
  }

  /**
   * Union this sketch with that one
   * @param that the other sketch
   * @return this sketch
   */
  public HllSketch union(final HllSketch that) {
    fields = that.fields.unionInto(fields, updateCallback);
    return this;
  }

  /**
   * Serializes this sketch to a byte array
   * @return this sketch as a byte array
   */
  public byte[] toByteArray() {
    final int numBytes = (preamble.getPreambleLongs() << 3) + fields.numBytesToSerialize();
    final byte[] retVal = new byte[numBytes];

    fields.intoByteArray(retVal, preamble.intoByteArray(retVal, 0));

    return retVal;
  }

  /**
   * Serializes this sketch without the Preamble as a byte array
   * @return this sketch without the Preamble as a byte array
   */
  public byte[] toByteArrayNoPreamble() {
    final byte[] retVal = new byte[fields.numBytesToSerialize()];
    fields.intoByteArray(retVal, 0);
    return retVal;
  }

  /**
   * Returns this sketch in compact form
   * @return this sketch in compact form
   */
  public HllSketch asCompact() {
    return new HllSketch(fields.toCompact());
  }

  /**
   * Returns the number of configured buckets (k)
   * @return the number of configured buckets (k)
   */
  public int numBuckets() {
    return preamble.getConfigK();
  }

  /**
   * Gets the Preamble of this sketch
   * @return the Preamble of this sketch
   */
  public Preamble getPreamble() {
    return preamble;
  }

  /**
   * Returns an HllSketchBuilder
   * @return an HllSketchBuilder
   */
  public static HllSketchBuilder builder() {
    return new HllSketchBuilder();
  }

  /**
   * Deserialize HllSketch from bytes.
   * @param bytes the given byte array
   * @return HllSketch
   */
  public static HllSketch fromBytes(final byte[] bytes) {
    return fromBytes(bytes, 0, bytes.length);
  }

  /**
   * Deserialize HllSketch from bytes
   * @param bytes the given byte array
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @return HllSketch
   */
  public static HllSketch fromBytes( //only called from above. Why is this needed? //TODO
      final byte[] bytes,
      final int startOffset,
      final int endOffset) {
    final WritableMemory reg =
        WritableMemory.wrap(bytes).writableRegion(startOffset, endOffset - startOffset);
    final Preamble preamble = Preamble.fromMemory(reg); //extracts the preamble
    return fromBytes(preamble, bytes, (startOffset + preamble.getPreambleLongs()) << 3, endOffset);
  }

  /**
   * Deserializes a HllSketch from bytes.
   * @param preamble the given preamble
   * @param bytes the byte array
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @return HllSketch
   */
  public static HllSketch fromBytes( //only called from above. Why is this needed? //TODO
      final Preamble preamble,
      final byte[] bytes, //the whole array including preamble
      final int startOffset,
      final int endOffset) {
    final Fields fields = FieldsFactories.fromBytes(preamble, bytes, startOffset, endOffset);
    return preamble.isHip() ? new HipHllSketch(fields) : new HllSketch(fields);
  }

  /**
   * Returns a simple or detailed summary of this sketch
   * @param detail if true, lists the detail of the internal arrays.
   * @return a simple or detailed summary of this sketch
   */
  public String toString(final boolean detail) {
    final String thisSimpleName = this.getClass().getSimpleName();
    final StringBuilder sb = new StringBuilder();
    sb.append("## ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append(fields.toString(detail));
    return sb.toString();
  }

  //Restricted

  /**
   * Set the update callback. It is final so that it can not be overridden.
   *
   * @param updateCallback the update callback for the HllSketch to use when talking with its Fields
   */
  protected final void setUpdateCallback(final Fields.UpdateCallback updateCallback) {
    this.updateCallback = updateCallback;
  }

  //Helper methods that are potential extension points for children

  /**
   * The sum of the inverse powers of 2
   *
   * @return the sum of the inverse powers of 2
   */
  protected double inversePowerOf2Sum() {
    return HllUtils.computeInvPow2Sum(numBuckets(), fields.getBucketIterator());
  }

  protected int numBucketsAtZero() {
    int retVal = 0;
    int count = 0;

    final BucketIterator bucketIter = fields.getBucketIterator();
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

  private double getRawEstimate() {
    final int numBuckets = preamble.getConfigK();
    double correctionFactor = 0.7213 / (1.0 + (1.079 / numBuckets));
    correctionFactor *= numBuckets * numBuckets;
    correctionFactor /= inversePowerOf2Sum();
    return correctionFactor;
  }

  private double getLinearEstimate() {
    final int configK = preamble.getConfigK();
    final long longV = numBucketsAtZero();
    if (longV == 0) {
      return configK * Math.log(configK / 0.5);
    }
    return (configK * (HarmonicNumbers.harmonicNumber(configK) - HarmonicNumbers.harmonicNumber(longV)));
  }

  private void updateWithHash(final long[] hash) {
    final int slotno = (int) hash[0] & (preamble.getConfigK() - 1);
    final int lz = Long.numberOfLeadingZeros(hash[1]);
    final byte value = (byte) ((lz > 62 ? 62 : lz) + 1);
    fields = fields.updateBucket(slotno, value, updateCallback);
  }

  private double eps(final double numStdDevs) {
    return (numStdDevs * HLL_REL_ERROR_NUMER) / Math.sqrt(preamble.getConfigK());
  }

}
