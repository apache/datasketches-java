/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tdigest;

import java.nio.ByteOrder;
import java.util.function.Function;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * t-Digest for estimating quantiles and ranks.
 * This implementation is based on the following paper:
 * Ted Dunning, Otmar Ertl. Extremely Accurate Quantiles Using t-Digests
 * and the following implementation:
 * https://github.com/tdunning/t-digest
 * This implementation is similar to MergingDigest in the above implementation
 */
public final class TDigestDouble {

  public static final boolean USE_ALTERNATING_SORT = true;
  public static final boolean USE_TWO_LEVEL_COMPRESSION = true;
  public static final boolean USE_WEIGHT_LIMIT = true;

  public static final short DEFAULT_K = 200;

  public static final String LS = System.getProperty("line.separator");

  private boolean reverseMerge_;
  private final short k_;
  private final short internalK_;
  private double minValue_;
  private double maxValue_;
  private int centroidsCapacity_;
  private int numCentroids_;
  private double[] centroidMeans_;
  private long[] centroidWeights_;
  private long centroidsWeight_;
  private int bufferCapacity_;
  private int numBuffered_;
  private double[] bufferValues_;
  private long[] bufferWeights_;
  private long bufferedWeight_;

  private static final byte PREAMBLE_LONGS_EMPTY_OR_SINGLE = 1;
  private static final byte PREAMBLE_LONGS_MULTIPLE = 2;
  private static final byte SERIAL_VERSION = 1;

  private static final int COMPAT_DOUBLE = 1;
  private static final int COMPAT_FLOAT = 2;

  enum Flags { IS_EMPTY, IS_SINGLE_VALUE, REVERSE_MERGE };

  /**
   * Constructor with the default K
   */
  public TDigestDouble() {
    this(DEFAULT_K);
  }

  /**
   * Constructor
   * @param k affects the size of TDigest and its estimation error
   */
  public TDigestDouble(final short k) {
    this(false, k, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null, null, 0);
  }

  /**
   * @return parameter k (compression) that was used to configure this TDigest
   */
  public short getK() {
    return k_;
  }

  /**
   * Update this TDigest with the given value
   * @param value to update the TDigest with
   */
  public void update(final double value) {
    if (Double.isNaN(value)) { return; }
    if (numBuffered_ == bufferCapacity_ - numCentroids_) { mergeBuffered(); }
    bufferValues_[numBuffered_] = value;
    bufferWeights_[numBuffered_] = 1;
    numBuffered_++;
    bufferedWeight_++;
    minValue_ = Math.min(minValue_, value);
    maxValue_ = Math.max(maxValue_, value);
  }

  /**
   * Merge the given TDigest into this one
   * @param other TDigest to merge
   */
  public void merge(final TDigestDouble other) {
    if (other.isEmpty()) { return; }
    final int num = numCentroids_ + numBuffered_ + other.numCentroids_ + other.numBuffered_;
    if (num <= bufferCapacity_) {
      System.arraycopy(other.bufferValues_, 0, bufferValues_, numBuffered_, other.numBuffered_);
      System.arraycopy(other.bufferWeights_, 0, bufferWeights_, numBuffered_, other.numBuffered_);
      numBuffered_ += other.numBuffered_;
      System.arraycopy(other.centroidMeans_, 0, bufferValues_, numBuffered_, other.numCentroids_);
      System.arraycopy(other.centroidWeights_, 0, bufferWeights_, numBuffered_, other.numCentroids_);
      numBuffered_ += other.numCentroids_;
      bufferedWeight_ += other.getTotalWeight();
      minValue_ = Math.min(minValue_, other.minValue_);
      maxValue_ = Math.max(maxValue_, other.maxValue_);
    } else {
      final double[] values = new double[num];
      final long[] weights = new long[num];
      System.arraycopy(bufferValues_, 0, values, 0, numBuffered_);
      System.arraycopy(bufferWeights_, 0, weights, 0, numBuffered_);
      System.arraycopy(other.bufferValues_, 0, values, numBuffered_, other.numBuffered_);
      System.arraycopy(other.bufferWeights_, 0, weights, numBuffered_, other.numBuffered_);
      numBuffered_ += other.numBuffered_;
      System.arraycopy(other.centroidMeans_, 0, values, numBuffered_, other.numCentroids_);
      System.arraycopy(other.centroidWeights_, 0, weights, numBuffered_, other.numCentroids_);
      numBuffered_ += other.numCentroids_;
      merge(values, weights, bufferedWeight_ + other.getTotalWeight(), numBuffered_);
    }
  }

  /**
   * Process buffered values and merge centroids if needed
   */
  public void compress() {
    mergeBuffered();
  }

  /**
   * @return true if TDigest has not seen any data
   */
  public boolean isEmpty() {
    return numCentroids_ == 0 && numBuffered_ == 0;
  }

  /**
   * @return minimum value seen by TDigest
   */
  public double getMinValue() {
    if (isEmpty()) { throw new SketchesStateException(QuantilesAPI.EMPTY_MSG); }
    return minValue_;
  }

  /**
   * @return maximum value seen by TDigest
   */
  public double getMaxValue() {
    if (isEmpty()) { throw new SketchesStateException(QuantilesAPI.EMPTY_MSG); }
    return maxValue_;
  }

  /**
   * @return total weight
   */
  public long getTotalWeight() {
    return centroidsWeight_ + bufferedWeight_;
  }

  /**
   * Compute approximate normalized rank of the given value.
   * @param value to be ranked
   * @return normalized rank (from 0 to 1 inclusive)
   */
  public double getRank(final double value) {
    if (isEmpty()) { throw new SketchesStateException(QuantilesAPI.EMPTY_MSG); }
    if (Double.isNaN(value)) { throw new SketchesArgumentException("Operation is undefined for Nan"); }
    if (value < minValue_) { return 0; }
    if (value > maxValue_) { return 1; }
    if (numCentroids_ + numBuffered_ == 1) { return 0.5; }

    mergeBuffered(); // side effect

    // left tail
    final double firstMean = centroidMeans_[0];
    if (value < firstMean) {
      if (firstMean - minValue_ > 0) {
        if (value == minValue_) { return 0.5 / centroidsWeight_; }
        return (1.0 + (value - minValue_) / (firstMean - minValue_) * (centroidWeights_[0] / 2.0 - 1.0));
      }
      return 0; // should never happen
    }

    // right tail
    final double lastMean = centroidMeans_[numCentroids_ - 1];
    if (value > lastMean) {
      if (maxValue_ - lastMean > 0) {
        if (value == maxValue_) { return 1.0 - 0.5 / centroidsWeight_; }
        return 1.0 - ((1.0 + (maxValue_ - value) / (maxValue_ - lastMean)
            * (centroidWeights_[numCentroids_ - 1] / 2.0 - 1.0)) / centroidsWeight_);
      }
      return 1; // should never happen
    }

    int lower = BinarySearch.lowerBound(centroidMeans_, 0, numCentroids_, value);
    if (lower == numCentroids_) { throw new SketchesStateException("lower == end in getRank()"); }
    int upper = BinarySearch.upperBound(centroidMeans_, lower, numCentroids_, value);
    if (upper == 0) { throw new SketchesStateException("upper == begin in getRank()"); }
    if (value < centroidMeans_[lower]) { lower--; }
    if (upper == numCentroids_ || !(centroidMeans_[upper - 1] < value)) { upper--; }

    double weightBelow = 0;
    int i = 0;
    while (i != lower) { weightBelow += centroidWeights_[i++]; }
    weightBelow += centroidWeights_[lower] / 2.0;

    double weightDelta = 0;
    while (i != upper) { weightDelta += centroidWeights_[i++]; }
    weightDelta -= centroidWeights_[lower] / 2.0;
    weightDelta += centroidWeights_[upper] / 2.0;
    if (centroidMeans_[upper] - centroidMeans_[lower] > 0) {
      return (weightBelow + weightDelta * (value - centroidMeans_[lower])
          / (centroidMeans_[upper] - centroidMeans_[lower])) / centroidsWeight_;
    }
    return (weightBelow + weightDelta / 2.0) / centroidsWeight_;
  }

  /**
   * Compute approximate quantile value corresponding to the given normalized rank
   * @param rank normalized rank (from 0 to 1 inclusive)
   * @return quantile value corresponding to the given rank
   */
  public double getQuantile(final double rank) {
    if (isEmpty()) { throw new SketchesStateException(QuantilesAPI.EMPTY_MSG); }
    if (Double.isNaN(rank)) { throw new SketchesArgumentException("Operation is undefined for Nan"); }
    if (rank < 0 || rank > 1) { throw new SketchesArgumentException("Normalized rank must be within [0, 1]"); } 
    
    mergeBuffered(); // side effect

    if (numCentroids_ == 1) { return centroidMeans_[0]; }

    // at least 2 centroids
    final double weight = rank * centroidsWeight_;
    if (weight < 1) { return minValue_; }
    if (weight > centroidsWeight_ - 1.0) { return maxValue_; }
    final double firstWeight = centroidWeights_[0];
    if (firstWeight > 1 && weight < firstWeight / 2.0) {
      return minValue_ + (weight - 1.0) / (firstWeight / 2.0 - 1.0) * (centroidMeans_[0] - minValue_);
    }
    final double lastWeight = centroidWeights_[numCentroids_ - 1];
    if (lastWeight > 1 && centroidsWeight_ - weight <= lastWeight / 2.0) {
      return maxValue_ + (centroidsWeight_ - weight - 1.0) / (lastWeight / 2.0 - 1.0) * (maxValue_ - centroidMeans_[numCentroids_ - 1]);
    }

    // interpolate between extremes
    double weightSoFar = firstWeight / 2.0;
    for (int i = 0; i < numCentroids_ - 1; i++) {
      final double dw = (centroidWeights_[i] + centroidWeights_[i + 1]) / 2.0;
      if (weightSoFar + dw > weight) {
        // the target weight is between centroids i and i+1
        double leftWeight = 0;
        if (centroidWeights_[i] == 1) {
          if (weight - weightSoFar < 0.5) { return centroidMeans_[i]; }
          leftWeight = 0.5;
        }
        double rightWeight = 0;
        if (centroidWeights_[i + 1] == 1) {
          if (weightSoFar + dw - weight <= 0.5) { return centroidMeans_[i + 1]; }
          rightWeight = 0.5;
        }
        final double w1 = weight - weightSoFar - leftWeight;
        final double w2 = weightSoFar + dw - weight - rightWeight;
        return weightedAverage(centroidMeans_[i], w1, centroidMeans_[i + 1], w2);
      }
      weightSoFar += dw;
    }
    final double w1 = weight - centroidsWeight_ - centroidWeights_[numCentroids_ - 1] / 2.0;
    final double w2 = centroidWeights_[numCentroids_ - 1] / 2.0 - w1;
    return weightedAverage(centroidWeights_[numCentroids_ - 1], w1, maxValue_, w2);
  }

  /**
   * Serialize this TDigest to a byte array form.
   * @return byte array
   */
  public byte[] toByteArray() {
    mergeBuffered(); // side effect
    final byte preambleLongs = isEmpty() || isSingleValue() ? PREAMBLE_LONGS_EMPTY_OR_SINGLE : PREAMBLE_LONGS_MULTIPLE;
    final int sizeBytes = preambleLongs * Long.BYTES
        + (isEmpty() ? 0 : (isSingleValue() ? Double.BYTES : 2 * Double.BYTES + (Double.BYTES + Long.BYTES) * numCentroids_));
    final byte[] bytes = new byte[sizeBytes];
    final WritableBuffer wbuf = WritableMemory.writableWrap(bytes).asWritableBuffer();
    wbuf.putByte(preambleLongs);
    wbuf.putByte(SERIAL_VERSION);
    wbuf.putByte((byte) Family.TDIGEST.getID());
    wbuf.putShort(k_);
    wbuf.putByte((byte) (
      (isEmpty() ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (isSingleValue() ? 1 << Flags.IS_SINGLE_VALUE.ordinal() : 0) |
      (reverseMerge_ ? 1 << Flags.REVERSE_MERGE.ordinal() : 0)
    ));
    wbuf.putShort((short) 0); // unused
    if (isEmpty()) { return bytes; }
    if (isSingleValue()) {
      wbuf.putDouble(minValue_);
      return bytes;
    }
    wbuf.putInt(numCentroids_);
    wbuf.putInt(0); // unused
    wbuf.putDouble(minValue_);
    wbuf.putDouble(maxValue_);
    for (int i = 0; i < numCentroids_; i++) {
      wbuf.putDouble(centroidMeans_[i]);
      wbuf.putLong(centroidWeights_[i]);
    }
    return bytes;
  }

  /**
   * Deserialize TDigest from a given memory.
   * Supports reading format of the reference implementation (autodetected).
   * @param mem instance of Memory
   * @return an instance of TDigest
   */
  public static TDigestDouble heapify(final Memory mem) {
    return heapify(mem, false);
  }

  /**
   * Deserialize TDigest from a given memory. Supports reading compact format
   * with (float, int) centroids as opposed to (double, long) to represent (mean, weight).
   * Supports reading format of the reference implementation (autodetected).
   * @param mem instance of Memory
   * @param isFloat if true the input represents (float, int) format
   * @return an instance of TDigest
   */
  public static TDigestDouble heapify(final Memory mem, final boolean isFloat) {
    final Buffer buff = mem.asBuffer();
    final byte preambleLongs = buff.getByte();
    final byte serialVersion = buff.getByte();
    final byte sketchType = buff.getByte();
    if (sketchType != (byte) Family.TDIGEST.getID()) {
      if (preambleLongs == 0 && serialVersion == 0 && sketchType == 0) { return heapifyCompat(mem); }
      throw new SketchesArgumentException("Sketch type mismatch: expected " + Family.TDIGEST.getID() + ", actual " + sketchType);
    }
    if (serialVersion != SERIAL_VERSION) {
      throw new SketchesArgumentException("Serial version mismatch: expected " + SERIAL_VERSION + ", actual " + serialVersion);
    }
    final short k = buff.getShort();
    final byte flagsByte = buff.getByte();
    final boolean isEmpty = (flagsByte & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    final boolean isSingleValue = (flagsByte & (1 << Flags.IS_SINGLE_VALUE.ordinal())) > 0;
    final byte expectedPreambleLongs = isEmpty || isSingleValue ? PREAMBLE_LONGS_EMPTY_OR_SINGLE : PREAMBLE_LONGS_MULTIPLE;
    if (preambleLongs != expectedPreambleLongs) {
      throw new SketchesArgumentException("Preamble longs mismatch: expected " + expectedPreambleLongs + ", actual " + preambleLongs);
    }
    buff.getShort(); // unused
    if (isEmpty) { return new TDigestDouble(k); }
    final boolean reverseMerge = (flagsByte & (1 << Flags.REVERSE_MERGE.ordinal())) > 0;
    if (isSingleValue) {
      final double value;
      if (isFloat) {
        value = buff.getFloat();
      } else {
        value = buff.getDouble();
      }
      return new TDigestDouble(reverseMerge, k, value, value, new double[] {value}, new long[] {1}, 1);
    }
    final int numCentroids = buff.getInt();
    buff.getInt(); // unused
    final double min;
    final double max;
    if (isFloat) {
      min = buff.getFloat();
      max = buff.getFloat();
    } else {
      min = buff.getDouble();
      max = buff.getDouble();
    }
    final double[] means = new double[numCentroids];
    final long[] weights = new long[numCentroids];
    long totalWeight = 0;
    for (int i = 0; i < numCentroids; i++) {
      means[i] = isFloat ? buff.getFloat() : buff.getDouble();
      weights[i] = isFloat ? buff.getInt() : buff.getLong();
      totalWeight += weights[i];
    }
    return new TDigestDouble(reverseMerge, k, min, max, means, weights, totalWeight);
  }

  // compatibility with the format of the reference implementation
  // default byte order of ByteBuffer is used there, which is big endian
  private static TDigestDouble heapifyCompat(final Memory mem) {
    final Buffer buff = mem.asBuffer(ByteOrder.BIG_ENDIAN);
    final int type = buff.getInt();
    if (type != COMPAT_DOUBLE && type != COMPAT_FLOAT) {
      throw new SketchesArgumentException("unexpected compatibility type " + type);
    }
    if (type == COMPAT_DOUBLE) { // compatibility with asBytes()
      final double min = buff.getDouble();
      final double max = buff.getDouble();
      final short k = (short) buff.getDouble();
      final int numCentroids = buff.getInt();
      final double[] means = new double[numCentroids];
      final long[] weights = new long[numCentroids];
      long totalWeight = 0;
      for (int i = 0; i < numCentroids; i++) {
        weights[i] = (long) buff.getDouble();
        means[i] = buff.getDouble();
        totalWeight += weights[i];
      }
      return new TDigestDouble(false, k, min, max, means, weights, totalWeight);
    }
    // COMPAT_FLOAT: compatibility with asSmallBytes()
    final double min = buff.getDouble(); // reference implementation uses doubles for min and max
    final double max = buff.getDouble();
    final short k = (short) buff.getFloat();
    // reference implementation stores capacities of the array of centroids and the buffer as shorts
    // they can be derived from k in the constructor
    buff.getInt(); // unused
    final int numCentroids = buff.getShort();
    final double[] means = new double[numCentroids];
    final long[] weights = new long[numCentroids];
    long totalWeight = 0;
    for (int i = 0; i < numCentroids; i++) {
      weights[i] = (long) buff.getFloat();
      means[i] = buff.getFloat();
      totalWeight += weights[i];
    }
    return new TDigestDouble(false, k, min, max, means, weights, totalWeight);
  }

  /**
   * Human-readable summary of this TDigest as a string
   * @return summary of this TDigest
   */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Human-readable summary of this TDigest as a string
   * @param printCentroids if true append the list of centroids with weights
   * @return summary of this TDigest
   */
  public String toString(final boolean printCentroids) {
    final StringBuilder sb = new StringBuilder();

    sb.append("MergingDigest").append(LS)
      .append(" Nominal Compression: ").append(k_).append(LS)
      .append(" Internal Compression: ").append(internalK_).append(LS)
      .append(" Centroids: ").append(numCentroids_).append(LS)
      .append(" Buffered: ").append(numBuffered_).append(LS)
      .append(" Centroids Capacity: ").append(centroidsCapacity_).append(LS)
      .append(" Buffer Capacity: ").append(bufferCapacity_).append(LS)
      .append("Centroids Weight: ").append(centroidsWeight_).append(LS)
      .append(" Buffered Weight: ").append(bufferedWeight_).append(LS)
      .append(" Total Weight: ").append(getTotalWeight()).append(LS)
      .append(" Reverse Merge: ").append(reverseMerge_).append(LS);
    if (!isEmpty()) {
      sb.append(" Min: ").append(minValue_).append(LS)
        .append(" Max: ").append(maxValue_).append(LS);
    }
    if (printCentroids) {
      if (numCentroids_ > 0) {
        sb.append("Centroids:").append(LS);
        for (int i = 0; i < numCentroids_; i++) {
          sb.append(i).append(": ").append(centroidMeans_[i]).append(", ").append(centroidWeights_[i]).append(LS);
        }
      }
      if (numBuffered_ > 0) {
        sb.append("Buffer:").append(LS);
        for (int i = 0; i < numBuffered_; i++) {
          sb.append(i).append(": ").append(bufferValues_[i]).append(", ").append(bufferWeights_[i]).append(LS);
        }
      }
    }
    return sb.toString();
  }

  private TDigestDouble(final boolean reverseMerge, final short k, final double min, final double max,
      final double[] means, final long[] weights, final long weight) {
    reverseMerge_ = reverseMerge; 
    k_ = k;
    minValue_ = min;
    maxValue_ = max;
    if (k < 10) { throw new SketchesArgumentException("k must be at least 10"); }
    int fudge = 0;
    if (USE_WEIGHT_LIMIT) {
      fudge = 10;
      if (k < 30) { fudge += 20; }
    }
    centroidsCapacity_ = k_ * 2 + fudge;
    bufferCapacity_ = centroidsCapacity_ * 5;
    double scale = Math.max(1.0, (double) bufferCapacity_ / centroidsCapacity_ - 1.0);
    if (!USE_TWO_LEVEL_COMPRESSION) { scale = 1; }
    internalK_ = (short) Math.ceil(Math.sqrt(scale) * k_);
    centroidsCapacity_ = Math.max(centroidsCapacity_, internalK_ + fudge);
    bufferCapacity_ = Math.max(bufferCapacity_, centroidsCapacity_ * 2);
    centroidMeans_ = new double[centroidsCapacity_];
    centroidWeights_ = new long[centroidsCapacity_];
    bufferValues_ =  new double[bufferCapacity_];
    bufferWeights_ = new long[bufferCapacity_];
    numCentroids_ = 0;
    numBuffered_ = 0;
    centroidsWeight_ = weight;
    bufferedWeight_ = 0;
    if (means != null && weights != null) {
      System.arraycopy(means, 0, centroidMeans_, 0, means.length);
      System.arraycopy(weights, 0, centroidWeights_, 0, weights.length);
      numCentroids_ = means.length;
    }
  }

  private void mergeBuffered() {
    if (numBuffered_ == 0) { return; }
    merge(bufferValues_, bufferWeights_, bufferedWeight_, numBuffered_);
  }

  // assumes that there is enough room in the input arrays to add centroids from this TDigest
  private void merge(final double[] values, final long[] weights, final long weight, int num) {
    final boolean reverse = USE_ALTERNATING_SORT & reverseMerge_;
    System.arraycopy(centroidMeans_, 0, values, num, numCentroids_);
    System.arraycopy(centroidWeights_, 0, weights, num, numCentroids_);
    num += numCentroids_;
    centroidsWeight_ += weight;
    numCentroids_ = 0;
    Sort.stableSort(values, weights, num);
    if (reverse) { // this might be avoidable if stableSort could be implemented with a boolean parameter to invert the logic
      Sort.reverse(values, num);
      Sort.reverse(weights, num);
    }
    centroidMeans_[0] = values[0];
    centroidWeights_[0] = weights[0];
    numCentroids_++;
    int current = 1;
    double weightSoFar = 0;
    final double normalizer = ScaleFunction.normalizer(internalK_, centroidsWeight_);
    double k1 = ScaleFunction.k(0, normalizer);
    double wLimit = centroidsWeight_ * ScaleFunction.q(k1 + 1, normalizer);
    while (current != num) {
      final double proposedWeight = centroidWeights_[numCentroids_ - 1] + weights[current];
      final boolean addThis;
      if (current == 1 || current == num - 1) {
        addThis = false;
      } else if (USE_WEIGHT_LIMIT) {
        final double q0 = weightSoFar / centroidsWeight_;
        final double q2 = (weightSoFar + proposedWeight) / centroidsWeight_;
        addThis = proposedWeight <= centroidsWeight_ * Math.min(ScaleFunction.max(q0, normalizer), ScaleFunction.max(q2, normalizer));
      } else {
        addThis = weightSoFar + proposedWeight <= wLimit;
      }
      if (addThis) { // merge into existing centroid
        centroidWeights_[numCentroids_ - 1] += weights[current];
        centroidMeans_[numCentroids_ - 1] += (values[current] - centroidMeans_[numCentroids_ - 1])
            * weights[current] / centroidWeights_[numCentroids_ - 1];
      } else { // copy to a new centroid
        weightSoFar += centroidWeights_[numCentroids_ - 1];
        if (!USE_WEIGHT_LIMIT) {
          k1 = ScaleFunction.k(weightSoFar / centroidsWeight_, normalizer);
          wLimit = centroidsWeight_ * ScaleFunction.q(k1 + 1, normalizer);
        }
        centroidMeans_[numCentroids_] = values[current];
        centroidWeights_[numCentroids_] = weights[current];
        numCentroids_++;
      }
      current++;
    }
    if (reverse) {
      Sort.reverse(centroidMeans_, numCentroids_);
      Sort.reverse(centroidWeights_, numCentroids_);
    }
    numBuffered_ = 0;
    bufferedWeight_ = 0;
    reverseMerge_ = !reverseMerge_;
    minValue_ = Math.min(minValue_, centroidMeans_[0]);
    maxValue_ = Math.max(maxValue_, centroidMeans_[numCentroids_ - 1]);
  }

  private boolean isSingleValue() {
    return getTotalWeight() == 1;
  }

  /*
   * Generates cluster sizes proportional to q*(1-q).
   * The use of a normalizing function results in a strictly bounded number of clusters no matter how many samples.
   * Corresponds to K_2 in the reference implementation
   */
  private static class ScaleFunction {
    static double k(final double q, final double normalizer) {
      return limit(new Function<Double, Double>() {
        @Override
        public Double apply(final Double q) {
          return Math.log(q / (1 - q)) * normalizer;
        }
      }, q, 1e-15, 1 - 1e-15);
    }

    static double q(final double k, final double normalizer) {
      final double w = Math.exp(k / normalizer);
      return w / (1 + w);
    }

    static double max(final double q, final double normalizer) {
      return q * (1 - q) / normalizer;
    }

    static double normalizer(final double compression, final double n) {
      return compression / z(compression, n);
    }

    static double z(final double compression, final double n) {
      return 4 * Math.log(n / compression) + 24;
    }

    static double limit(final Function<Double, Double> f, final double x, final double low, final double high) {
      if (x < low) {
        return f.apply(low);
      } else if (x > high) {
        return f.apply(high);
      }
      return f.apply(x);
    }
  }
  
  private static double weightedAverage(final double x1, final double w1, final double x2, final double w2) {
    return (x1 * w1 + x2 * w2) / (w1 + w2);
  }
}
