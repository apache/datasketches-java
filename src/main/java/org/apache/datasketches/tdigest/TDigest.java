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

import java.util.function.Function;

import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * t-Digest for estimating quantiles and ranks.
 * This implementation is based on the following paper:
 * Ted Dunning, Otmar Ertl. Extremely Accurate Quantiles Using t-Digests
 * and the following implementation:
 * https://github.com/tdunning/t-digest
 * This implementation is similar to MergingDigest in the above implementation
 */
public final class TDigest {

  public static final boolean USE_ALTERNATING_SORT = true;
  public static final boolean USE_TWO_LEVEL_COMPRESSION = true;
  public static final boolean USE_WEIGHT_LIMIT = true;

  private boolean reverseMerge_;
  private final int k_;
  private final int internalK_;
  private double minValue_;
  private double maxValue_;
  private int centroidsCapacity_;
  private int numCentroids_;
  private double[] centroidMeans_;
  private long[] centroidWeights_;
  private long totalWeight_;
  private int bufferCapacity_;
  private int numBuffered_;
  private double[] bufferValues_;
  private long[] bufferWeights_;
  private long bufferedWeight_;

  public TDigest(final int k) {
    this(false, k, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null, null, 0);
  }

  private TDigest(final boolean reverseMerge, final int k, final double min, final double max,
      final double[] means, final long[] weights, final long weight) {
    reverseMerge_ = reverseMerge; 
    k_ = k;
    minValue_ = min;
    maxValue_ = max;
    if (k < 10) throw new IllegalArgumentException("k must be at least 10");
    int fudge = 0;
    if (USE_WEIGHT_LIMIT) {
      fudge = 10;
      if (k < 30) fudge += 20;
    }
    centroidsCapacity_ = k_ * 2 + fudge;
    bufferCapacity_ = centroidsCapacity_ * 5;
    double scale = Math.max(1.0, (double) bufferCapacity_ / centroidsCapacity_ - 1.0);
    if (!USE_TWO_LEVEL_COMPRESSION) scale = 1;
    internalK_ = (int) Math.ceil(Math.sqrt(scale) * k_);
    centroidsCapacity_ = Math.max(centroidsCapacity_, internalK_ + fudge);
    bufferCapacity_ = Math.max(bufferCapacity_, centroidsCapacity_ * 2);
    centroidMeans_ = new double[centroidsCapacity_];
    centroidWeights_ = new long[centroidsCapacity_];
    bufferValues_ =  new double[bufferCapacity_];
    bufferWeights_ = new long[bufferCapacity_];
    numCentroids_ = 0;
    numBuffered_ = 0;
    totalWeight_ = 0;
    bufferedWeight_ = 0;
  }

  public int getK() {
    return k_;
  }

  public void update(final double value) {
    if (Double.isNaN(value)) return;
    if (numBuffered_ == bufferCapacity_ - numCentroids_) mergeBuffered();
    bufferValues_[numBuffered_] = value;
    bufferWeights_[numBuffered_] = 1;
    numBuffered_++;
    bufferedWeight_++;
    minValue_ = Math.min(minValue_, value);
    maxValue_ = Math.max(maxValue_, value);
  }

  public void merge(final TDigest other) {
  }

  public void compress() {
    mergeBuffered();
  }

  public boolean isEmpty() {
    return numCentroids_ == 0 && numBuffered_ == 0;
  }

  public double getMinValue() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return minValue_;
  }

  public double getMaxValue() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return maxValue_;
  }

  public long getTotalWeight() {
    return totalWeight_ + bufferedWeight_;
  }

  public double getRank(final double quantile) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return 0;
  }

  public double getQuantile(final double rank) {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return 0;
  }

  /**
   * Serialize this TDigest to a byte array form.
   * @return byte array
   */
  public byte[] toByteArray() {
    return new byte[0];
  }

  /**
   * Returns summary information about this TDigest. Used for debugging.
   * @return summary of the TDigest
   */
  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean printCentroids) {
    String str = "MergingDigest\n"
        + " Nominal Compression: " + k_ + "\n"
        + " Internal Compression: " + internalK_ + "\n"
        + " Centroids: " + numCentroids_ + "\n"
        + " Buffered: " + numBuffered_ + "\n"
        + " Centroids Capacity: " + centroidsCapacity_ + "\n"
        + " Buffer Capacity: " + bufferCapacity_ + "\n"
        + " Total Weight: " + totalWeight_ + "\n"
        + " Unmerged Weight: " + bufferedWeight_ + "\n"
        + " Reverse Merge: " + reverseMerge_ + "\n";
    if (!isEmpty()) {
      str += " Min: " + minValue_ + "\n"
           + " Max: " + maxValue_ + "\n";
    }
    if (printCentroids) {
      if (numCentroids_ > 0) {
        str += "Centroids:\n";
        for (int i = 0; i < numCentroids_; i++) {
          str += i + ": " + centroidMeans_[i] + ", " + centroidWeights_[i] + "\n";
        }
      }
      if (numBuffered_ > 0) {
        str += "Buffer:\n";
        for (int i = 0; i < numBuffered_; i++) {
          str += i + ": " + bufferValues_[i] + ", " + bufferWeights_[i] + "\n";
        }
      }
    }
    return str;
  }

  private void mergeBuffered() {
    if (numBuffered_ == 0) return;
    final boolean reverse = USE_ALTERNATING_SORT & reverseMerge_;
    System.arraycopy(centroidMeans_, 0, bufferValues_, numBuffered_, numCentroids_);
    System.arraycopy(centroidWeights_, 0, bufferWeights_, numBuffered_, numCentroids_);
    numBuffered_ += numCentroids_;
    totalWeight_ += bufferedWeight_;
    numCentroids_ = 0;
    Sort.stableSort(bufferValues_, bufferWeights_, numBuffered_);
    if (reverse) {
      Sort.reverse(bufferValues_, numBuffered_);
      Sort.reverse(bufferWeights_, numBuffered_);
    }
    centroidMeans_[0] = bufferValues_[0];
    centroidWeights_[0] = bufferWeights_[0];
    numCentroids_++;
    int current = 1;
    double weightSoFar = 0;
    final double normalizer = ScaleFunction.normalizer(internalK_, totalWeight_);
    double k1 = ScaleFunction.k(0, normalizer);
    double wLimit = totalWeight_ * ScaleFunction.q(k1 + 1, normalizer);
    while (current != numBuffered_) {
      final double proposedWeight = centroidWeights_[numCentroids_ - 1] + bufferWeights_[current];
      final boolean addThis;
      if (current == 1 || current == numBuffered_ - 1) {
        addThis = false;
      } else if (USE_WEIGHT_LIMIT) {
        final double q0 = weightSoFar / totalWeight_;
        final double q2 = (weightSoFar + proposedWeight) / totalWeight_;
        addThis = proposedWeight <= totalWeight_ * Math.min(ScaleFunction.max(q0, normalizer), ScaleFunction.max(q2, normalizer));
      } else {
        addThis = weightSoFar + proposedWeight <= wLimit;
      }
      if (addThis) { // merge into existing centroid
        centroidWeights_[numCentroids_ - 1] += bufferWeights_[current];
        centroidMeans_[numCentroids_ - 1] += (bufferValues_[current] - centroidMeans_[numCentroids_ - 1]) * bufferWeights_[current] / centroidWeights_[numCentroids_ - 1];
      } else { // copy to a new centroid
        weightSoFar += centroidWeights_[numCentroids_ - 1];
        if (!USE_WEIGHT_LIMIT) {
          k1 = ScaleFunction.k(weightSoFar / totalWeight_, normalizer);
          wLimit = totalWeight_ * ScaleFunction.q(k1 + 1, normalizer);
        }
        centroidMeans_[numCentroids_] = bufferValues_[current];
        centroidWeights_[numCentroids_] = bufferWeights_[current];
        numCentroids_++;
      }
      current++;
    }
    if (reverse) {
      Sort.reverse(centroidMeans_, numCentroids_);
      Sort.reverse(centroidWeights_, numCentroids_);
    }
    reverseMerge_ = !reverseMerge_;
    numBuffered_ = 0;
    bufferedWeight_ = 0;
    minValue_ = Math.min(minValue_, centroidMeans_[0]);
    maxValue_ = Math.max(maxValue_, centroidMeans_[numCentroids_ - 1]);
  }

  /**
   * Generates cluster sizes proportional to q*(1-q).
   * The use of a normalizing function results in a strictly bounded number of clusters no matter how many samples.
   */
  static class ScaleFunction {
    static double k(final double q, final double normalizer) {
      return limit(new Function<Double, Double>() {
        @Override
        public Double apply(Double q) {
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
}
