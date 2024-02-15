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
  private long centroidsWeight_;
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
    centroidsWeight_ = 0;
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
    if (other.isEmpty()) return;
    int num = numCentroids_ + numBuffered_ + other.numCentroids_ + other.numBuffered_;
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
      merge(values, weights, bufferedWeight_ + other.centroidsWeight_, numBuffered_);
    }
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
    return centroidsWeight_ + bufferedWeight_;
  }

  public double getRank(final double value) {
    if (isEmpty()) throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG);
    if (Double.isNaN(value)) throw new IllegalArgumentException("Operation is undefined for Nan");
    if (value < minValue_) return 0;
    if (value > maxValue_) return 1;
    if (numCentroids_ + numBuffered_ == 1) return 0.5;

    mergeBuffered(); // side effect

    // left tail
    final double firstMean = centroidMeans_[0];
    if (value < firstMean) {
      if (firstMean - minValue_ > 0) {
        if (value == minValue_) return 0.5 / centroidsWeight_;
        return (1.0 + (value - minValue_) / (firstMean - minValue_) * (centroidWeights_[0] / 2.0 - 1.0));
      }
      return 0; // should never happen
    }

    // right tail
    final double lastMean = centroidMeans_[numCentroids_ - 1];
    if (value > lastMean) {
      if (maxValue_ - lastMean > 0) {
        if (value == maxValue_) return 1.0 - 0.5 / centroidsWeight_;
        return 1.0 - ((1.0 + (maxValue_ - value) / (maxValue_ - lastMean) * (centroidWeights_[numCentroids_ - 1] / 2.0 - 1.0)) / centroidsWeight_);
      }
      return 1; // should never happen
    }

    int lower = BinarySearch.lowerBound(centroidMeans_, 0, numCentroids_, value);
    if (lower == numCentroids_) throw new RuntimeException("lower == end in getRank()");
    int upper = BinarySearch.upperBound(centroidMeans_, lower, numCentroids_, value);
    if (upper == 0) throw new RuntimeException("upper == begin in getRank()");
    if (value < centroidMeans_[lower]) lower--;
    if (upper == numCentroids_ || !(centroidMeans_[upper - 1] < value)) upper--;

    double weightBelow = 0;
    int i = 0;
    while (i != lower) weightBelow += centroidWeights_[i++];
    weightBelow += centroidWeights_[lower] / 2.0;

    double weightDelta = 0;
    while (i != upper) weightDelta += centroidWeights_[i++];
    weightDelta -= centroidWeights_[lower] / 2.0;
    weightDelta += centroidWeights_[upper] / 2.0;
    if (centroidMeans_[upper] - centroidMeans_[lower] > 0) {
      return (weightBelow + weightDelta * (value - centroidMeans_[lower]) / (centroidMeans_[upper] - centroidMeans_[lower])) / centroidsWeight_;
    }
    return (weightBelow + weightDelta / 2.0) / centroidsWeight_;
  }

  public double getQuantile(final double rank) {
    if (isEmpty()) throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG);
    if (Double.isNaN(rank)) throw new IllegalArgumentException("Operation is undefined for Nan");
    if (rank < 0 || rank > 1) throw new IllegalArgumentException("Normalized rank must be within [0, 1]"); 
    
    mergeBuffered(); // side effect

    if (numCentroids_ == 1) return centroidMeans_[0];

    // at least 2 centroids
    final double weight = rank * centroidsWeight_;
    if (weight < 1) return minValue_;
    if (weight > centroidsWeight_ - 1.0) return maxValue_;
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
          if (weight - weightSoFar < 0.5) return centroidMeans_[i];
          leftWeight = 0.5;
        }
        double rightWeight = 0;
        if (centroidWeights_[i + 1] == 1) {
          if (weightSoFar + dw - weight <= 0.5) return centroidMeans_[i + 1];
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
        + " Centroids Weight: " + centroidsWeight_ + "\n"
        + " Buffered Weight: " + bufferedWeight_ + "\n"
        + " Total Weight: " + getTotalWeight() + "\n"
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
        centroidMeans_[numCentroids_ - 1] += (values[current] - centroidMeans_[numCentroids_ - 1]) * weights[current] / centroidWeights_[numCentroids_ - 1];
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
  
  private static double weightedAverage(final double x1, final double w1, final double x2, final double w2) {
    return (x1 * w1 + x2 * w2) / (w1 + w2);
  }
}
