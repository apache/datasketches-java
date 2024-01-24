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

package org.apache.datasketches.sampling;

import java.util.ArrayList;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

public class EbppsItemsSketch<T> {
  private static final int MAX_K = Integer.MAX_VALUE - 2;

  private int k_;                      // max size of sketch, in items
  private long n_;                     // total number of items processed by the sketch

  private double cumulativeWt_;        // total weight of items processed by the sketch
  private double wtMax_;               // maximum weight seen so far
  private double rho_;                 // latest scaling parameter for downsampling

  private EbppsItemsSample<T> sample_; // Object holding the current state of the sample

  private EbppsItemsSample<T> tmp_;    // temporary storage

  public EbppsItemsSketch(final int k) {
    checkK(k);
    k_ = k;
    rho_ = 1.0;
    sample_ = new EbppsItemsSample<>(k);
    tmp_ = new EbppsItemsSample<>(1);
  }

  // private copy constrcutor
  private EbppsItemsSketch(EbppsItemsSketch<T> other) {
    k_ = other.k_;
    n_ = other.n_;
    rho_ = other.rho_;
    cumulativeWt_ = other.cumulativeWt_;
    wtMax_ = other.wtMax_;
    rho_ = other.rho_;
    sample_ = new EbppsItemsSample<>(other.sample_);
    tmp_ = new EbppsItemsSample<>(1);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem,
   * which must be a Memory representation of this sketch class.
   *
   * @param <T>    The type of item this sketch contains
   * @param srcMem a Memory representation of a sketch of this class.
   *               <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param serDe  An instance of ArrayOfItemsSerDe
   * @return a sketch instance of this class
   */
  public static <T> VarOptItemsSketch<T> heapify(final Memory srcMem,
                                                 final ArrayOfItemsSerDe<T> serDe) {
    return null;
  }

  public void update(final T item) {
    update(item, 1.0);
  }

  public void update(final T item, final double weight) {
    if (weight < 0.0 || Double.isNaN(weight) || Double.isInfinite(weight))
      throw new SketchesArgumentException("Item weights must be nonnegative and finite. "
        + "Found: " + weight);
    if (weight == 0.0)
      return;

    final double newCumWt = cumulativeWt_ + weight;
    final double newWtMax = Math.max(wtMax_, weight);
    final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

    if (cumulativeWt_ > 0.0)
      sample_.downsample((newRho / rho_));

    tmp_.replaceContent(item, newRho * weight);
    sample_.merge(tmp_);

    cumulativeWt_ = newCumWt;
    wtMax_ = newWtMax;
    rho_ = newRho;
    ++n_;
  }

  /* Merging
   * There is a trivial merge algorithm that involves downsampling each sketch A and B
   * as A.cum_wt / (A.cum_wt + B.cum_wt) and B.cum_wt / (A.cum_wt + B.cum_wt),
   * respectively. That merge does preserve first-order probabilities, specifically
   * the probability proportional to size property, and like all other known merge
   * algorithms distorts second-order probabilities (co-occurrences). There are
   * pathological cases, most obvious with k=2 and A.cum_wt == B.cum_wt where that
   * approach will always take exactly 1 item from A and 1 from B, meaning the
   * co-occurrence rate for two items from either sketch is guaranteed to be 0.0.
   * 
   * With EBPPS, once an item is accepted into the sketch we no longer need to
   * track the item's weight: All accepted items are treated equally. As a result, we
   * can take inspiration from the reservoir sampling merge in the datasketches-java
   * library. We need to merge the smaller sketch into the larger one, swapping as
   * needed to ensure that, at which point we simply call update() with the items
   * in the smaller sketch as long as we adjust the weight appropriately.
   * Merging smaller into larger is essential to ensure that no item has a
   * contribution to C > 1.0.
   */

  /**
   * Merges the provided sketch into the current one.
   * This method takes an lvalue.
   * @param sketch the sketch to merge into the current object
   */
  public void merge(final EbppsItemsSketch<T> other) {
    if (other.getCumulativeWeight() == 0.0) return;
    else if (other.getCumulativeWeight() > cumulativeWt_) {
      // need to swap this with other
      // make a copy of other, merge into it, and take the result
      EbppsItemsSketch<T> copy = new EbppsItemsSketch<>(other);
      copy.internalMerge(this);
      k_ = copy.k_;
      n_ = copy.n_;
      cumulativeWt_ = copy.cumulativeWt_;
      wtMax_ = copy.wtMax_;
      rho_ = copy.rho_;
      sample_ = copy.sample_;
    } else {
      internalMerge(other);
    }
  }

  private void internalMerge(EbppsItemsSketch<T> other) {
    // assumes that other.cumulativeWeight_ <= cumulativeWt_m
    // which must be checked before calling this

    final double finalCumWt = cumulativeWt_ + other.cumulativeWt_;
    final double newWtMax = Math.max(wtMax_, other.wtMax_);
    k_ = Math.min(k_, other.k_);
    final long newN = n_ + other.n_;

    // Insert other's items with the cumulative weight
    // split between the input items. We repeat the same process
    // for full items and the partial item, scaling the input
    // weight appropriately.
    // We handle all C input items, meaning we always process
    // the partial item using a scaled down weight.
    // Handling the partial item by probabilistically including
    // it as a full item would be correct on average but would
    // introduce bias for any specific merge operation.
    final double avgWt = other.cumulativeWt_ / other.getC();
    ArrayList<T> items = other.sample_.getFullItems();
    if (items != null) {
      for (int i = 0; i < items.size(); ++i) {
        // newWtMax is pre-computed
        final double newCumWt = cumulativeWt_ + avgWt;
        final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

        if (cumulativeWt_ > 0.0)
          sample_.downsample(newRho / rho_);
      
        tmp_.replaceContent(items.get(i), newRho * avgWt);
        sample_.merge(tmp_);

        cumulativeWt_ = newCumWt;
        rho_ = newRho;
      }
    }

    // insert partial item with weight scaled by the fractional part of C
    if (other.sample_.hasPartialItem()) {
      final double otherCFrac = other.getC() % 1;
      final double newCumWt = cumulativeWt_ + (otherCFrac * avgWt);
      final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

      if (cumulativeWt_ > 0.0)
        sample_.downsample(newRho / rho_);
  
      tmp_.replaceContent(other.sample_.getPartialItem(), newRho * otherCFrac * avgWt);
      sample_.merge(tmp_);

      cumulativeWt_ = newCumWt;
      rho_ = newRho;
    }

    // avoid numeric issues by setting cumulative weight to the
    // pre-computed value
    cumulativeWt_ = finalCumWt;
    n_ = newN;
  }

  public ArrayList<T> getResult() { return sample_.getSample(); }

  public String toString() {
    return null;
  }

  public int getK() { return k_; }

  public long getN() { return n_; }

  public double getCumulativeWeight() { return cumulativeWt_; }

  public double getC() { return sample_.getC(); }

  public boolean isEmpty() { return n_ == 0; }

  public void reset() {
    n_ = 0;
    cumulativeWt_ = 0.0;
    wtMax_ = 0.0;
    rho_ = 1.0;
    sample_ = new EbppsItemsSample<>(k_);
  }

  public int getSerializedSizeBytes(final ArrayOfItemsSerDe<? super T> serDe) {
    return -1;
  }

  /**
   * Returns a byte array representation of this sketch. May fail for polymorphic item types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<? super T> serDe) {
    return null;
  }

  private void checkK(final int k) {
    if (k == 0 || k > MAX_K)
      throw new SketchesArgumentException("k must be strictly positive and less than " + MAX_K);
  }
}
