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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class provides access to the samples contained in a VarOptItemsSketch. It provides two
 * mechanisms for access:
 * <ul>
 *   <li>An <code>Iterator</code> over <code>WeightedSample</code> objects which can can be used to
 *   access both the items and weights in the sample, and which avoids copying data from the
 *   sketch.</li>
 *   <li>Getter methods to obtain items or weights as arrays, or individual items. These
 *   methods create a (shallow) copy of data from the sketch on the first call to any get
 *   method.</li>
 * </ul>
 *
 * <p>If using getters with a sketch storing heterogeneous items from a polymorphic base class, you
 * must call <code>setClass()</code> prior to calling one of the getter methods. This is not
 * necessary if using the iterator.</p>
 *
 * <p>The class also implements <code>Iterable</code> to allow the use of forEach loops for
 * convenience.</p>
 *
 * @param <T> an item of type T
 *
 * @author Jon Malkin
 */
public class VarOptItemsSamples<T> implements Iterable<VarOptItemsSamples<T>.WeightedSample> {

  final VarOptItemsSketch<T> sketch_;
  VarOptItemsSketch<T>.Result sampleLists;
  final long n_;
  final int h_;
  final double rWeight_;

  /**
   * A convenience class to allow easy iterator access to a VarOpt sample.
   */
  //@SuppressWarnings("synthetic-access")
  public final class WeightedSample {
    private final int idx_;
    private double adjustedWeight_;

    WeightedSample(final int i) {
      idx_ = i;
      adjustedWeight_ = Double.NaN;
    }

    WeightedSample(final int i, final double adjustedWeight) {
      idx_ = i;
      adjustedWeight_ = adjustedWeight;
    }

    /**
     * Accesses the iterator's current object
     * @return An item from the sketch's data sample
     */
    public T getItem() {
      return sketch_.getItem(idx_);
    }

    /**
     * Accesses the iterator's current weight value
     * @return A weight from the sketch's data sample
     */
    public double getWeight() {
      if (idx_ > h_) {
        return Double.isNaN(adjustedWeight_) ? rWeight_ : adjustedWeight_;
      } else {
        return sketch_.getWeight(idx_);
      }
    }

    // only used in resolving union gadget
    boolean getMark() { return sketch_.getMark(idx_); }
  }

  /**
   * The standard iterator
   */
  //@SuppressWarnings("synthetic-access")
  public class VarOptItemsIterator implements Iterator<WeightedSample> {
    int currIdx_;
    int finalIdx_; // inclusive final index

    VarOptItemsIterator() {
      currIdx_ = h_ == 0 ? 1 : 0;
      final int k = sketch_.getK();
      finalIdx_ = (int) (n_ <= k ? n_ - 1 : k); // -1 since finalIdx_ is inclusive
    }

    // package private iterator to crawl only H or only R region values
    VarOptItemsIterator(final boolean useRRegion) {
      if (useRRegion) {
        currIdx_ = h_ + 1;                    // to handle the gap
        finalIdx_ = sketch_.getNumSamples();  // no +1 since inclusive
      } else {
        currIdx_ = 0;
        finalIdx_ = h_ - 1;                   // need stop before h_ since incluside
      }
    }

    @Override
    public boolean hasNext() {
      // If sketch is in exact mode, we'll have a next item as long as index < k.
      // If in sampling mode, the last index is k (array length k+1) but there will always be at
      // least one item in R, so no need to check if the last element is null.
      return currIdx_ <= finalIdx_;
    }

    @Override
    public WeightedSample next() {
      if (n_ != sketch_.getN()) {
        throw new ConcurrentModificationException();
      } else if (currIdx_ > finalIdx_) {
        throw new NoSuchElementException();
      }

      // grab current index, apply logic to update currIdx_ for the next call
      final int tgt = currIdx_;

      ++currIdx_;
      if ((currIdx_ == h_) && (h_ != n_)) {
        ++currIdx_;
      }

      return new WeightedSample(tgt);
    }
  }

  //@SuppressWarnings("synthetic-access")
  class WeightCorrectingRRegionIterator extends VarOptItemsIterator {
    private double cumWeight = 0.0;

    WeightCorrectingRRegionIterator() {
      super(true);
    }

    @Override
    public WeightedSample next() {
      if (n_ != sketch_.getN()) {
        throw new ConcurrentModificationException();
      } else if (currIdx_ > finalIdx_) {
        throw new NoSuchElementException();
      }

      // grab current index, apply logic to update currIdx_ for the next call
      final int tgt = currIdx_;

      ++currIdx_;
      // only covers R region, no need to check for gap

      final WeightedSample sample;
      if (tgt == finalIdx_) {
        sample = new WeightedSample(tgt, sketch_.getTotalWtR() - cumWeight);
      } else {
        sample = new WeightedSample(tgt);
        cumWeight += rWeight_;
      }

      return sample;
    }
  }

  VarOptItemsSamples(final VarOptItemsSketch<T> sketch) {
    Objects.requireNonNull(sketch, "sketch must not be null");
    sketch_ = sketch;
    n_ = sketch.getN();
    h_ = sketch.getHRegionCount();
    rWeight_ = sketch.getTau();
  }

  @Override
  public Iterator<WeightedSample> iterator() {
    return new VarOptItemsIterator();
  }

  Iterator<WeightedSample> getHIterator() { return new VarOptItemsIterator(false); }

  Iterator<WeightedSample> getRIterator() { return new VarOptItemsIterator(true); }

  Iterator<WeightedSample> getWeightCorrRIter() { return new WeightCorrectingRRegionIterator(); }

  /**
   * Specifies the class to use when copying the item array from the sketch. This method is
   * required if the sketch stores heterogeneous item types of some base class, for instance a
   * sketch over <code>Number</code>s.
   *
   * @param clazz The class to use when creating the item array result
   */
  public void setClass(final Class<?> clazz) {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamplesAsArrays(clazz);
    }
  }

  /**
   * Returns the length Copies items and weights from the sketch, if necessary, and returns the
   * length of
   * any
   * resulting array. The result will be 0 for an empty sketch.
   *
   * @return The number of items and weights in the sketch
   */
  public int getNumSamples() {
    loadArrays();
    return (sampleLists == null || sampleLists.weights == null ? 0 : sampleLists.weights.length);
  }

  /**
   * Returns a shallow copy of the array of sample items contained in the sketch. If this is the
   * first getter call, copies data arrays from the sketch.
   * @return The number of samples contained in the sketch.
   */
  public T[] items() {
    loadArrays();
    return (sampleLists == null ? null : sampleLists.items);
  }

  /**
   * Returns a single item from the samples contained in the sketch. Does not perform bounds
   * checking on the input. If this is the first getter call, copies data arrays from the sketch.
   * @param i An index into the list of samples
   * @return The sample at array position <code>i</code>
   */
  public T items(final int i) {
    loadArrays();
    return (sampleLists == null || sampleLists.items == null ? null : sampleLists.items[i]);
  }

  /**
   * Returns a copy of the array of weights contained in the sketch. If this is the first
   * getter call, copies data arrays from the sketch.
   * @return The number of samples contained in the sketch.
   */
  public double[] weights() {
    loadArrays();
    return (sampleLists == null ? null : sampleLists.weights);
  }

  /**
   * Returns a single weight from the samples contained in the sketch. Does not perform bounds
   * checking on the input. If this is the first getter call, copies data arrays from the sketch.
   * @param i An index into the list of weights
   * @return The weight at array position <code>i</code>
   */
  public double weights(final int i) {
    loadArrays();
    return (sampleLists == null || sampleLists.weights == null ? Double.NaN : sampleLists.weights[i]);
  }

  private void loadArrays() {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamplesAsArrays();
    }
  }
}
