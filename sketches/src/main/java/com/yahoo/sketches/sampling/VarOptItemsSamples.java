/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * This class provides access to the samples contained in a VarOptItemsSketch. It provides two
 * mechanisms for access:
 * <ul>
 *   <li>An <tt>Iterator</tt> over <tt>WeightedSample</tt> objects which can can be used to
 *   access both the items and weights in the sample, and which avoids copying data from the
 *   sketch.</li>
 *   <li>Getter methods to obtain items or weights as arrays, or individual items. These
 *   methods create a (shallow) copy of data from the sketch on the first call to any get
 *   method.</li>
 * </ul>
 *
 * <p>If using getters with a sketch storing heterogeneous items from a polymorphic base class, you
 * must call <tt>setClass()</tt> prior to calling one of the getter methods. This is not
 * necessary if using the iterator.</p>
 *
 * <p>The class also implements <tt>Iterable</tt> to allow the use of forEach loops for
 * convenience.</p>
 *
 * @author Jon Malkin
 */
public class VarOptItemsSamples<T> implements Iterable<VarOptItemsSamples<T>.WeightedSample> {

  private final VarOptItemsSketch<T> sketch_;
  private VarOptItemsSketch.Result sampleLists;
  private final long n_;
  private final double rWeight_;
  private final int h_;

  /**
   * A convenience class to allow easy iterator access to a VarOpt sample.
   */
  public final class WeightedSample {
    private final int idx_;

    private WeightedSample(final int i) {
      idx_ = i;
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
      return idx_ > h_ ? rWeight_ : sketch_.getWeight(idx_);
    }
  }

  public class VarOptItemsIterator implements Iterator<WeightedSample> {
    private int currIdx_ = 0;

    @Override
    public boolean hasNext() {
      // If sketch is in exact mode, we'll have a next item as long as index < k.
      // If in sampling mode, the last index is k (array length k+1) but there will always be at
      // least one item in R, so no need to check if the last element is null.
      final int k = sketch_.getK();
      return (n_ <= k && currIdx_ < n_) || (n_ > k && currIdx_ <= k);
    }

    @Override
    public WeightedSample next() {
      if (n_ != sketch_.getN()) {
        throw new ConcurrentModificationException();
      }

      // grab current index, apply logic to update currIdx_ for the next call
      final int tgt = currIdx_;

      ++currIdx_;
      if (currIdx_ == h_ && h_ != n_) {
        ++currIdx_;
      }

      return new WeightedSample(tgt);
    }
  }

  VarOptItemsSamples(final VarOptItemsSketch<T> sketch) {
    sketch_ = sketch;
    n_ = sketch.getN();
    h_ = sketch.getHRegionCount();
    rWeight_ = sketch.getRRegionWeight();
  }

  @Override
  public Iterator<WeightedSample> iterator() {
    return new VarOptItemsIterator();
  }

  /**
   * Specifies the class to use when copying the item array from the sketch. This method is
   * required if the sketch stores heterogeneous item types of some base class, for instance a
   * sketch over <tt>Number</tt>s.
   *
   * @param clazz The class to use when creating the item array result
   */
  public void setClass(final Class clazz) {
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
    return (sampleLists == null ? 0 : sampleLists.weights.length);
  }

  /**
   * Returns a shallow copy of the array of sample items contained in the sketch. If this is the
   * first getter call, copies data arrays from the sketch.
   * @return The number of samples contained in the sketch.
   */
  @SuppressWarnings("unchecked")
  public T[] items() {
    loadArrays();
    return (sampleLists == null ? null : (T[]) sampleLists.items);
  }

  /**
   * Returns a single item from the samples contained in the sketch. Does not perform bounds
   * checking on the input. If this is the first getter call, copies data arrays from the sketch.
   * @param i An index into the list of samples
   * @return The sample at array posiiton <tt>i</tt>
   */
  @SuppressWarnings("unchecked")
  public T items(final int i) {
    loadArrays();
    return (sampleLists == null ? null : (T) sampleLists.items[i]);
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
   * @return The weight at array posiiton <tt>i</tt>
   */
  public double weights(final int i) {
    loadArrays();
    return (sampleLists == null ? Double.NaN : sampleLists.weights[i]);
  }

  private void loadArrays() {
    if (sampleLists == null) {
      sampleLists = sketch_.getSamplesAsArrays();
    }
  }
}
