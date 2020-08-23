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

package org.apache.datasketches.req;

import static java.lang.Math.max;

import java.util.ArrayList;
import java.util.List;


/**
 * Proof-of-concept code for paper "Relative Error Streaming Quantiles",
 * https://arxiv.org/abs/2004.01668.
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 * <ul><li>The algorithm requires no upper bound on the stream length.
 * Instead, each relative-compactor (i.e. buffer) counts the number of compaction operations performed
 * so far (variable numCompactions). Initially, the relative-compactor starts with 2 buffer sections
 * and each time the numCompactions exceeds 2^{# of sections}, we double the number of sections
 * (variable numSections).
 * </li>
 * <li>The size of each buffer section (variable k and sectionSize in the code and parameter k in
 * the paper) is initialized with a value set by the user via variable k.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2)
 * (for which we use a float variable sectionSizeF). As above, this is applied
 * at each level separately. Thus, when we double the number of sections, the buffer size
 * increases by a factor of sqrt(2) (up to +-1 after rounding).</li>
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight analysis of the sketch.</li>
 * </ul>
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class RelativeErrorSketch {
  //An initial upper bound on log_2 (number of compactions) + 1 COMMMENT: Huh?
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  //should be even; value of 50 roughly corresponds to 0.01-relative error guarantee wiTH
  //constant probability (TODO determine confidence bounds)
  final static int DEFAULT_K = 50;

  private int k; //default
  private boolean debug = false;
  List<RelativeCompactor> compactors = new ArrayList<>();
  //int levels; //number of compactors; was H
  int size; //retained items
  private int maxSize; //capacity
  private long totalN; //total items offered to sketch

  /**
   * Constructor with default k = 50;
   *
   */
  RelativeErrorSketch() {
    this(DEFAULT_K, false);
  }

  /**
   * Constructor
   * @param k Controls the size and error of the sketch
   */
  RelativeErrorSketch(final int k) {
    this(k, false);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one.
   * @param debug debug mode
   */
  RelativeErrorSketch(final int k, final boolean debug) {
    this.k = max(k & -2, MIN_K);
    this.debug = debug;
    size = 0;
    maxSize = 0;
    totalN = 0;
    grow();
  }

  /**
   * Copy Constructor
   * @param other the other sketch to be deep copied into this one.
   */
  RelativeErrorSketch(final RelativeErrorSketch other) {
    k = other.k;
    debug = other.debug;
    for (int i = 0; i < other.levels(); i++) {
      compactors.add(new RelativeCompactor(other.compactors.get(i)));
    }
    size = other.size;
    maxSize = other.maxSize;
    totalN = other.totalN;

  }

  void compress(final boolean lazy) {
    if (debug) { println("Compression Start ..."); }
    updateMaxSize();
    if (size < maxSize) { return; }
    for (int h = 0; h < compactors.size(); h++) { //# compactors
      final RelativeCompactor c = compactors.get(h);
      if (c.length() >= c.capacity()) {
        if ((h + 1) >= levels()) { grow(); } //add a level
        final float[] arr = c.compact();
        compactors.get(h + 1).extend(arr);
        size += arr.length;
        if (lazy && (size < maxSize)) { break; }
      }
    }
    if (debug) {
      println("Compresssion Done:\n  RetainedItems:\t" + size + "\n  Capacity     :\t" + maxSize);
    }
  }

  double[] getCDF(final float[] splitPoints) {
    return getPmfOrCdf(splitPoints, true);
  }

  @SuppressWarnings("static-method")
  private double[] getPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    return null;
  }

  public double getQuantile(final double rank) {

    return 0;
  }

  void grow() {
    compactors.add( new RelativeCompactor(k, levels(), debug));
    updateMaxSize();
  }

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() {
    return totalN == 0;
  }

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  public boolean isEstimationMode() {
    return levels() > 1;
  }

  /**
   * Returns an iterator for all the items in this sketch.
   * @return an iterator for all the items in this sketch.
   */
  public RelativeErrorSketchIterator iterator() {
    return new RelativeErrorSketchIterator(this);
  }

  int levels() {
    return compactors.size();
  }

  /**
   * Merge other sketch into this one. The other sketch is not modified.
   * @param other sketch to be merged into this one.
   */
  RelativeErrorSketch mergeIntoSelf(final RelativeErrorSketch other) {
    //Grow until self has at least as many compactors as other
    while (levels() < other.levels()) { grow(); }
    //Append the items in same height compactors
    for (int i = 0; i < levels(); i++) {
      compactors.get(i).mergeIntoSelf(other.compactors.get(i));
    }
    updateRetainedItems();
    // After merging, we should not be lazy when compressing the sketch (as the maxSize bound may
    // be exceeded on many levels)
    if (size >= maxSize) { compress(false); }
    assert size < maxSize;
    return this;
  }

  class Pair {
    float rank;
    float value;
  }

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value.
   * @param value the given value
   * @return the normalized rank of the given value in the stream.
   */
  double rank(final float value) {
    int nnRank = 0;
    for (int i = 0; i < levels(); i++) {
      final RelativeCompactor c = compactors.get(i);
      nnRank += c.rank(value) * (1 << c.lgWeight);
    }
    return (double)nnRank / totalN;
  }

  Pair[] ranks() { //debug
    return null;
  }

  void update(final float item) {
    final RelativeCompactor c = compactors.get(0).append(item);
    size++;
    if (size >= maxSize) { compress(true); }
    totalN++;
  }

/**
 * Computes a new bound for determining when to compress the sketch.
 * @return this
 */
RelativeErrorSketch updateMaxSize() {
  int cap = 0;
  for (RelativeCompactor c : compactors) { cap += c.capacity(); } //get or set?
  maxSize = cap;
  return this;
}

/**
 * Computes the size for the sketch.
 * @return this
 */
RelativeErrorSketch updateRetainedItems() {
  int count = 0;
  for (RelativeCompactor c : compactors) { count += c.length(); }
  size = count;
  return this;
}

  //temporary
  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }

}
