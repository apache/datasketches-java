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
import static org.apache.datasketches.req.Buffer.LS;

import java.util.ArrayList;
import java.util.List;


/**
 * Java implementation for paper "Relative Error Streaming Quantiles",
 * https://arxiv.org/abs/2004.01668.
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 * <ul><li>The algorithm requires no upper bound on the stream length.
 * Instead, each relative-compactor counts the number of compaction operations performed
 * so far (variable numCompactions). Initially, the relative-compactor starts with 3 sections
 * and each time the numCompactions exceeds 2^{# of sections}, we double the number of sections
 * (variable numSections).
 * </li>
 * <li>The size of each section (variable k and sectionSize in the code and parameter k in
 * the paper) is initialized with a value set by the user via variable k.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2).
 * This is applied at each level separately. Thus, when we double the number of sections, the
 * size increases by a factor of sqrt(2) (up to +-1 after rounding).</li>
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight analysis of the sketch.</li>
 * </ul>
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class RelativeErrorQuantiles {
  //An initial upper bound on log_2 (number of compactions) + 1 COMMMENT: Huh?
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  //should be even; value of 50 roughly corresponds to 0.01-relative error guarantee with
  //constant probability
  final static int DEFAULT_K = 50;

  private int k; //default
  private boolean debug = false;
  List<RelativeCompactor> compactors = new ArrayList<>();
  //int levels; //number of compactors; was H
  int size; //retained items
  private int maxNomSize; //nominal capacity
  private long totalN; //total items offered to sketch
  private float minValue = Float.MAX_VALUE;
  private float maxValue = Float.MIN_VALUE;

  /**
   * Constructor with default k = 50;
   *
   */
  public RelativeErrorQuantiles() {
    this(DEFAULT_K, false);
  }

  /**
   * Constructor
   * @param k Controls the size and error of the sketch
   */
  public RelativeErrorQuantiles(final int k) {
    this(k, false);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one.
   * @param debug debug mode
   */
  public RelativeErrorQuantiles(final int k, final boolean debug) {
    this.k = max(k & -2, MIN_K);
    this.debug = debug;
    size = 0;
    maxNomSize = 0;
    totalN = 0;
    if (debug) { println("START:"); }
    grow();
  }

  /**
   * Copy Constructor
   * @param other the other sketch to be deep copied into this one.
   */
  RelativeErrorQuantiles(final RelativeErrorQuantiles other) {
    k = other.k;
    debug = other.debug;
    for (int i = 0; i < other.levels(); i++) {
      compactors.add(new RelativeCompactor(other.compactors.get(i)));
    }
    size = other.size;
    maxNomSize = other.maxNomSize;
    totalN = other.totalN;

  }

  void compress(final boolean lazy) {
    updateMaxNomSize();
    if (debug) {
      println("COMPRESS:       sKsize: " + size + "\t>=\t"
          + "\tmaxNomSize: " + maxNomSize
          + "\tN: " + totalN);
    }

    if (size < maxNomSize) { return; }
    for (int h = 0; h < compactors.size(); h++) {
      final RelativeCompactor c = compactors.get(h);
      final int re = c.getNumRetainedEntries();
      final int nc = c.getNomCapacity();

      if (re >= nc) {
        if ((h + 1) >= levels()) {
          if (debug) {
            println("  Must Add Compactor: len(c[" + h + "]): "
                + re + "\t>=\tc[" + h + "].nomCapacity(): " + nc);
          }
          grow(); //add a level
        }
        final float[] promoted = c.compact();
        compactors.get(h + 1).extendAndMerge(promoted);
        updateRetainedItems();
        if (lazy && (size < maxNomSize)) { break; }
      }
    }
    if (debug) {
      for (int h = 0; h < compactors.size(); h++) {
        final RelativeCompactor c = compactors.get(h);
        print(c.toHorizontalList(0));
      }
      println("COMPRESS: DONE: sKsize: " + size
          + "\tMaxNomSize: " + maxNomSize + LS);
    }
  }

  //  public double[] getCDF(final float[] splitPoints) {
  //    return getPmfOrCdf(splitPoints, true);
  //  }

  @SuppressWarnings("static-method")
  private double[] getPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    return null;
  }

  //  public double getQuantile(final double rank) {
  //
  //    return 0;
  //  }

  void grow() {
    compactors.add( new RelativeCompactor(k, levels(), debug));
    updateMaxNomSize();
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
  RelativeErrorQuantiles merge(final RelativeErrorQuantiles other) {
    //Grow until self has at least as many compactors as other
    while (levels() < other.levels()) { grow(); }
    //Append the items in same height compactors
    for (int i = 0; i < levels(); i++) {
      final boolean mergeSort = i > 0;
      compactors.get(i).merge(other.compactors.get(i), mergeSort);
    }
    updateRetainedItems();
    // After merging, we should not be lazy when compressing the sketch (as the maxNomSize bound may
    // be exceeded on many levels)
    if (size >= maxNomSize) { compress(false); }
    updateRetainedItems();
    assert size < maxNomSize;
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
      nnRank += c.rank(value) * (1 << c.getLgWeight());
    }
    return (double)nnRank / totalN;
  }

  Pair[] ranks() { //debug
    return null;
  }

  /**
   * Returns a summary of the sketch and the horizontal lists for all compactors.
   * @param decimals number of digits after the decimal point
   * @return a summary of the sketch and the horizontal lists for all compactors.
   */
  public String getSummary(final int decimals) {
    final StringBuilder sb = new StringBuilder();
    sb.append("**********Relative Error Quantiles Sketch Summary**********").append(LS);
    final int numC = compactors.size();
    sb.append("  N               : " + totalN).append(LS);
    sb.append("  Retained Entries: " + size).append(LS);
    sb.append("  Max Nominal Size: " + maxNomSize).append(LS);
    sb.append("  Min Value       : " + minValue).append(LS);
    sb.append("  Max Value       : " + maxValue).append(LS);

    sb.append("  Levels          : " + compactors.size()).append(LS);
    for (int i = 0; i < numC; i++) {
      final RelativeCompactor c = compactors.get(i);
      sb.append("  " + c.toHorizontalList(decimals));
    }
    sb.append("************************End Summary************************").append(LS);
    return sb.toString();
  }

  void update(final float item) {
    if (!Float.isFinite(item)) { return; }

    final RelativeCompactor c = compactors.get(0).append(item);
    size++;
    totalN++;
    minValue = (item < minValue) ? item : minValue;
    maxValue = (item > maxValue) ? item : maxValue;
    if (size >= maxNomSize) {
      c.sort();
      compress(true);
    }
  }

/**
 * Computes a new bound for determining when to compress the sketch.
 * @return this
 */
RelativeErrorQuantiles updateMaxNomSize() {
  int cap = 0;
  for (RelativeCompactor c : compactors) { cap += c.getNomCapacity(); }
  maxNomSize = cap;
  return this;
}

/**
 * Computes the size for the sketch.
 * @return this
 */
RelativeErrorQuantiles updateRetainedItems() {
  int count = 0;
  for (RelativeCompactor c : compactors) { count += c.getNumRetainedEntries(); }
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
