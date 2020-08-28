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
import static org.apache.datasketches.req.FloatBuffer.LS;

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.SketchesArgumentException;


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
public class ReqSketch {
  //An initial upper bound on log_2 (number of compactions) + 1 COMMMENT: Huh?
  final static int INIT_NUMBER_OF_SECTIONS = 3;
  final static int MIN_K = 4;
  //should be even; value of 50 roughly corresponds to 0.01-relative error guarantee with
  //constant probability
  final static int DEFAULT_K = 50;

  private int k;
  private boolean debug = false;
  List<ReqCompactor> compactors = new ArrayList<>();
  private ReqAuxiliary aux = null;
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
  public ReqSketch() {
    this(DEFAULT_K, false);
  }

  /**
   * Constructor
   * @param k Controls the size and error of the sketch
   */
  public ReqSketch(final int k) {
    this(k, false);
  }

  /**
   * Constructor.
   * @param k Controls the size and error of the sketch. It must be even, if not, it will be
   * rounded down by one.
   * @param debug debug mode
   */
  public ReqSketch(final int k, final boolean debug) {
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
  ReqSketch(final ReqSketch other) {
    k = other.k;
    debug = other.debug;
    for (int i = 0; i < other.getNumLevels(); i++) {
      compactors.add(new ReqCompactor(other.compactors.get(i)));
    }
    size = other.size;
    maxNomSize = other.maxNomSize;
    totalN = other.totalN;
    aux = null;
  }

  void compress(final boolean lazy) {
    updateMaxNomSize();
    if (debug) { printStartCompress(); }

    if (size < maxNomSize) { return; }
    for (int h = 0; h < compactors.size(); h++) {
      final ReqCompactor c = compactors.get(h);
      final int retEnt = c.getNumRetainedEntries();
      final int nomCap = c.getNomCapacity();

      if (retEnt >= nomCap) {
        if ((h + 1) >= getNumLevels()) {
          if (debug) { printAddCompactor(h, retEnt, nomCap); }
          grow(); //add a level
        }
        final float[] promoted = c.compact();
        compactors.get(h + 1).extendAndMerge(promoted);
        updateRetainedItems();
        if (lazy && (size < maxNomSize)) { break; }
      }
    }
    if (debug) { printAllHorizList(); }
    aux = null;
  }

    @SuppressWarnings("javadoc")
    public double[] getCDF(final float[] splitPoints) {
      return null; //getPmfOrCdf(splitPoints, true);
    }

  /**
   * Gets the total number of items offered to the sketch.
   * @return the total number of items offered to the sketch.
   */
  public long getN() {
    return totalN;
  }

  /**
   * Gets the number of levels of compactors in the sketch.
   * @return the number of levels of compactors in the sketch.
   */
  public int getNumLevels() {
    return compactors.size();
  }

  @SuppressWarnings("static-method")
  private double[] getPmfOrCdf(final float[] splitPoints, final boolean isCdf) {
    return null;
  }

  /**
   * Gets the quantile of the largest normalized rank that is less than the given normalized rank.
   * The normalized rank must be in the range [0.0, 1.0] (inclusive, inclusive).
   * A given normalized rank of 0.0 will return the minimum value from the stream.
   * A given normalized rank of 1.0 will return the maximum value from the stream.
   * @param normRank the given normalized rank
   * @return the largest quantile less than the given normalized rank.
   */
  public float getQuantile(final float normRank) {
    if ((normRank < 0) || (normRank > 1.0)) {
      throw new SketchesArgumentException("Normalized rank must be in the range [0, 1.0]: "
          + normRank);
    }
    if (normRank == 0.0) { return minValue; }
    if (normRank == 1.0) { return maxValue; }
    if (aux == null) {
      aux = new ReqAuxiliary(this);
    }
    final float q = aux.getQuantile(normRank);
    if (q == Float.NaN) { return minValue; }
    return q;
  }

  /**
   * Gets an array of quantiles that correspond to the given array of normalized ranks.
   * @param normRanks the given array of normalized ranks.
   * @return the array of quantiles that correspond to the given array of normalized ranks.
   * @see #getQuantile(float)
   */
  public float[] getQuantiles(final float[] normRanks) {
    final int len = normRanks.length;
    final float[] qArr = new float[len];
    for (int i = 0; i < len; i++) {
      qArr[i] = getQuantile(normRanks[i]);
    }
    return qArr;
  }

  /**
   * Gets the number of retained entries of this sketch
   * @return the number of retained entries of this sketch
   */
  public int getRetainedEntries() { return size; }

  void grow() {
    compactors.add( new ReqCompactor(k, getNumLevels(), debug));
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
    return getNumLevels() > 1;
  }

  /**
   * Returns an iterator for all the items in this sketch.
   * @return an iterator for all the items in this sketch.
   */
  public ReqIterator iterator() {
    return new ReqIterator(this);
  }

  /**
   * Merge other sketch into this one. The other sketch is not modified.
   * @param other sketch to be merged into this one.
   * @return this
   */
  public ReqSketch merge(final ReqSketch other) {
    //Grow until self has at least as many compactors as other
    while (getNumLevels() < other.getNumLevels()) { grow(); }
    //Append the items in same height compactors
    for (int i = 0; i < getNumLevels(); i++) {
      final boolean mergeSort = i > 0;
      compactors.get(i).merge(other.compactors.get(i), mergeSort);
    }
    updateRetainedItems();
    // After merging, we should not be lazy when compressing the sketch (as the maxNomSize bound may
    // be exceeded on many levels)
    if (size >= maxNomSize) { compress(false); }
    updateRetainedItems();
    assert size < maxNomSize;
    aux = null;
    return this;
  }

  /**
   * Computes the normalized rank of the given value in the stream.
   * The normalized rank is the fraction of values less than the given value.
   * @param value the given value
   * @return the normalized rank of the given value in the stream.
   */
  public float getRank(final float value) {
    int nnRank = 0;
    for (int i = 0; i < getNumLevels(); i++) {
      final ReqCompactor c = compactors.get(i);
      nnRank += c.rank(value) * (1 << c.getLgWeight());
    }
    return (float)nnRank / totalN;
  }

  /**
   * Returns a summary of the sketch and the horizontal lists for all compactors.
   * @param decimals number of digits after the decimal point
   * @return a summary of the sketch and the horizontal lists for all compactors.
   */
  public String toString(final int decimals) {
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
      final ReqCompactor c = compactors.get(i);
      sb.append("  " + c.toHorizontalList(decimals));
    }
    sb.append("************************End Summary************************").append(LS);
    return sb.toString();
  }

  /**
   * Updates this sketch with the given item.
   * @param item the given item
   */
  public void update(final float item) {
    if (!Float.isFinite(item)) { return; } //TODO: We may want to throw instead
    final ReqCompactor c = compactors.get(0).append(item);
    size++;
    totalN++;
    minValue = (item < minValue) ? item : minValue;
    maxValue = (item > maxValue) ? item : maxValue;
    if (size >= maxNomSize) {
      c.sort();
      compress(true);
    }
    aux = null;
  }

/**
 * Computes a new bound for determining when to compress the sketch.
 * @return this
 */
ReqSketch updateMaxNomSize() {
  int cap = 0;
  for (ReqCompactor c : compactors) { cap += c.getNomCapacity(); }
  maxNomSize = cap;
  return this;
}

/**
 * Computes the size for the sketch.
 * @return this
 */
ReqSketch updateRetainedItems() {
  int count = 0;
  for (ReqCompactor c : compactors) { count += c.getNumRetainedEntries(); }
  size = count;
  return this;
}

//debug print functions

private static void printAddCompactor(final int h, final int retEnt, final int nomCap) {
  final StringBuilder sb = new StringBuilder();
  sb.append("  ");
  sb.append("Must Add Compactor: len(c[").append(h).append("]): ");
  sb.append(retEnt).append(" >= c[").append(h).append("].nomCapacity(): ").append(nomCap);
  println(sb.toString());
}

private void printStartCompress() {
  final StringBuilder sb = new StringBuilder();
  sb.append("COMPRESS: ");
  sb.append("skSize: ").append(size).append(" >= ");
  sb.append("MaxNomSize: ").append(maxNomSize);
  sb.append("; N: ").append(totalN);
  println(sb.toString());
}

private void printAllHorizList() {
  for (int h = 0; h < compactors.size(); h++) {
    final ReqCompactor c = compactors.get(h);
    print(c.toHorizontalList(0));
  }
  println("COMPRESS: DONE: sKsize: " + size
      + "\tMaxNomSize: " + maxNomSize + LS);
}

  //temporary
  static final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
  }

  static final void print(final Object o) { System.out.print(o.toString()); }

  static final void println(final Object o) { System.out.println(o.toString()); }

}
