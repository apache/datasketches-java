/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import static com.yahoo.sketches.Util.MAX_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.simpleIntLog2;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSketch;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;

/**
 * A Frequent Unique Nodes sketch.
 * @author Lee Rhodes
 */
public class FunSketch {
  private final int lgK;
  private final ArrayOfStringsSketch sketch;

  /**
   * Create new instance of Frequent Unique Nodes Sketch with the given
   * Log-base2 of required nominal entries.
   * @param lgK Log-base2 of required nominal entries.
   */
  public FunSketch(final int lgK) {
    this.lgK = lgK;
    sketch = new ArrayOfStringsSketch(lgK);
  }

  FunSketch(final ArrayOfStringsSketch aosSketch) {
    sketch = aosSketch;
    lgK = simpleIntLog2(aosSketch.getNominalEntries());
  }

  /**
   * Create a new instance of  Frequent Unique Nodes Sketch with the given
   * threshold and rse.
   * @param threshold the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) item.
   * @param rse the desired Relative Standard Error for an estimate of a returned frequent sketch
   * at the threshold.
   */
  public FunSketch(final double threshold, final double rse) {
    lgK = computeLgK(threshold, rse);
    sketch = new ArrayOfStringsSketch(lgK);
  }

  public double getEstimate() {
    return sketch.getEstimate();
  }

  public double getLowerBound(final int numStdDev) {
    return sketch.getLowerBound(numStdDev);
  }

  public double getUpperBound(final int numStdDev) {
    return sketch.getUpperBound(numStdDev);
  }

  public SketchIterator<ArrayOfStringsSummary> iterator() {
    return sketch.iterator();
  }

  /**
   * Returns the sketch iterator.
   * @return the iterator over the sketch contents
   */
  public SketchIterator<ArrayOfStringsSummary> getIterator() {
    return sketch.iterator();
  }

  /**
   * Update the sketch with the given nodes array.
   * @param nodesArr the given array of node names.
   */
  public void update(final String[] nodesArr) {
    sketch.update(nodesArr, nodesArr);
  }

  public byte[] toByteArray() {
    return sketch.toByteArray();
  }

  /**
   * Computes LgK given the threshold and RSE.
   * @param threshold the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) item.
   * @param rse the desired Relative Standard Error for an estimate of a returned frequent sketch
   * at the threshold.
   * @return LgK
   */
  static int computeLgK(final double threshold, final double rse) {
    final int k = ceilingPowerOf2((int) Math.ceil(1.0 / (threshold * rse * rse)));
    if (k > (1 << MAX_LG_NOM_LONGS)) {
      throw new SketchesArgumentException("Requested Sketch is too large, "
          + "either increase the threshold or the rse or both.");
    }
    return simpleIntLog2(k);
  }

}
