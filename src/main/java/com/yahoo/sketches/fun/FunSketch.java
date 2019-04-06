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
import com.yahoo.sketches.tuple.UpdatableSketch;
import com.yahoo.sketches.tuple.UpdatableSketchBuilder;

/**
 * A Frequent Unique Nodes sketch.
 * @author Lee Rhodes
 */
public class FunSketch {
  private final int lgK;
  private final UpdatableSketchBuilder<String[], NodesSummary> bldr =
      new UpdatableSketchBuilder<>(new NodesSummaryFactory());
  private final UpdatableSketch<String[], NodesSummary> sketch;

  /**
   * Create new instance of Frequent Unique Nodes Sketch with the given
   * Log-base2 of required nominal entries.
   * @param lgK Log-base2 of required nominal entries.
   */
  public FunSketch(final int lgK) {
    this.lgK = lgK;
    bldr.reset();
    bldr.setNominalEntries(1 << this.lgK);
    sketch = bldr.build();
  }

  /**
   * Create a new instance of  Frequent Unique Nodes Sketch with the given
   * threshold and rse.
   * @param threshold the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) item.
   * @param rse the desired Relative Standard Error for the sketch.
   */
  public FunSketch(final double threshold, final double rse) {
    lgK = computeLgK(threshold, rse);
    bldr.reset();
    bldr.setNominalEntries(1 << lgK);
    sketch = bldr.build();
  }

  /**
   * Update the sketch with the given nodes array.
   * @param nodesArr the given array of node names.
   */
  public void update(final String[] nodesArr) {
    final int[] key = computeKey(nodesArr);
    sketch.update(key, nodesArr);
  }

  /**
   * Returns the sketch iterator.
   * @return the iterator over the sketch contents
   */
  public SketchIterator<NodesSummary> getIterator() {
    return sketch.iterator();
  }

  /**
   * Computes LgK given the threshold and RSE.
   * @param threshold the fraction, between zero and 1.0, of the total stream length that defines
   * a "Frequent" (or heavy) item.
   * @param rse the desired Relative Standard Error for a computed estimate at the threshold.
   * @return LgK
   */
  public static int computeLgK(final double threshold, final double rse) {
    final int k = ceilingPowerOf2((int) Math.ceil(1.0 / (threshold * rse * rse)));
    if (k > (1 << MAX_LG_NOM_LONGS)) {
      throw new SketchesArgumentException("Requested Sketch is too large, "
          + "either increase the threshold or the rse or both.");
    }
    return simpleIntLog2(k);
  }

  /**
   * @param nodesArr array of node Strings
   * @return int array of node hashCodes.
   */
  public static int[] computeKey(final String[] nodesArr) {
    final int len = nodesArr.length;
    final int[] arr = new int[len];
    for (int i = 0; i < len; i++) { arr[i] = nodesArr.hashCode(); }
    return arr;
  }

}
