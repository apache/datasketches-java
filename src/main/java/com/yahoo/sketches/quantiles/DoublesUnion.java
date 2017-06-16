/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * The API for Union operations for quantiles DoublesSketches
 *
 * @author Lee Rhodes
 */
public abstract class DoublesUnion {

  /**
   * Returns a new UnionBuilder
   * @return a new UnionBuilder
   */
  public static DoublesUnionBuilder builder() {
    return new DoublesUnionBuilder();
  }

  /**
   * Returns true if this union is empty
   * @return true if this union is empty
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if this union is direct
   * @return true if this union is direct
   */
  public abstract boolean isDirect();

  /**
   * Returns the configured <i>maxK</i> of this Union.
   * @return the configured <i>maxK</i> of this Union.
   */
  public abstract int getMaxK();

  /**
   * Returns the effective <i>k</i> of this Union.
   * @return the effective <i>k</i> of this Union.
   */
  public abstract int getEffectiveK();


  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given sketch into this union object.
   * The given sketch is not modified.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.
   *
   * @param sketchIn the sketch to be merged into this one.
   */
  public abstract void update(DoublesSketch sketchIn);

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given Memory image of a DoublesSketch into this union object.
   * The given Memory object is not modified and a link to it is not retained.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.
   *
   * @param mem Memory image of sketch to be merged
   */
  public abstract void update(Memory mem);

  /**
   * Update this union with the given double (or float) data Item.
   *
   * @param dataItem The given double datum.
   */
  public abstract void update(double dataItem);

  /**
   * Gets the result of this Union as an UpdateDoublesSketch, which enables further update
   * operations on the resulting sketch. The Union state has not been changed, which allows
   * further union operations.
   *
   * @return the result of this Union operation
   */
  public abstract UpdateDoublesSketch getResult();

  /**
   * Places the result of this Union into the provided memory as an UpdateDoublesSketch,
   * which enables further update operations on the resulting sketch. The Union state has not
   * been changed, which allows further union operations.
   *
   * @param dstMem the destination memory for the result
   * @return the result of this Union operation
   */
  public abstract UpdateDoublesSketch getResult(WritableMemory dstMem);

  /**
   * Gets the result of this Union  as an UpdateDoublesSketch, which enables further update
   * operations on the resulting sketch. The Union is reset to the virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public abstract UpdateDoublesSketch getResultAndReset();

  /**
   * Resets this Union to a virgin state.
   */
  public abstract void reset();

  /**
   * Returns summary information about the backing sketch.
   */
  @Override
  public abstract String toString();


  /**
   * Returns summary information about the backing sketch. Used for debugging.
   * @param sketchSummary if true includes sketch summary
   * @param dataDetail if true includes data detail
   * @return summary information about the sketch.
   */
  public abstract String toString(boolean sketchSummary, boolean dataDetail);
}
