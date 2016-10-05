/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * The API for Union operations for QuantilesSketches
 *
 * @author Lee Rhodes
 */
public abstract class DoublesUnion {

  /**
   * Returns a new UnionBuilder
   * @return a new UnionBuilder
   */
  public static final DoublesUnionBuilder builder() {
    return new DoublesUnionBuilder();
  }

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
   * Merges the given Memory image of a QuantilesSketch into this union object.
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
   * Gets the result of this Union operation as a copy of the internal state.
   * This enables further union update operations on this state.
   * @return the result of this Union operation
   */
  public abstract DoublesSketch getResult();

  /**
   * Gets the result of this Union operation (without a copy) and resets this Union to the
   * virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public abstract DoublesSketch getResultAndReset();

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
