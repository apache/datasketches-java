/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * For building a new DoublesSketch Union operation.
 *
 * @author Lee Rhodes
 */
public class DoublesUnionBuilder {
  private int bK = DoublesSketch.DEFAULT_K;
  private Memory bMem = null;

  /**
   * Constructor for a new DoublesUnionBuilder. The default configuration is
   * <ul>
   * <li>k: {@value DoublesSketch#DEFAULT_K}
   * This produces a normalized rank error of about 1.7%</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public DoublesUnionBuilder() {}

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch that
   * results from a union.
   * @param k determines the accuracy and size of the union.
   * It is recommended that <i>k</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>. It is only possible to union from
   * larger values of <i>k</i> to smaller values.
   * @return this builder
   */
  public DoublesUnionBuilder setK(final int k) {
    Util.checkK(k);
    bK = k;
    return this;
  }

  /**
   * Specifies the Memory to be initialized for a new off-heap version of the union.
   * @param mem the given Memory.
   * @return this builder
   */
  public DoublesUnionBuilder initMemory(final Memory mem) {
    bMem = mem;
    return this;
  }

  /**
   * Gets the current configured value of <i>k</i>
   * @return the current configured value of <i>k</i>
   */
  public int getK() {
    return bK;
  }

  /**
   * Gets the configured Memory to be initialized by the union for off-heap use.
   * @return the configured Memory.
   */
  public Memory getMemory() {
    return bMem;
  }

  /**
   * Returns a new empty Union object with the current configuration of this Builder.
   * @return a Union object
   */
  public DoublesUnion build() {
    return (bMem == null) ? DoublesUnionImpl.heapInstance(bK)
        : DoublesUnionImpl.directInstance(bK, bMem);
  }

  /**
   * Returns a Heap Union object that has been initialized with the data from the given sketch.
   * @param sketch A DoublesSketch to be used as a source of data only and will not be modified.
   * @return a DoublesUnion object
   */
  public static DoublesUnion heapify(final DoublesSketch sketch) {
    return DoublesUnionImpl.heapifyInstance(sketch);
  }

  /**
   * Returns a Heap Union object that has been initialized with the data from the given memory
   * image of a sketch.
   *
   * @param srcMem A memory image of a DoublesSketch to be used as a source of data,
   * but will not be modified.
   * @return a Union object
   */
  public static DoublesUnion heapify(final Memory srcMem) {
    return DoublesUnionImpl.heapifyInstance(srcMem);
  }

  /**
   * Returns a Union object that wraps off-heap data of the given memory image of
   * a sketch. The data structures of the Union remain off-heap.
   *
   * @param mem A memory region to be used as the data structure for the sketch
   * and will be modified.
   * @return a Union object
   */
  public static DoublesUnion wrap(final Memory mem) {
    return DoublesUnionImpl.wrapInstance(mem);
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * sketch.
   *
   * @param sketch A DoublesSketch to be used as a source of data only and will not be modified.
   * @return a DoublesUnion object
   * @deprecated changed name to heapify to more accuately reflect its intent
   */
  public static DoublesUnion build(final DoublesSketch sketch) {
    return DoublesUnionImpl.heapifyInstance(sketch);
  }

  /**
   * Returns a heap Union object that has been initialized with the data from the given Memory
   * image of a DoublesSketch. A reference to this Memory image is not retained.
   *
   * @param srcMem a Memory image of a QuantilesSketch
   * @return a Union object
   * @deprecated changed name to heapify to more accuately reflect its intent
   */
  @Deprecated
  public static DoublesUnion build(final Memory srcMem) {
    return DoublesUnionImpl.heapifyInstance(srcMem);
  }

  /**
   * Returns a Union object that has been initialized with the data from the given sketch.
   *
   * @param sketch A QuantilesSketch to be used as a source of data, but will not be modified.
   * @return a Union object
   * @deprecated this is a duplicate of heapify(DoublesSketch) and no longer needed.
   */
  @Deprecated
  public static DoublesUnion copyBuild(final DoublesSketch sketch) {
    return DoublesUnionImpl.heapifyInstance(DoublesUtil.copyToHeap(sketch));
  }

}
