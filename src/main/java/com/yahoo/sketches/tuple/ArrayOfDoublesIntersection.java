/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static java.lang.Math.min;

import com.yahoo.sketches.memory.Memory;

/**
 * This is to compute an intersection of two or more sketches.
 * A new instance represents the Universal Set.
 * Every update() computes an intersection with the internal set
 * and can only reduce the internal set.
 */
public abstract class ArrayOfDoublesIntersection {

  protected final int numValues_;
  protected final long seed_;
  protected final short seedHash_;
  protected ArrayOfDoublesQuickSelectSketch sketch_;
  protected boolean isEmpty_;
  protected long theta_;
  protected boolean isFirstCall_;

  protected ArrayOfDoublesIntersection(int numValues, long seed) {
    numValues_ = numValues;
    seed_ = seed;
    seedHash_ = Util.computeSeedHash(seed);
    isEmpty_ = false;
    theta_ = Long.MAX_VALUE;
    isFirstCall_ = true;
  }

  /**
   * Updates the internal set by intersecting it with the given sketch.
   * @param sketchIn Input sketch to intersect with the internal set.
   * @param combiner Method of combining two arrays of double values
   */
  public void update(ArrayOfDoublesSketch sketchIn, ArrayOfDoublesCombiner combiner) {
    boolean isFirstCall = isFirstCall_;
    isFirstCall_ = false;
    if (sketchIn == null) {
      isEmpty_ = true;
      sketch_ = null;
      return;
    }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    theta_ = min(theta_, sketchIn.getThetaLong());
    isEmpty_ |= sketchIn.isEmpty();
    if (sketchIn.getRetainedEntries() == 0) {
      sketch_ = null;
      return;
    }
    if (isFirstCall) {
      sketch_ = createSketch(sketchIn.getRetainedEntries(), numValues_, seed_);
      ArrayOfDoublesSketchIterator it = sketchIn.iterator();
      while (it.next()) {
        sketch_.insert(it.getKey(), it.getValues());
      }
    } else {
      int matchSize = min(sketch_.getRetainedEntries(), sketchIn.getRetainedEntries());
      long[] matchKeys = new long[matchSize];
      double[][] matchValues = new double[matchSize][];
      int matchCount = 0;
      ArrayOfDoublesSketchIterator it = sketchIn.iterator();
      while (it.next()) {
        double[] values = sketch_.find(it.getKey());
        if (values != null) {
          matchKeys[matchCount] = it.getKey();
          matchValues[matchCount] = combiner.combine(values, it.getValues());
          matchCount++;
        }
      }
      sketch_ = null;
      if (matchCount > 0) {
        sketch_ = createSketch(matchCount, numValues_, seed_);
        for (int i = 0; i < matchCount; i++) sketch_.insert(matchKeys[i], matchValues[i]);
      }
      if (sketch_ != null) {
        sketch_.setThetaLong(theta_);
        sketch_.setIsEmpty(isEmpty_);
      }
    }
  }

  /**
   * Gets the internal set as an off-heap compact sketch using the given memory.
   * @param dstMem Memory for the compact sketch (can be null).
   * @return Result of the intersections so far as a compact sketch.
   */
  public ArrayOfDoublesCompactSketch getResult(Memory mem) {
    if (isFirstCall_) throw new IllegalStateException("getResult() with no intervening intersections is not a legal result.");
    if (sketch_ == null) return new HeapArrayOfDoublesCompactSketch(numValues_);
    return sketch_.compact(mem);
  }

  /**
   * Gets the internal set as an on-heap compact sketch.
   * @return Result of the intersections so far as a compact sketch.
   */
  public ArrayOfDoublesCompactSketch getResult() {
    return getResult(null);
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    isEmpty_ = false;
    theta_ = Long.MAX_VALUE;
    sketch_ = null;
    isFirstCall_ = true;
  }

  protected abstract ArrayOfDoublesQuickSelectSketch createSketch(int size, int numValues, long seed);

}
