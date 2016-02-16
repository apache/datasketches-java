/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.memory.Memory;

/**
 * This is a base class for unions of ArrayOfDoublesSketch.
 * It is supposed to maintain a hash table based sketch to represent the union,
 * and maintain one more theta outside of the sketch, which can get smaller
 * than theta in the sketch, so that it can be taken into account at the very end
 * in getResult(). This is done for performance since we cannot lower the theta
 * inside the sketch without an expensive rebuild.
 */
public abstract class ArrayOfDoublesUnion {
  protected final int nomEntries_;
  protected final int numValues_;
  protected final long seed_;
  protected final short seedHash_;
  protected ArrayOfDoublesQuickSelectSketch sketch_;
  protected long theta_;

  protected ArrayOfDoublesUnion(ArrayOfDoublesQuickSelectSketch sketch) {
    nomEntries_ = sketch.getNominalEntries();
    numValues_ = sketch.getNumValues();
    seed_ = sketch.getSeed();
    seedHash_ = Util.computeSeedHash(seed_);
    sketch_ = sketch;
    theta_ = sketch.getThetaLong();
  }

  /**
   * Updates the union by adding a set of entries from a given sketch
   * @param sketchIn sketch to add to the union
   */
  public void update(ArrayOfDoublesSketch sketchIn) {
    if (sketchIn == null || sketchIn.isEmpty()) return;
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    if (sketchIn.getThetaLong() < theta_) theta_ = sketchIn.getThetaLong();
    ArrayOfDoublesSketchIterator it = sketchIn.iterator();
    while (it.next()) {
      sketch_.merge(it.getKey(), it.getValues());
    }
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @param mem memory for the result (can be null)
   * @return compact sketch representing the union (off-heap if memory is provided)
   */
  public ArrayOfDoublesCompactSketch getResult(Memory mem) {
    if (theta_ < sketch_.getThetaLong()) {
      sketch_.setThetaLong(theta_);
      sketch_.rebuild();
    }
    return sketch_.compact(mem);
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @return on-heap compact sketch representing the union
   */
  public ArrayOfDoublesCompactSketch getResult() {
    return getResult(null);
  }

  /**
   * Resets the union to an empty state
   */
  public abstract void reset();

  /**
   * @return a byte array representation of this object
   */
  public byte[] toByteArray() {
    if (theta_ < sketch_.getThetaLong()) {
      sketch_.setThetaLong(theta_);
      sketch_.rebuild();
    }
    return sketch_.toByteArray();
  }

  /**
   * @return maximum required storage bytes given nomEntries and numValues
   */
  public static int getMaxBytes(int nomEntries, int numValues) {
    return ArrayOfDoublesQuickSelectSketch.getMaxBytes(nomEntries, numValues);
  }

}
