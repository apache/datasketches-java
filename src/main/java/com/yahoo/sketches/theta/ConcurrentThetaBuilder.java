/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;



/**
 * For building concurrent buffers and shared theta sketch
 *
 * @author Lee Rhodes
 */
public class ConcurrentThetaBuilder {
  private int bLgNomLongs;
  private long bSeed;
  private int bCacheLimit;
  private boolean bPropagateOrderedCompact;
  private int bPoolThreads;
  private ConcurrentDirectThetaSketch bShared;

  /**
   * Constructor for building concurrent buffers and the shared theta sketch.
   * The shared theta sketch must be built first.
   *
   */
  public ConcurrentThetaBuilder() {
    bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
    bSeed = DEFAULT_UPDATE_SEED;
    bCacheLimit = 1;
    bPropagateOrderedCompact = true;
    bPoolThreads = 3;
    bShared = null;
  }



  /**
   * Returns a ConcurrentHeapThetaBuffer with the current configuration of this Builder,
   * which must include a valid ConcurrentDirectThetaSketch.
   * @return an ConcurrentHeapThetaBuffer
   */
  public ConcurrentHeapThetaBuffer build() {
    if (bShared == null) {
      throw new SketchesStateException("The ConcurrentDirectThetaSketch must be build first.");
    }
    return new ConcurrentHeapThetaBuffer(
        bLgNomLongs, bSeed, bCacheLimit, bShared, bPropagateOrderedCompact);
  }

  /**
   * Returns a ConcurrentDirectThetaSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   * @param dstMem the given WritableMemory
   * @return a ConcurrentDirectThetaSketch with the current configuration of the Builder
   * and the given destination WritableMemory.
   */
  public ConcurrentDirectThetaSketch build(final WritableMemory dstMem) {
    ConcurrentDirectThetaSketch sketch = null;
    if (dstMem == null) {
      throw new SketchesArgumentException("Destination WritableMemory cannot be null.");
    }
    return ConcurrentDirectThetaSketch.initNewDirectInstance(
        bLgNomLongs, bSeed, dstMem, bPoolThreads);
  }

}
