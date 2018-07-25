/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static java.lang.Math.floor;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.ResizeFactor;

/**
 * The theta filtering buffer that operates in a single writing thread.
 * @author eshcar
 * @author Lee Rhodes
 */
public final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {
  private int cacheLimit;
  private final ConcurrentDirectThetaSketch shared;
  private final AtomicBoolean localPropagationInProgress;
  private final boolean propagateOrderedCompact;


  ConcurrentHeapThetaBuffer(
      final int lgNomLongs,
      final long seed,
      final int cacheLimit,
      final ConcurrentDirectThetaSketch shared,
      final boolean propagateOrderedCompact) {
    super(lgNomLongs,
        seed,
        1.0F, //p
        ResizeFactor.X1, //rf
        false); //not a union gadget

    final int maxLimit = (int) floor(REBUILD_THRESHOLD * (1 << getLgArrLongs()));
    this.cacheLimit = max(min(cacheLimit, maxLimit), 0);
    this.shared = shared;
    localPropagationInProgress = new AtomicBoolean(false);
    this.propagateOrderedCompact = propagateOrderedCompact;
  }

  //Sketch

  @Override //specifically overridden
  public double getEstimate() {
    return shared.getEstimationSnapshot();
  }

  //restricted methods

  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > cacheLimit;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) { //Simplified
    if (cacheLimit == 0) {
      final long thLong = getThetaLong();
      if (hash < thLong) {
        propagateToSharedSketch(hash);
        return InsertedCountIncremented;
      } else {
        return RejectedOverTheta;
      }
    }
    final UpdateReturnState state = super.hashUpdate(hash);
    if (isOutOfSpace(getRetainedEntries())) {
      propagateToSharedSketch();
    }
    return state;
  }

  @Override
  public void reset() {
    if (cacheLimit > 0) {
      java.util.Arrays.fill(getCache(), 0L);
    }
    empty_ = true;
    curCount_ = 0;
    thetaLong_ = shared.getVolatileTheta();
    localPropagationInProgress.set(false);
  }

  AtomicBoolean getPropagationInProgress() {
    return localPropagationInProgress;
  }

  private void propagateToSharedSketch(final long hash) {
    while (localPropagationInProgress.compareAndSet(false, true)) {}  //busy wait until free
    shared.propagate(this,  null, hash);
  }

  private void propagateToSharedSketch() {
    while (localPropagationInProgress.compareAndSet(false, true)) {}  //busy wait until free
    final CompactSketch compactOrderedSketch = propagateOrderedCompact ? compact() : null;
    shared.propagate(this,  compactOrderedSketch, -1L);
  }

}
