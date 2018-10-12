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

import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;

/**
 * The theta filtering buffer that operates in a single writing thread.
 * @author eshcar
 * @author Lee Rhodes
 */
public final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {
  private int cacheLimit;
  private final SharedThetaSketch shared;
  private final AtomicBoolean localPropagationInProgress;
  private final boolean propagateOrderedCompact;


  ConcurrentHeapThetaBuffer(
      final int lgNomLongs,
      final long seed,
      final int cacheLimit,
      final SharedThetaSketch shared,
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
      final long thetaLong = getThetaLong();
      //The over-theta and zero test
      if (HashOperations.continueCondition(thetaLong, hash)) {
        return RejectedOverTheta; //signal that hash was rejected due to theta or zero.
      }
      propagateToSharedSketch(hash);
      return InsertedCountIncremented; //not totally correct
    }
    final UpdateReturnState state = super.hashUpdate(hash);
    if (isOutOfSpace(getRetainedEntries() + 1)) {
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
  }

  AtomicBoolean getPropagationInProgress() {
    return localPropagationInProgress;
  }

  private void propagateToSharedSketch(final long hash) {
    while (localPropagationInProgress.get()) {} //busy wait until previous propagation completed
    localPropagationInProgress.set(true);
    shared.propagate(localPropagationInProgress,  null, hash);
    //in this case the parent empty_ and curCount_ were not touched
    thetaLong_ = shared.getVolatileTheta();
  }

  private void propagateToSharedSketch() {
    while (localPropagationInProgress.get()) {} //busy wait until previous propagation completed

    final CompactSketch compactSketch = compact(propagateOrderedCompact, null);
    localPropagationInProgress.set(true);
    shared.propagate(localPropagationInProgress, compactSketch, -1L);
    reset();
  }
}
