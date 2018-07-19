/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static java.lang.Math.floor;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.ResizeFactor;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {
  private int cacheLimit_;
  private final ConcurrentDirectThetaSketch shared;
  private final AtomicBoolean propagationInProgress;
  private final boolean propagateOrderedCompact;

  //used only by factory
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
    cacheLimit_ = max(min(cacheLimit, maxLimit), 0);
    this.shared = shared;
    propagationInProgress = shared.getPropogationInProgress();
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
    return numEntries > cacheLimit_;
  }

  void reset(final long thetaLong) {
    super.reset();
    thetaLong_ =  thetaLong;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) { //Simplified
    super.hashUpdate(hash);

    if (isOutOfSpace(getRetainedEntries() + 1)) {
        propagateToSharedSketch();
    }
    return InsertedCountIncremented;
  }

  private void propagateToSharedSketch() { //Added
    while (propagationInProgress.get()) {} //busy wait until free. TODO compareAndSet( ??
    propagationInProgress.set(true);
    final HeapCompactOrderedSketch compactOrderedSketch = propagateOrderedCompact
        ? (HeapCompactOrderedSketch) compact()
        : null;
    shared.propagate(this,  compactOrderedSketch);
  }

}
