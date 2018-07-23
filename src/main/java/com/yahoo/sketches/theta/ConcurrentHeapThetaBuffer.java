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

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.ResizeFactor;

/**
 * The theta filtering buffer that operates in a single writing thread.
 * @author eshcar
 * @author Lee Rhodes
 */
public final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {
  private final Field thetaLongField;
  private int cacheLimit;
  private final ConcurrentDirectThetaSketch shared;
  private final AtomicBoolean localPropagationInProgress;
  private final boolean propagateOrderedCompact;
  private long singleItem;

  {
    try {
      thetaLongField = getClass().getSuperclass().getDeclaredField("thetaLong_");
      thetaLongField.setAccessible(true);
    } catch (final Exception e) {
      throw new RuntimeException("Could not reflect thetaLong_ field: " + e);
    }
  }

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
    this.cacheLimit = max(min(cacheLimit, maxLimit), 0);
    this.shared = shared;
    localPropagationInProgress = new AtomicBoolean(false);
    this.propagateOrderedCompact = propagateOrderedCompact;
    singleItem = -1L;
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
    super.hashUpdate(hash);

    if (isOutOfSpace(getRetainedEntries() + 1)) {
        propagateToSharedSketch();
    }
    return InsertedCountIncremented;
  }

  @Override
  public void reset() {
    if (cacheLimit == 0) {
      singleItem = -1L;
    } else {

    }

  }

  private void propagateToSharedSketch() {

    while (localPropagationInProgress.compareAndSet(false, true)) {}  //busy wait until free
    if (cacheLimit == 0) {

    } else {

    }
    final CompactSketch compactOrderedSketch = propagateOrderedCompact ? compact() : null;

    shared.propagate(this,  compactOrderedSketch, localPropagationInProgress);

    reset();
    try {
      thetaLongField.setLong(this, curThetaLong);
    } catch (final Exception e) {
      throw new RuntimeException("Could not set thetaLong. " + e);
    }
    //TODO I would think that the lock should be released here if it is needed at all.
  }

}
