/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;

/**
 * The theta filtering buffer that operates in the context of a single writing thread.
 * This is a bounded size filter. When the buffer becomes full its content is propagated into the
 * shared sketch.
 * The limit on the buffer size is configurable. Bound of size 1 allows to maintain error bound
 * that is close to the error bound of a sequential theta sketch.
 * Propagation is done either synchronously by the updating thread, or asynchronously by a
 * background propagation thread.
 *
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {

  private static int computeLogBufferSize(final int lgNomLongs, final long exactSize,
      final int maxNumLocalBuffers) {
    return Math.min(lgNomLongs, (int)Math.log(Math.sqrt(exactSize) / (2 * maxNumLocalBuffers)));
  }

  // Shared sketch consisting of the global sample set and theta value.
  private final ConcurrentSharedThetaSketch shared;

  // A flag indicating whether the shared sketch is in shared mode and requires eager propagation
  // Initially this is true. Once it is set to false (estimation mode) it never flips back.
  private boolean isExactMode;

  // A flag to indicate if we expect the propagated data to be ordered
  private final boolean propagateOrderedCompact;

  // Propagation flag is set to true while propagation is in progress (or pending).
  // It is the synchronization primitive to coordinate the work with the propagation thread.
  private final AtomicBoolean localPropagationInProgress;

  ConcurrentHeapThetaBuffer(final int lgNomLongs, final long seed,
      final ConcurrentSharedThetaSketch shared, final boolean propagateOrderedCompact,
      final int maxNumLocalThreads) {
    super(computeLogBufferSize(lgNomLongs, shared.getExactLimit(), maxNumLocalThreads),
      seed, 1.0F, //p
      ResizeFactor.X1, //rf
      false); //not a union gadget

    this.shared = shared;
    isExactMode = true;
    this.propagateOrderedCompact = propagateOrderedCompact;
    localPropagationInProgress = new AtomicBoolean(false);
  }

  //Sketch overrides

  @Override
  public int getCurrentBytes(final boolean compact) {
    return shared.getCurrentBytes(compact);
  }

  @Override
  public double getEstimate() {
    return shared.getEstimate();
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    return shared.getLowerBound(numStdDev);
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return shared.getUpperBound(numStdDev);
  }

  @Override
  public boolean hasMemory() {
    return shared.hasMemory();
  }

  @Override
  public boolean isDirect() {
    return shared.isDirect();
  }

  @Override
  public boolean isEmpty() {
    return shared.isEmpty();
  }

  @Override
  public boolean isEstimationMode() {
    return shared.isEstimationMode();
  }

  @Override
  public byte[] toByteArray() {
    throw new UnsupportedOperationException("Local theta buffer need not be serialized");
  }

  //UpdateSketch overrides

  @Override
  public void reset() {
    super.reset();
    isExactMode = true;
    localPropagationInProgress.set(false);
  }

  //restricted methods

  /**
   * Updates buffer with given hash value.
   * Triggers propagation to shared sketch if buffer is full.
   *
   * @param hash the given input hash value.  A hash of zero or Long.MAX_VALUE is ignored.
   * A negative hash value will throw an exception.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  @Override
  UpdateReturnState hashUpdate(final long hash) {
    if (isExactMode) {
      isExactMode = !shared.isEstimationMode();
    }
    HashOperations.checkHashCorruption(hash);
    if ((getHashTableThreshold() == 0) || isExactMode ) {
      final long thetaLong = getThetaLong();
      //The over-theta and zero test
      if (HashOperations.continueCondition(thetaLong, hash)) {
        return RejectedOverTheta; //signal that hash was rejected due to theta or zero.
      }
      if (propagateToSharedSketch(hash)) {
        return InsertedCountIncremented; //not totally correct
      }
    }
    final UpdateReturnState state = super.hashUpdate(hash);
    if (isOutOfSpace(getRetainedEntries() + 1)) {
      propagateToSharedSketch();
    }
    return state;
  }

  /**
   * Propagates a single hash value to the shared sketch
   *
   * @param hash to be propagated
   */
  private boolean propagateToSharedSketch(final long hash) {
    //noinspection StatementWithEmptyBody
    while (localPropagationInProgress.get()) {
    } //busy wait until previous propagation completed
    localPropagationInProgress.set(true);
    final boolean res = shared.propagate(localPropagationInProgress, null, hash);
    //in this case the parent empty_ and curCount_ were not touched
    thetaLong_ = shared.getVolatileTheta();
    return res;
  }

  /**
   * Propagates the content of the buffer as a sketch to the shared sketch
   */
  private void propagateToSharedSketch() {
    //noinspection StatementWithEmptyBody
    while (localPropagationInProgress.get()) {
    } //busy wait until previous propagation completed

    final CompactSketch compactSketch = compact(propagateOrderedCompact, null);
    localPropagationInProgress.set(true);
    shared.propagate(localPropagationInProgress, compactSketch,
        ConcurrentSharedThetaSketch.NOT_SINGLE_HASH);
    super.reset();
    thetaLong_ = shared.getVolatileTheta();
  }
}
