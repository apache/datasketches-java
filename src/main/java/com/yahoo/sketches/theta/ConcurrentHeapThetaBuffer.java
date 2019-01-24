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

//  private ExecutorService propagationExecutorService_ = Executors.newSingleThreadExecutor();

  /**
   * The bound on the size of the buffer
   */
  private final int cacheLimit;

  /**
   * Shared sketch consisting of the global sample set and theta value.
   */
  private final ConcurrentSharedThetaSketch shared;

  /**
   * Propagation flag is set to true while propagation is in progress (or pending).
   * It is the synchronization primitive to coordinate the work with the propagation thread.
   */
  private final AtomicBoolean localPropagationInProgress;

  /**
   * A flag to indicate if we expect the propagated data to be ordered
   */
  private final boolean propagateOrderedCompact;

  ConcurrentHeapThetaBuffer(final int lgNomLongs, final long seed, final int cacheLimit,
      final ConcurrentSharedThetaSketch shared, final boolean propagateOrderedCompact) {
    super(lgNomLongs, seed, 1.0F, //p
        ResizeFactor.X1, //rf
        false); //not a union gadget

    final int maxLimit = (int) floor(REBUILD_THRESHOLD * (1 << getLgArrLongs()));
    this.cacheLimit = max(min(cacheLimit, maxLimit), 0);
    this.shared = shared;
    localPropagationInProgress = new AtomicBoolean(false);
    this.propagateOrderedCompact = propagateOrderedCompact;
  }

  //Sketch overrides

  @Override
  public int getCurrentBytes(final boolean compact) {
    return ((UpdateSketch)shared).getCurrentBytes(compact);
  }

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  @Override //specifically overridden
  public double getEstimate() {
    return shared.getEstimationSnapshot();
  }

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  @Override
  public double getLowerBound(final int numStdDev) {
    return ((UpdateSketch)shared).getLowerBound(numStdDev);
  }

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  @Override
  public double getUpperBound(final int numStdDev) {
    return ((UpdateSketch)shared).getUpperBound(numStdDev);
  }

  @Override
  public boolean isDirect() {
    return ((UpdateSketch)shared).isDirect();
  }

  @Override
  public boolean isEmpty() {
    return ((UpdateSketch)shared).isEmpty();
  }

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * @return true if the sketch is in estimation mode.
   */
  @Override
  public boolean isEstimationMode() {
    return shared.isSharedEstimationMode();
  }

  /**
   * Resets this sketch back to a virgin empty state.
   */
  @Override
  public void reset() {
    if (cacheLimit > 0) {
      java.util.Arrays.fill(getCache(), 0L);
    }
    localPropagationInProgress.set(false);
    curCount_ = 0;
    thetaLong_ = shared.getVolatileTheta();
  }

  @Override
  public byte[] toByteArray() {
    return ((UpdateSketch)shared).toByteArray();
  }

  //restricted methods

  /**
   * Updates buffer with given hash value.
   * Triggers propagation to shared sketch if buffer is full.
   *
   * @param hash the given input hash value.  A hash of zero or Long.MAX_VALUE is ignored.
   * A negative hash value will throw an exception.
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  @Override
  UpdateReturnState hashUpdate(final long hash) { //Simplified
    HashOperations.checkHashCorruption(hash);
    if (cacheLimit == 0 || !shared.isSharedEstimationMode()) {
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

  /**
   * Returns true if numEntries (curCount) is greater than the buffer bound.
   * @param numEntries the given number of entries (or current count).
   * @return true if numEntries (curCount) is greater than the buffer bound.
   */
  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > cacheLimit;
  }

  /**
   * Propagates a single hash value to the shared sketch
   *
   * @param hash to be propagated
   */
  private void propagateToSharedSketch(final long hash) {
    //noinspection StatementWithEmptyBody
    while (localPropagationInProgress.get()) {
    } //busy wait until previous propagation completed
    localPropagationInProgress.set(true);
    shared.propagate(localPropagationInProgress, null, hash);
    //in this case the parent empty_ and curCount_ were not touched
    thetaLong_ = shared.getVolatileTheta();
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
    shared.propagate(localPropagationInProgress, compactSketch, ConcurrentSharedThetaSketch.NOT_SINGLE_HASH);
    reset();
  }
}
