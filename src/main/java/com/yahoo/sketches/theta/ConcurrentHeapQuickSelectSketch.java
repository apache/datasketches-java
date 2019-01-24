/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;


import com.yahoo.sketches.ResizeFactor;

/**
 * A shared sketch that is based on HeapQuickSelectSketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time
 *
 * @author eshcar
 */
class ConcurrentHeapQuickSelectSketch extends HeapQuickSelectSketch
    implements ConcurrentSharedThetaSketch {

  /**
   * A flag to coordinate between several propagation threads
   */
  private final AtomicBoolean sharedPropagationInProgress_;

  /**
   * Theta value of concurrent sketch
   */
  private volatile long volatileThetaLong_;

  /**
   * A snapshot of the estimated number of unique entries
   */
  private volatile double volatileEstimate_;

  /**
   * Num of retained entries in which the sketch toggles from sync (exact) mode to async propagation mode
   */
  private final long exactLimit_;

  /**
   * An epoch defines an interval between two resets. A propagation invoked at epoch i cannot
   * affect the sketch at epoch j>i.
   */
  private volatile long epoch_;

  /**
   * Construct a new sketch instance on the java heap.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed       <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  ConcurrentHeapQuickSelectSketch(final int lgNomLongs, final long seed) {
    super(lgNomLongs, seed, 1.0F, //p
        ResizeFactor.X1, //rf,
        false); //unionGadget
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
    exactLimit_ = getExactLimit();
    sharedPropagationInProgress_ = new AtomicBoolean(false);
    epoch_ = 0;
  }

  //Sketch overrides

  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  @Override
  public double getEstimate() {
    return getEstimationSnapshot();
  }

  //HeapQuickSelectSketch overrides

  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   */
  @Override
  public UpdateSketch rebuild() {
    super.rebuild();
    updateEstimationSnapshot();
    return this;
  }

  /**
   * Resets this sketch back to a virgin empty state.
   * Takes care of mutual exclusion with propagation thread
   */
  @Override
  public void reset() {
    advanceEpoch();
    super.reset();
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
  }

  //ConcurrentSharedThetaSketch overrides

  /**
   * Completes the propagation: end mutual exclusion block.
   * Notifies the local thread the propagation is completed
   *
   * @param localPropagationInProgress the synchronization primitive through which propagator
   *                                   notifies local thread the propagation is completed
   */
  @Override
  public void endPropagation(final AtomicBoolean localPropagationInProgress) {
    //update volatile theta, uniques estimate and propagation flag
    updateVolatileTheta();
    updateEstimationSnapshot();
    sharedPropagationInProgress_.set(false);
    if (localPropagationInProgress != null) {
      localPropagationInProgress.set(false); //clear local propagation flag
    }
  }

  /**
   * Returns a (fresh) estimation of the number of unique entries
   * @return a (fresh) estimation of the number of unique entries
   */
  @Override
  public double getEstimationSnapshot() {
    return volatileEstimate_;
  }

  /**
   * Returns the value of the volatile theta manged by the shared sketch
   * @return the value of the volatile theta manged by the shared sketch
   */
  @Override
  public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  /**
   * Returns true if a propagation is in progress, otherwise false
   * @return an indication of whether there is a pending propagation in progress
   */
  @Override
  public boolean isPropagationInProgress() {
    return sharedPropagationInProgress_.get();
  }

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * @return true if the sketch is in estimation mode.
   */
  @Override
  public boolean isSharedEstimationMode() {
    return (getRetainedEntries(false) > exactLimit_) || isEstimationMode();
  }

  /**
   * Propagates the given sketch or hash value into this sketch
   *
   * @param localPropagationInProgress the flag to be updated when done
   * @param sketchIn                   any Theta sketch with the data
   * @param singleHash                 a single hash value
   */
  @Override
  public void propagate(final AtomicBoolean localPropagationInProgress,
      final Sketch sketchIn, final long singleHash) {
    final long epoch = epoch_;
    if ((singleHash != NOT_SINGLE_HASH)                 //namely, is a single hash and
        && (getRetainedEntries(false) < exactLimit_)) { //a small sketch then propagate myself (blocking)
      startPropagation();
      if (!validateEpoch(epoch)) {
        endPropagation(null); // do not change local flag
        return;
      }
      sharedHashUpdate(singleHash);
      endPropagation(localPropagationInProgress);
      return;
    }
    // otherwise, be nonblocking, let background thread do the work
    final ConcurrentBackgroundThetaPropagation job =
        new ConcurrentBackgroundThetaPropagation(this, localPropagationInProgress, sketchIn, singleHash,
            epoch);
//    ConcurrentPropagationService.execute(job);
    ConcurrentBackgroundThetaPropagation.propagationExecutorService.execute(job);
//    propagationExecutorService_.execute(job);
  }

  @Override
  public long calcK() {
    final long k = 1 << getLgNomLongs();
    return k;
  }


  /**
   * Resets the content of the shared sketch to an empty sketch
   */
  @Override
  public void resetShared() {
    reset();
  }

  /**
   * Ensures mutual exclusion. No other thread can update the shared sketch while propagation is
   * in progress
   */
  @Override
  public void startPropagation() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) {
    } //busy wait till free
  }

  /**
   * Updates the estimation of the number of unique entries by capturing a snapshot of the sketch
   * data, namely, volatile theta and the num of valid entries in the sketch
   */
  @Override
  public void updateEstimationSnapshot() {
    volatileEstimate_ = super.getEstimate();
  }

  /**
   * Updates the shared sketch with the given hash
   * @param hash to be propagated to the shared sketch
   */
  @Override
  public void sharedHashUpdate(final long hash) {
    hashUpdate(hash);
  }

  /**
   * Updates the value of the volatile theta by extracting it from the underlying sketch managed
   * by the shared sketch
   */
  @Override
  public void updateVolatileTheta() {
    volatileThetaLong_ = getThetaLong();
  }

  /**
   * Validates the shared sketch is in the context of the given epoch
   *
   * @param epoch the epoch number to be validates
   * @return true iff the shared sketch is in the context of the given epoch
   */
  @Override
  public boolean validateEpoch(final long epoch) {
    return epoch_ == epoch;
  }

  //restricted

  /**
   * Advances the epoch while there is no background propagation
   * This ensures a propagation invoked before the reset cannot affect the sketch after the reset
   * is completed.
   */
  private void advanceEpoch() {
    startPropagation();
    //noinspection NonAtomicOperationOnVolatileField
    // this increment of a volatile field is done within the scope of the propagation
    // synchronization and hence is done by a single thread
    epoch_++;
    endPropagation(null);
  }
}
