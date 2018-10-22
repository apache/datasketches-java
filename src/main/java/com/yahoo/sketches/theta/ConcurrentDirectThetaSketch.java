/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;

/**
 * This is a concurrent theta sketch that is based on a sequential direct (memory-based)
 * quick-select sketch.
 * Background propagation threads are used to propagate data from thread local buffers into this
 * sketch which stores the most up-to-date estimation of number of unique items.
 *
 * @author eshcar
 * @author Lee Rhodes
 */
public class ConcurrentDirectThetaSketch extends DirectQuickSelectSketch
    implements SharedThetaSketch {
  private volatile long volatileThetaLong_;
  private volatile double volatileEstimate_;
  // A flag to coordinate between several propagation threads
  private final AtomicBoolean sharedPropagationInProgress_;
  // An epoch defines an interval between two resets. A propagation invoked at epoch i cannot
  // affect the sketch at epoch j>i.
  private volatile long epoch_;


  /**
   * Get a new sketch instance and initialize the given Memory as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param dstMem the given Memory object destination. It cannot be null.
   * The hash table area will be cleared prior to use.
   */
  ConcurrentDirectThetaSketch(
      final int lgNomLongs,
      final long seed,
      final WritableMemory dstMem) {
    super(lgNomLongs,
        seed,
        1.0F, //p
        ResizeFactor.X1, //rf,
        null,
        dstMem,
        false);

    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
    sharedPropagationInProgress_ = new AtomicBoolean(false);
    epoch_ = 0;
  }

  //Concurrent methods

  @Override public double getEstimationSnapshot() {
    return volatileEstimate_;
  }

  @Override public void updateEstimationSnapshot() {
    volatileEstimate_ = getEstimate();
  }

  @Override public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  @Override public void updateVolatileTheta() {
    volatileThetaLong_ = getThetaLong();
  }

  @Override public boolean isPropagationInProgress() {
    return sharedPropagationInProgress_.get();
  }

  /**
   * Propagate the ConcurrentHeapThetaBuffer into this sketch
   * @param localPropagationInProgress the flag to be updated when done
   * @param sketchIn any Theta sketch with the data
   * @param singleHash a single hash value
   */
  @Override public void propagate(
      final AtomicBoolean localPropagationInProgress,
      final Sketch sketchIn,
      final long singleHash) {
    final long epoch = epoch_;
    final long k = 1 << getLgNomLongs();
    if (singleHash != NOT_SINGLE_HASH           // namely, is a single hash
        && getRetainedEntries(false) < (2 * k)  // and a small sketch
    ) {                                         // then propagate myself (blocking)
      startPropagation();
      if (!validateEpoch(epoch)) {
        endPropagation(null); // do not change local flag
        return;
      }
      updateSingle(singleHash);
      endPropagation(localPropagationInProgress);
      return;
    }
    // otherwise, be nonblocking, let background thread do the work
    final BackgroundThetaPropagation job =
        new BackgroundThetaPropagation(
            this, localPropagationInProgress, sketchIn, singleHash, epoch);
    BackgroundThetaPropagation.propagationExecutorService.execute(job);
  }

  @Override public void startPropagation() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) {} //busy wait till free
  }

  @Override public void endPropagation(final AtomicBoolean localPropagationInProgress) {
    //update volatile theta, uniques estimate and propagation flag
    updateVolatileTheta();
    updateEstimationSnapshot();
    sharedPropagationInProgress_.set(false);
    if (localPropagationInProgress != null) {
      localPropagationInProgress.set(false); //clear local propagation flag
    }
  }

  @Override public void updateSingle(final long hash) {
    hashUpdate(hash);
  }

  @Override public void resetShared() {
    reset();
  }

  @Override public void rebuildShared() {
    rebuild();
  }

  @Override public CompactSketch compactShared() {
    return compact();
  }

  @Override public void reset() {
    advanceEpoch();
    super.reset();
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
  }

  @Override public boolean validateEpoch(final long epoch) {
    return epoch_ == epoch;
  }

  // Advances the epoch while there is no background propagation
  // This ensures a propagation invoked before the reset cannot affect the sketch after the reset
  // is completed.
  private void advanceEpoch() {
    startPropagation();
    //noinspection NonAtomicOperationOnVolatileField
    // this increment of a volatile field is done within the scope of the propagation
    // synchronization and hence is done by a single thread
    epoch_++;
    endPropagation(null);
  }

}
