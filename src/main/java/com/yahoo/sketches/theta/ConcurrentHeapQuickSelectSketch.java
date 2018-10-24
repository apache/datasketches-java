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
public class ConcurrentHeapQuickSelectSketch extends HeapQuickSelectSketch
    implements SharedThetaSketch {

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
    sharedPropagationInProgress_ = new AtomicBoolean(false);
  }

  //Concurrent methods

  /**
   * Returns a (fresh) estimation of the number of unique entries
   * @return a (fresh) estimation of the number of unique entries
   */
  @Override public double getEstimationSnapshot() {
    return volatileEstimate_;
  }

  /**
   * Updates the estimation of the number of unique entries by capturing a snapshot of the sketch
   * data, namely, volatile theta and the num of valid entries in the sketch
   */
  @Override public void updateEstimationSnapshot() {
    volatileEstimate_ = getEstimate();
  }

  /**
   * Returns the value of the volatile theta manged by the shared sketch
   * @return the value of the volatile theta manged by the shared sketch
   */
  @Override public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  /**
   * Updates the value of the volatile theta by extracting it from the underlying sketch managed
   * by the shared sketch
   */
  @Override public void updateVolatileTheta() {
    volatileThetaLong_ = getThetaLong();
  }

  /**
   * Returns true if a propagation is in progress, otherwise false
   * @return an indication of whether there is a pending propagation in progress
   */
  @Override public boolean isPropagationInProgress() {
    return sharedPropagationInProgress_.get();
  }

  /**
   * Propagates the given sketch or hash value into this sketch
   *
   * @param localPropagationInProgress the flag to be updated when done
   * @param sketchIn                   any Theta sketch with the data
   * @param singleHash                 a single hash value
   */
  @Override public void propagate(final AtomicBoolean localPropagationInProgress,
      final Sketch sketchIn, final long singleHash) {
    final long epoch = epoch_;
    final long k = 1 << getLgNomLongs();
    if (singleHash != NOT_SINGLE_HASH            // namely, is a single hash
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
        new BackgroundThetaPropagation(this, localPropagationInProgress, sketchIn, singleHash,
            epoch);
    BackgroundThetaPropagation.propagationExecutorService.execute(job);
  }

  /**
   * Ensures mutual exclusion. No other thread can update the shared sketch while propagation is
   * in progress
   */
  @Override public void startPropagation() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) {
    } //busy wait till free
  }

  /**
   * Completes the propagation: end mutual exclusion block.
   * Notifies the local thread the propagation is completed
   *
   * @param localPropagationInProgress the synchronization primitive through which propagator
   *                                   notifies local thread the propagation is completed
   */
  @Override public void endPropagation(final AtomicBoolean localPropagationInProgress) {
    //update volatile theta, uniques estimate and propagation flag
    updateVolatileTheta();
    updateEstimationSnapshot();
    sharedPropagationInProgress_.set(false);
    if (localPropagationInProgress != null) {
      localPropagationInProgress.set(false); //clear local propagation flag
    }
  }

  /**
   * Validates the shared sketch is in the context of the given epoch
   *
   * @param epoch the epoch number to be validates
   * @return true iff the shared sketch is in the context of the given epoch
   */
  @Override public boolean validateEpoch(final long epoch) {
    return epoch_ == epoch;
  }

  /**
   * Updates the shared sketch with the given hash
   * @param hash to be propagated to the shared sketch
   */
  @Override public void updateSingle(final long hash) {
    hashUpdate(hash);
  }

  /**
   * Resets the content of the shared sketch to an empty sketch
   */
  @Override public void resetShared() {
    reset();
  }

  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   */
  @Override public void rebuildShared() {
    rebuild();
  }

  /**
   * Converts this UpdateSketch to an ordered CompactSketch on the Java heap.
   * @return this sketch as an ordered CompactSketch on the Java heap.
   */
  @Override public CompactSketch compactShared() {
    return compact();
  }

  /**
   * Resets this sketch back to a virgin empty state.
   * Takes care of mutual exclusion with propagation thread
   */
  @Override public void reset() {
    advanceEpoch();
    super.reset();
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
  }

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
