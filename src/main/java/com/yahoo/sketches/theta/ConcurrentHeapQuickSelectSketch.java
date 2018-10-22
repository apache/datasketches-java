package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.sketches.ResizeFactor;


/**
 * @author eshcar
 */
public class ConcurrentHeapQuickSelectSketch extends HeapQuickSelectSketch
    implements SharedThetaSketch {

  private volatile long volatileThetaLong_;
  private volatile double volatileEstimate_;
  // A flag to coordinate between several propagation threads
  private final AtomicBoolean sharedPropagationInProgress_;
  // An epoch defines an interval between two resets. A propagation invoked at epoch i cannot
  // affect the sketch at epoch j>i.
  private volatile long epoch_;


  /**
   * Construct a new sketch instance on the java heap.
   *
   * @param lgNomLongs  <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed        <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  ConcurrentHeapQuickSelectSketch(final int lgNomLongs, final long seed) {
    super(lgNomLongs, seed,
        1.0F, //p
        ResizeFactor.X1, //rf,
        false); //unionGadget
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
    sharedPropagationInProgress_ = new AtomicBoolean(false);
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
   * Propagates the given sketch or hash value into this sketch
   *
   * @param localPropagationInProgress the flag to be updated when done
   * @param sketchIn                   any Theta sketch with the data
   * @param singleHash                 a single hash value
   */
  @Override public void propagate(final AtomicBoolean localPropagationInProgress,
      final Sketch sketchIn,
      final long singleHash) {
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

  @Override public boolean validateEpoch(final long epoch) {
    return epoch_ == epoch;
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
