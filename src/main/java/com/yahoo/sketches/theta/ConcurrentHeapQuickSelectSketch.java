package com.yahoo.sketches.theta;

import com.yahoo.sketches.ResizeFactor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author eshcar
 */
public class ConcurrentHeapQuickSelectSketch extends HeapQuickSelectSketch
    implements SharedThetaSketch{

  private volatile long volatileThetaLong_;
  private volatile double volatileEstimate_;
  // A flag to coordinate between several propagation threads
  private AtomicBoolean sharedPropagationInProgress_;


  /**
   * Construct a new sketch instance on the java heap.
   *
   * @param lgNomLongs  <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed        <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  ConcurrentHeapQuickSelectSketch(int lgNomLongs, long seed) {
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
  @Override public void propagate(AtomicBoolean localPropagationInProgress, Sketch sketchIn,
      long singleHash) {
    final BackgroundThetaPropagation job =
        new BackgroundThetaPropagation(this, localPropagationInProgress, sketchIn, singleHash);
    BackgroundThetaPropagation.propagationExecutorService.execute(job);
  }

  @Override public void startPropagation() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) {} //busy wait till free
  }

  @Override public void endPropagation() {
    sharedPropagationInProgress_.set(false);
  }

  @Override public void updateSingle(long hash) {
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
    while (sharedPropagationInProgress_.get()) {}  // wait until no background processing
    super.reset();
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
  }

}
