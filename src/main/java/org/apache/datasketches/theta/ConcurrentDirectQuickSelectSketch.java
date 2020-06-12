/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.memory.WritableMemory;

/**
 * A concurrent shared sketch that is based on DirectQuickSelectSketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time.
 * Background propagation threads are used to propagate data from thread local buffers into this
 * sketch which stores the most up-to-date estimation of number of unique items.
 *
 * @author eshcar
 * @author Lee Rhodes
 */
class ConcurrentDirectQuickSelectSketch extends DirectQuickSelectSketch
    implements ConcurrentSharedThetaSketch {

  // The propagation thread
  private ExecutorService executorService_;

  // A flag to coordinate between several eager propagation threads
  private final AtomicBoolean sharedPropagationInProgress_;

  // Theta value of concurrent sketch
  private volatile long volatileThetaLong_;

  // A snapshot of the estimated number of unique entries
  private volatile double volatileEstimate_;

  // Num of retained entries in which the sketch toggles from sync (exact) mode to async
  //  propagation mode
  private final long exactLimit_;

  // An epoch defines an interval between two resets. A propagation invoked at epoch i cannot
  // affect the sketch at epoch j > i.
  private volatile long epoch_;

  /**
   * Construct a new sketch instance and initialize the given Memory as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed       <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param maxConcurrencyError the max error value including error induced by concurrency.
   * @param dstMem     the given Memory object destination. It cannot be null.
   */
  ConcurrentDirectQuickSelectSketch(final int lgNomLongs, final long seed,
      final double maxConcurrencyError, final WritableMemory dstMem) {
    super(lgNomLongs, seed, 1.0F, //p
      ResizeFactor.X1, //rf,
      null, dstMem, false); //unionGadget

    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
    exactLimit_ = ConcurrentSharedThetaSketch.computeExactLimit(1L << getLgNomLongs(),
        maxConcurrencyError);
    sharedPropagationInProgress_ = new AtomicBoolean(false);
    epoch_ = 0;
    initBgPropagationService();
  }

  ConcurrentDirectQuickSelectSketch(final UpdateSketch sketch, final long seed,
      final double maxConcurrencyError, final WritableMemory dstMem) {
    super(sketch.getLgNomLongs(), seed, 1.0F, //p
        ResizeFactor.X1, //rf,
        null, //mem Req Svr
        dstMem,
        false); //unionGadget

    exactLimit_ = ConcurrentSharedThetaSketch.computeExactLimit(1L << getLgNomLongs(),
        maxConcurrencyError);
    sharedPropagationInProgress_ = new AtomicBoolean(false);
    epoch_ = 0;
    initBgPropagationService();
    for (final long hashIn : sketch.getCache()) {
      propagate(hashIn);
    }
    wmem_.putLong(THETA_LONG, sketch.getThetaLong());
    updateVolatileTheta();
    updateEstimationSnapshot();
  }

  //Sketch overrides

  @Override
  public double getEstimate() {
    return volatileEstimate_;
  }

  @Override
  public boolean isEstimationMode() {
    return (getRetainedEntries(false) > exactLimit_) || super.isEstimationMode();
  }

  @Override
  public byte[] toByteArray() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) { } //busy wait till free
    final byte[] res = super.toByteArray();
    sharedPropagationInProgress_.set(false);
    return res;
  }

  //UpdateSketch overrides

  @Override
  public UpdateSketch rebuild() {
    super.rebuild();
    updateEstimationSnapshot();
    return this;
  }

  /**
   * {@inheritDoc}
   * Takes care of mutual exclusion with propagation thread.
   */
  @Override
  public void reset() {
    advanceEpoch();
    super.reset();
    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    final String msg = "No update method should be called directly to a shared theta sketch."
        + " Updating the shared sketch is only permitted through propagation from local sketches.";
    throw new UnsupportedOperationException(msg);
  }

  //ConcurrentSharedThetaSketch declarations

  @Override
  public long getExactLimit() {
    return exactLimit_;
  }

  @Override
  public boolean startEagerPropagation() {
    while (!sharedPropagationInProgress_.compareAndSet(false, true)) { } //busy wait till free
    return (!isEstimationMode());// no eager propagation is allowed in estimation mode
  }

  @Override
  public void endPropagation(final AtomicBoolean localPropagationInProgress, final boolean isEager) {
    //update volatile theta, uniques estimate and propagation flag
    updateVolatileTheta();
    updateEstimationSnapshot();
    if (isEager) {
      sharedPropagationInProgress_.set(false);
    }
    if (localPropagationInProgress != null) {
      localPropagationInProgress.set(false); //clear local propagation flag
    }
  }

  @Override
  public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  @Override
  public void awaitBgPropagationTermination() {
    try {
      executorService_.shutdown();
      while (!executorService_.awaitTermination(1, TimeUnit.MILLISECONDS)) {
        Thread.sleep(1);
      }
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void initBgPropagationService() {
    executorService_ = ConcurrentPropagationService.getExecutorService(Thread.currentThread().getId());
  }

  @Override
  public boolean propagate(final AtomicBoolean localPropagationInProgress,
                           final Sketch sketchIn, final long singleHash) {
    final long epoch = epoch_;
    if ((singleHash != NOT_SINGLE_HASH)                   // namely, is a single hash and
        && (getRetainedEntries(false) < exactLimit_)) {   // a small sketch then propagate myself (blocking)
      if (!startEagerPropagation()) {
        endPropagation(localPropagationInProgress, true);
        return false;
      }
      if (!validateEpoch(epoch)) {
        endPropagation(null, true); // do not change local flag
        return true;
      }
      propagate(singleHash);
      endPropagation(localPropagationInProgress, true);
      return true;
    }
    // otherwise, be nonblocking, let background thread do the work
    final ConcurrentBackgroundThetaPropagation job = new ConcurrentBackgroundThetaPropagation(
        this, localPropagationInProgress, sketchIn, singleHash, epoch);
    executorService_.execute(job);
    return true;
  }

  @Override
  public void propagate(final long singleHash) {
    super.hashUpdate(singleHash);
  }

  @Override
  public void updateEstimationSnapshot() {
    volatileEstimate_ = super.getEstimate();
  }

  @Override
  public void updateVolatileTheta() {
    volatileThetaLong_ = getThetaLong();
  }

  @Override
  public boolean validateEpoch(final long epoch) {
    return epoch_ == epoch;
  }

  //Restricted

  /**
   * Advances the epoch while there is no background propagation
   * This ensures a propagation invoked before the reset cannot affect the sketch after the reset
   * is completed. Ignore VO_VOLATILE_INCREMENT findbugs warning, it is False Positive.
   */
  private void advanceEpoch() {
    awaitBgPropagationTermination();
    startEagerPropagation();
    ConcurrentPropagationService.resetExecutorService(Thread.currentThread().getId());
    //noinspection NonAtomicOperationOnVolatileField
    // this increment of a volatile field is done within the scope of the propagation
    // synchronization and hence is done by a single thread.
    epoch_++;
    endPropagation(null, true);
    initBgPropagationService();
  }

}
