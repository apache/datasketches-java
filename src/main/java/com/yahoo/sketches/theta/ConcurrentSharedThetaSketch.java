/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An interface to define the API of a concurrent shared theta sketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time
 *
 * @author eshcar
 */
interface ConcurrentSharedThetaSketch {

  long NOT_SINGLE_HASH = -1L;
  double MIN_ERROR = 0.0000001;

  static long getLimit(long k, double error) {
    return 2 * Math.min((k), (long)Math.ceil(1.0/Math.pow(Math.max(error,MIN_ERROR), 2.0)));
  }

  /**
   * Completes the propagation: end mutual exclusion block.
   * Notifies the local thread the propagation is completed
   *
   * @param localPropagationInProgress the synchronization primitive through which propagator
   *                                   notifies local thread the propagation is completed
   */
  void endPropagation(AtomicBoolean localPropagationInProgress);

  /**
   * Returns a (fresh) estimation of the number of unique entries
   * @return a (fresh) estimation of the number of unique entries
   */
  double getEstimationSnapshot();

  /**
   * Returns the value of the volatile theta manged by the shared sketch
   * @return the value of the volatile theta manged by the shared sketch
   */
  long getVolatileTheta();

  /**
   * Returns true if a propagation is in progress, otherwise false
   * @return an indication of whether there is a pending propagation in progress
   */
  boolean isPropagationInProgress();

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * @return true if the sketch is in estimation mode.
   */
  boolean isSharedEstimationMode();

  /**
   * Propagates the given sketch or hash value into this sketch
   *
   * @param localPropagationInProgress the flag to be updated when propagation is done
   * @param sketchIn                   any Theta sketch with the data
   * @param singleHash                 a single hash value
   */
  void propagate(final AtomicBoolean localPropagationInProgress, final Sketch sketchIn,
      final long singleHash);

  default long getExactLimit() {
    return getLimit(calcK(), getError());
  }

  long calcK();

  double getError();

  // ----------------------------------
  // Methods for tests
  // ----------------------------------

  /**
   * Ensures mutual exclusion. No other thread can update the shared sketch while propagation is
   * in progress
   */
  void startPropagation();

  /**
   * Updates the estimation of the number of unique entries by capturing a snapshot of the sketch
   * data, namely, volatile theta and the num of valid entries in the sketch
   */
  void updateEstimationSnapshot();

  /**
   * Updates the shared sketch with the given hash
   * @param hash to be propagated to the shared sketch
   */
  void sharedHashUpdate(long hash);

  /**
   * Updates the value of the volatile theta by extracting it from the underlying sketch managed
   * by the shared sketch
   */
  void updateVolatileTheta();

  /**
   * Validates the shared sketch is in the context of the given epoch
   *
   * @param epoch the epoch number to be validates
   * @return true iff the shared sketch is in the context of the given epoch
   */
  boolean validateEpoch(long epoch);

  // ----------------------------------
  // Methods for tests
  // ----------------------------------

  /**
   * Resets the content of the shared sketch to an empty sketch
   */
  void resetShared();

}

