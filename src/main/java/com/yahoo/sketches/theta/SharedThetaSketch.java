package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An interface to define the API of a shared theta sketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time
 *
 * @author eshcar
 */
public interface SharedThetaSketch {

  long NOT_SINGLE_HASH = -1L;

  /**
   * Returns a (fresh) estimation of the number of unique entries
   * @return a (fresh) estimation of the number of unique entries
   */
  double getEstimationSnapshot();

  /**
   * Updates the estimation of the number of unique entries by capturing a snapshot of the sketch
   * data, namely, volatile theta and the num of valid entries in the sketch
   */
  void updateEstimationSnapshot();

  /**
   * Returns the value of the volatile theta manged by the shared sketch
   * @return the value of the volatile theta manged by the shared sketch
   */
  long getVolatileTheta();

  /**
   * Updates the value of the volatile theta by extracting it from the underlying sketch managed
   * by the shared sketch
   */
  void updateVolatileTheta();

  /**
   * Returns true if a propagation is in progress, otherwise false
   * @return an indication of whether there is a pending propagation in progress
   */
  boolean isPropagationInProgress();

  /**
   * Propagates the given sketch or hash value into this sketch
   *
   * @param localPropagationInProgress the flag to be updated when propagation is done
   * @param sketchIn                   any Theta sketch with the data
   * @param singleHash                 a single hash value
   */
  void propagate(final AtomicBoolean localPropagationInProgress, final Sketch sketchIn,
      final long singleHash);

  /**
   * Ensures mutual exclusion. No other thread can update the shared sketch while propagation is
   * in progress
   */
  void startPropagation();

  /**
   * Completes the propagation: end mutual exclusion block.
   * Notifies the local thread the propagation is completed
   *
   * @param localPropagationInProgress the synchronization primitive through which propagator
   *                                   notifies local thread the propagation is completed
   */
  void endPropagation(AtomicBoolean localPropagationInProgress);

  /**
   * Validates the shared sketch is in the context of the given epoch
   *
   * @param epoch the epoch number to be validates
   * @return true iff the shared sketch is in the context of the given epoch
   */
  boolean validateEpoch(long epoch);

  /**
   * Updates the shared sketch with the given hash
   * @param hash to be propagated to the shared sketch
   */
  void updateSingle(long hash);

  // ----------------------------------
  // Methods for characterization tests
  // ----------------------------------

  /**
   * Resets the content of the shared sketch to an empty sketch
   */
  void resetShared();

  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   */
  void rebuildShared();

  /**
   * Converts this UpdateSketch to an ordered CompactSketch on the Java heap.
   * @return this sketch as an ordered CompactSketch on the Java heap.
   */
  CompactSketch compactShared();

}
