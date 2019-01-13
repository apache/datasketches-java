/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.WritableMemory;

/**
 * An interface to define the API of a concurrent shared theta sketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time
 *
 * @author eshcar
 */
interface ConcurrentSharedThetaSketch {

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
  // Methods for tests
  // ----------------------------------

  /**
   * Returns whether the shared sketch is empty
   * @return whether the shared sketch is empty
   */
  boolean isSharedEmpty();

  /**
   * Returns the number of entries that have been retained by the sketch.
   * @param valid if true, returns the number of valid entries, which are less than theta and used
   * for estimation.
   * Otherwise, return the number of all entries, valid or not, that are currently in the internal
   * sketch cache.
   * @return the number of retained entries
   */
  int getSharedRetainedEntries(final boolean valid);

  /**
   * Returns true if the this sketch's internal data structure is backed by direct (off-heap)
   * Memory.
   * @return true if the this sketch's internal data structure is backed by direct (off-heap)
   * Memory.
   */
  boolean isSharedDirect();

  /**
   * Serialize this sketch to a byte array form.
   * @return byte array of this sketch
   */
  byte[] sharedToByteArray();

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  double getSharedLowerBound(final int numStdDev);

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  double getSharedUpperBound(final int numStdDev);

  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   *
   * @return true if the sketch is in estimation mode.
   */
  boolean isSharedEstimationMode();

  /**
   * Returns the number of storage bytes required for this Sketch in its current state.
   * @param compact if true, returns the bytes required for compact form.
   * If this sketch is already in compact form this parameter is ignored.
   * @return the number of storage bytes required for this sketch
   */
  int getSharedCurrentBytes(final boolean compact);

  /**
   * Convert this UpdateSketch to a CompactSketch in the chosen form.
   *
   * <p>This compacting process converts the hash table form of an UpdateSketch to
   * a simple list of the valid hash values from the hash table.  Any hash values equal to or
   * greater than theta will be discarded.  The number of valid values remaining in the
   * Compact Sketch depends on a number of factors, but may be larger or smaller than
   * <i>Nominal Entries</i> (or <i>k</i>). It will never exceed 2<i>k</i>.  If it is critical
   * to always limit the size to no more than <i>k</i>, then <i>rebuild()</i> should be called
   * on the UpdateSketch prior to this.
   *
   * @param dstOrdered <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   * @param dstMem     <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return this sketch as a CompactSketch in the chosen form
   */
  CompactSketch compactShared(final boolean dstOrdered, final WritableMemory dstMem);

  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   * @return this sketch
   */
  UpdateSketch rebuildShared();

  /**
   * Resets the content of the shared sketch to an empty sketch
   */
  void resetShared();

  /**
   * Converts this UpdateSketch to an ordered CompactSketch on the Java heap.
   * @return this sketch as an ordered CompactSketch on the Java heap.
   */
  CompactSketch compactShared();

}
