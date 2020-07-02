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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.datasketches.memory.WritableMemory;

/**
 * An internal interface to define the API of a concurrent shared theta sketch.
 * It reflects all data processed by a single or multiple update threads, and can serve queries at
 * any time.
 *
 * @author eshcar
 */
interface ConcurrentSharedThetaSketch {

  long NOT_SINGLE_HASH = -1L;
  double MIN_ERROR = 0.0000001;

  static long computeExactLimit(long k, double error) {
    return 2 * Math.min(k, (long) Math.ceil(1.0 / Math.pow(Math.max(error,MIN_ERROR), 2.0)));
  }

  /**
   * Returns flip point (number of updates) from exact to estimate mode.
   * @return flip point from exact to estimate mode
   */
  long getExactLimit();

  /**
   * Ensures mutual exclusion. No other thread can update the shared sketch while propagation is
   * in progress
   * @return true if eager propagation was started
   */
  boolean startEagerPropagation();

  /**
   * Completes the propagation: end mutual exclusion block.
   * Notifies the local thread the propagation is completed
   *
   * @param localPropagationInProgress the synchronization primitive through which propagator
   *                                   notifies local thread the propagation is completed
   * @param isEager true if the propagation is in eager mode
   */
  void endPropagation(AtomicBoolean localPropagationInProgress, boolean isEager);

  /**
   * Returns the value of the volatile theta manged by the shared sketch
   * @return the value of the volatile theta manged by the shared sketch
   */
  long getVolatileTheta();

  /**
   * Awaits termination of background (lazy) propagation tasks
   */
  void awaitBgPropagationTermination();

  /**
   * Init background (lazy) propagation service
   */
  void initBgPropagationService();

  /**
   * (Eager) Propagates the given sketch or hash value into this sketch
   * @param localPropagationInProgress the flag to be updated when propagation is done
   * @param sketchIn any Theta sketch with the data
   * @param singleHash a single hash value
   * @return true if propagation successfully started
   */
  boolean propagate(final AtomicBoolean localPropagationInProgress, final Sketch sketchIn,
    final long singleHash);

  /**
   * (Lazy/Eager) Propagates the given hash value into this sketch
   * @param singleHash a single hash value
   */
  void propagate(final long singleHash);

  /**
   * Updates the estimation of the number of unique entries by capturing a snapshot of the sketch
   * data, namely, volatile theta and the num of valid entries in the sketch
   */
  void updateEstimationSnapshot();

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

  //The following mirrors are public methods that already exist on the "extends" side of the dual
  // inheritance. They are provided here to allow casts to this interface access
  // to these methods without having to cast back to the extended parent class.
  //
  //This allows an internal class to cast either the Concurrent Direct or Concurrent Heap
  //shared class to this interface and have access to the above special concurrent methods as
  //well as the methods below.
  //
  //For the external user all of the below methods can be obtained by casting the shared
  //sketch to UpdateSketch.  However, these methods here also act as an alias so that an
  //attempt to access these methods from the local buffer will be divered to the shared
  //sketch.

  //From Sketch

  int getCompactBytes();

  int getCurrentBytes();

  double getEstimate();

  double getLowerBound(int numStdDev);

  double getUpperBound(int numStdDev);

  boolean hasMemory();

  boolean isDirect();

  boolean isEmpty();

  boolean isEstimationMode();

  byte[] toByteArray();

  int getRetainedEntries(boolean valid);

  CompactSketch compact();

  CompactSketch compact(boolean ordered, WritableMemory wmem);

  UpdateSketch rebuild();

  void reset();
}

