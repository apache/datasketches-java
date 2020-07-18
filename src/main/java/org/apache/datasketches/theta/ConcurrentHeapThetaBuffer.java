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

import static org.apache.datasketches.theta.UpdateReturnState.ConcurrentBufferInserted;
import static org.apache.datasketches.theta.UpdateReturnState.ConcurrentPropagated;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedOverTheta;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.datasketches.HashOperations;
import org.apache.datasketches.ResizeFactor;

/**
 * This is a theta filtering, bounded size buffer that operates in the context of a single writing
 * thread.  When the buffer becomes full its content is propagated into the shared sketch, which
 * may be on a different thread. The limit on the buffer size is configurable. A bound of size 1
 * allows the combination of buffers and shared sketch to maintain an error bound in real-time
 * that is close to the error bound of a sequential theta sketch.  Allowing larger buffer sizes
 * enables amortization of the cost propagations and substantially improves overall system throughput.
 * The error caused by the buffering is essentially a perspecitive of time and synchronization
 * and not really a true error.  At the end of a stream, after all the buffers have synchronized with
 * the shared sketch, there is no additional error.
 * Propagation is done either synchronously by the updating thread, or asynchronously by a
 * background propagation thread.
 *
 * <p>This is a buffer, not a sketch, and it extends the <i>HeapQuickSelectSketch</i>
 * in order to leverage some of the sketch machinery to make its work simple. However, if this
 * buffer receives a query, like <i>getEstimate()</i>, the correct answer does not come from the super
 * <i>HeapQuickSelectSketch</i>, which knows nothing about the concurrency relationship to the
 * shared concurrent sketch, it must come from the shared concurrent sketch. As a result nearly all
 * of the inherited sketch methods are redirected to the shared concurrent sketch.
 *
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentHeapThetaBuffer extends HeapQuickSelectSketch {

  // Shared sketch consisting of the global sample set and theta value.
  private final ConcurrentSharedThetaSketch shared;

  // A flag indicating whether the shared sketch is in shared mode and requires eager propagation
  // Initially this is true. Once it is set to false (estimation mode) it never flips back.
  private boolean isExactMode;

  // A flag to indicate if we expect the propagated data to be ordered
  private final boolean propagateOrderedCompact;

  // Propagation flag is set to true while propagation is in progress (or pending).
  // It is the synchronization primitive to coordinate the work with the propagation thread.
  private final AtomicBoolean localPropagationInProgress;

  ConcurrentHeapThetaBuffer(final int lgNomLongs, final long seed,
      final ConcurrentSharedThetaSketch shared, final boolean propagateOrderedCompact,
      final int maxNumLocalThreads) {
    super(computeLogBufferSize(lgNomLongs, shared.getExactLimit(), maxNumLocalThreads),
      seed, 1.0F, //p
      ResizeFactor.X1, //rf
      false); //not a union gadget

    this.shared = shared;
    isExactMode = true;
    this.propagateOrderedCompact = propagateOrderedCompact;
    localPropagationInProgress = new AtomicBoolean(false);
  }

  private static int computeLogBufferSize(final int lgNomLongs, final long exactSize,
      final int maxNumLocalBuffers) {
    return Math.min(lgNomLongs, (int)Math.log(Math.sqrt(exactSize) / (2 * maxNumLocalBuffers)));
  }

  //concurrent restricted methods

  /**
   * Propagates a single hash value to the shared sketch
   *
   * @param hash to be propagated
   */
  private boolean propagateToSharedSketch(final long hash) {
    //noinspection StatementWithEmptyBody
    while (localPropagationInProgress.get()) {
    } //busy wait until previous propagation completed
    localPropagationInProgress.set(true);
    final boolean res = shared.propagate(localPropagationInProgress, null, hash);
    //in this case the parent empty_ and curCount_ were not touched
    thetaLong_ = shared.getVolatileTheta();
    return res;
  }

  /**
   * Propagates the content of the buffer as a sketch to the shared sketch
   */
  private void propagateToSharedSketch() {
    //noinspection StatementWithEmptyBody
    while (localPropagationInProgress.get()) {
    } //busy wait until previous propagation completed

    final CompactSketch compactSketch = compact(propagateOrderedCompact, null);
    localPropagationInProgress.set(true);
    shared.propagate(localPropagationInProgress, compactSketch,
        ConcurrentSharedThetaSketch.NOT_SINGLE_HASH);
    super.reset();
    thetaLong_ = shared.getVolatileTheta();
  }

  //Public Sketch overrides proxies to shared concurrent sketch

  @Override
  public int getCompactBytes() {
    return shared.getCompactBytes();
  }

  @Override
  public int getCurrentBytes() {
    return shared.getCurrentBytes();
  }

  @Override
  public double getEstimate() {
    return shared.getEstimate();
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    return shared.getLowerBound(numStdDev);
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return shared.getUpperBound(numStdDev);
  }

  @Override
  public boolean hasMemory() {
    return shared.hasMemory();
  }

  @Override
  public boolean isDirect() {
    return shared.isDirect();
  }

  @Override
  public boolean isEmpty() {
    return shared.isEmpty();
  }

  @Override
  public boolean isEstimationMode() {
    return shared.isEstimationMode();
  }

  //End of proxies

  @Override
  public byte[] toByteArray() {
    throw new UnsupportedOperationException("Local theta buffer need not be serialized");
  }

  //Public UpdateSketch overrides

  @Override
  public void reset() {
    super.reset();
    isExactMode = true;
    localPropagationInProgress.set(false);
  }

  //Restricted UpdateSketch overrides

  /**
   * Updates buffer with given hash value.
   * Triggers propagation to shared sketch if buffer is full.
   *
   * @param hash the given input hash value.  A hash of zero or Long.MAX_VALUE is ignored.
   * A negative hash value will throw an exception.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  @Override
  UpdateReturnState hashUpdate(final long hash) {
    if (isExactMode) {
      isExactMode = !shared.isEstimationMode();
    }
    HashOperations.checkHashCorruption(hash);
    if ((getHashTableThreshold() == 0) || isExactMode ) {
      //The over-theta and zero test
      if (HashOperations.continueCondition(getThetaLong(), hash)) {
        return RejectedOverTheta; //signal that hash was rejected due to theta or zero.
      }
      if (propagateToSharedSketch(hash)) {
        return ConcurrentPropagated;
      }
    }
    final UpdateReturnState state = super.hashUpdate(hash);
    if (isOutOfSpace(getRetainedEntries(true) + 1)) {
      propagateToSharedSketch();
      return ConcurrentPropagated;
    }
    if (state == UpdateReturnState.InsertedCountIncremented) {
      return ConcurrentBufferInserted;
    }
    return state;
  }


}
