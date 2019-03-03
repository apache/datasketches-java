/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background propagation thread. Propagates a given sketch or a hash value from local threads
 * buffers into the shared sketch which stores the most up-to-date estimation of number of unique
 * items. This propagation is done at the background by dedicated threads, which allows
 * application threads to continue updating their local buffer.
 *
 * @author eshcar
 */
class ConcurrentBackgroundThetaPropagation implements Runnable {

  // Shared sketch to absorb the data
  private final ConcurrentSharedThetaSketch sharedThetaSketch;

  // Propagation flag of local buffer that is being processed.
  // It is the synchronization primitive to coordinate the work of the propagation with the
  // local buffer.  Updated when the propagation completes.
  private final AtomicBoolean localPropagationInProgress;


  // Sketch to be propagated to shared sketch. Can be null if only a single hash is propagated
  private final Sketch sketchIn;

  // Hash of the datum to be propagated to shared sketch. Can be ConcurrentSharedThetaSketch.NOT_SINGLE_HASH
  // if the data is propagated through a sketch.
  private final long singleHash;


  // The propagation epoch. The data can be propagated only within the context of this epoch.
  // The data should not be propagated if this epoch is not equal to the
  // shared sketch epoch.
  private final long epoch;

  ConcurrentBackgroundThetaPropagation(final ConcurrentSharedThetaSketch sharedThetaSketch,
      final AtomicBoolean localPropagationInProgress, final Sketch sketchIn, final long singleHash,
      final long epoch) {
    this.sharedThetaSketch = sharedThetaSketch;
    this.localPropagationInProgress = localPropagationInProgress;
    this.sketchIn = sketchIn;
    this.singleHash = singleHash;
    this.epoch = epoch;
  }

  /**
   * Propagation protocol:
   * 1) validate propagation is executed at the context of the right epoch, otherwise abort
   * 2) handle propagation: either of a single hash or of a sketch
   * 3) complete propagation: ping local buffer
   */
  @Override
  public void run() {
    // 1) validate propagation is executed at the context of the right epoch, otherwise abort
    if (!sharedThetaSketch.validateEpoch(epoch)) {
      // invalid epoch - should not propagate
      sharedThetaSketch.endPropagation(null, false);
      return;
    }

    // 2) handle propagation: either of a single hash or of a sketch
    if (singleHash != ConcurrentSharedThetaSketch.NOT_SINGLE_HASH) {
      sharedThetaSketch.propagate(singleHash);
    } else if (sketchIn != null) {
      final long volTheta = sharedThetaSketch.getVolatileTheta();
      assert volTheta <= sketchIn.getThetaLong() :
          "volTheta = " + volTheta + ", bufTheta = " + sketchIn.getThetaLong();

      // propagate values from input sketch one by one
      final long[] cacheIn = sketchIn.getCache();

      if (sketchIn.isOrdered()) { //Ordered compact, Use early stop
        for (final long hashIn : cacheIn) {
          if (hashIn >= volTheta) {
            break; //early stop
          }
          sharedThetaSketch.propagate(hashIn);
        }
      } else { //not ordered, also may have zeros (gaps) in the array.
        for (final long hashIn : cacheIn) {
          if (hashIn > 0) {
            sharedThetaSketch.propagate(hashIn);
          }
        }
      }
    }

    // 3) complete propagation: ping local buffer
    sharedThetaSketch.endPropagation(localPropagationInProgress, false);
  }

}
