package com.yahoo.sketches.theta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background propagation thread. Propagates a given sketch or a hash value from local threads
 * buffers into the shared sketch which stores the most up-to-date estimation of number of unique
 * items.
 * @author eshcar
 */
class BackgroundThetaPropagation implements Runnable {
  static private final int NUM_POOL_THREADS = 3;
  static final ExecutorService propagationExecutorService =
      Executors.newWorkStealingPool(NUM_POOL_THREADS);

  private final SharedThetaSketch sharedThetaSketch;
  private final AtomicBoolean localPropagationInProgress;
  private final Sketch sketchIn;
  private final long singleHash;
  private final long epoch;

  public BackgroundThetaPropagation(final SharedThetaSketch sharedThetaSketch,
      final AtomicBoolean localPropagationInProgress, final Sketch sketchIn,
      final long singleHash, final long epoch) {
    this.sharedThetaSketch = sharedThetaSketch;
    this.localPropagationInProgress = localPropagationInProgress;
    this.sketchIn = sketchIn;
    this.singleHash = singleHash;
    this.epoch = epoch;
  }

  @Override public void run() {
    sharedThetaSketch.startPropagation();
    //At this point we are sure only a single thread is propagating data to the shared sketch

    if (!sharedThetaSketch.validateEpoch(epoch)) {
      // invalid epoch - should not propagate
      sharedThetaSketch.endPropagation(null);
      return;
    }

    if (singleHash != SharedThetaSketch.NOT_SINGLE_HASH) {
      sharedThetaSketch.updateSingle(singleHash); // backdoor update, hash function is bypassed
    } else if (sketchIn != null) {
      final long volTheta = sharedThetaSketch.getVolatileTheta();
      assert volTheta <= sketchIn.getThetaLong() :
          "volTheta = " + volTheta + ", bufTheta = " + sketchIn.getThetaLong();

      // propagate values from input sketch one by one
      final long[] cacheIn = sketchIn.getCache();
      final int len = cacheIn.length;

      if (sketchIn.isOrdered()) { //Ordered compact, Use early stop
        for (final long hashIn : cacheIn) {
          if (hashIn >= volTheta) {
            break; //early stop
          }
          sharedThetaSketch.updateSingle(hashIn); // backdoor update, hash function is bypassed
        }
      } else { //not ordered, also may have zeros (gaps) in the array.
        for (int i = 0; i < len; i++) {
          final long hashIn = cacheIn[i];
          if (hashIn > 0) {
            sharedThetaSketch.updateSingle(hashIn); // backdoor update, hash function is bypassed
          }
        }
      }
    }

    //complete propagation
    sharedThetaSketch.endPropagation(localPropagationInProgress);
  }

}
