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
  static int NUM_POOL_THREADS = 3;
  static ExecutorService propagationExecutorService =
      Executors.newWorkStealingPool(NUM_POOL_THREADS);

  private SharedThetaSketch sharedThetaSketch;
  private AtomicBoolean localPropagationInProgress;
  private Sketch sketchIn;
  private long singleHash;

  public BackgroundThetaPropagation(SharedThetaSketch sharedThetaSketch,
      final AtomicBoolean localPropagationInProgress, final Sketch sketchIn,
      final long singleHash) {
    this.sharedThetaSketch = sharedThetaSketch;
    this.localPropagationInProgress = localPropagationInProgress;
    this.sketchIn = sketchIn;
    this.singleHash = singleHash;
  }

  @Override public void run() {
    sharedThetaSketch.startPropagation();
    //At this point we are sure only a single thread is propagating data to the shared sketch

    if (singleHash > 0) {
      sharedThetaSketch.updateSingle(singleHash); // backdoor update, hash function is bypassed
    } else if (sketchIn != null) {
      final long volTheta = sharedThetaSketch.getVolatileTheta();
      assert volTheta <= sketchIn.getThetaLong() :
          "volTheta = " + volTheta + ", bufTheta = " + sketchIn.getThetaLong();

      // propagate values from input sketch one by one
      final long[] cacheIn = sketchIn.getCache();
      final int len = cacheIn.length;

      if (sketchIn.isOrdered()) { //Ordered compact, Use early stop
        for (int i = 0; i < len; i++) {
          final long hashIn = cacheIn[i];
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

    //update volatile theta, uniques estimate and propagation flag
    sharedThetaSketch.updateVolatileTheta();
    sharedThetaSketch.updateEstimationSnapshot();
    //propagation completed
    sharedThetaSketch.endPropagation();
    localPropagationInProgress.set(false); //clear local propagation flag
  }

}
