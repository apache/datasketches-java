package com.yahoo.sketches.concurrent.theta;

import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author eshcar
 */
public class ConcurrentUpdateSketch extends UpdateSketchComposition {

  private static int PARALLELISM_LEVEL = 3;
  protected static ExecutorService PROPAGATION_EXECUTOR_SERVICE = Executors.newWorkStealingPool
      (PARALLELISM_LEVEL);

  private volatile long theta_;
  private volatile double estimation_;
  // A flag to coordinate between several propagation threads
  private AtomicBoolean propagationInProgress_;

  // package visibility constructors - to be created only by factory
  ConcurrentUpdateSketch(final UpdateSketch delegattee) {
    super(delegattee);
    theta_ = getThetaLong();
    estimation_ = 0;
    propagationInProgress_ = new AtomicBoolean(false);
  }

  public void propagate(final Sketch sketchIn, AtomicBoolean propagationInProgress) {
    BackgroundThetaPropagation job = new BackgroundThetaPropagation(sketchIn,
        propagationInProgress);
    PROPAGATION_EXECUTOR_SERVICE.execute(job);
  }

  public double getEstimationSnapshot() {
    return estimation_;
  }

  public long getVolatileTheta() {
    return theta_;
  }

  private class BackgroundThetaPropagation implements Runnable {

    private Sketch sketch_;
    private AtomicBoolean localThreadPropagationFlag_;

    public BackgroundThetaPropagation(Sketch sketch, AtomicBoolean localThreadPropagationFlag) {
      this.sketch_ = sketch;
      this.localThreadPropagationFlag_ = localThreadPropagationFlag;
    }

    @Override public void run() {
      assert getVolatileTheta() <= sketch_.getThetaLong();

      while(!propagationInProgress_.compareAndSet(false,true)) {
        //busy wait until can propagate
      }
      //At this point we are sure only a single thread is propagating data to the shared sketch

      // propagate values from input sketch one by one
      final long[] cacheIn = sketch_.getCache();
      final int numEntries = sketch_.getRetainedEntries(false);
      for (int i = 0; i < numEntries; i++) {
        // pre-condition: cacheIn values are sorted in ascending order
        final long hashIn = cacheIn[i];
        if (hashIn >= getVolatileTheta()) {
          break; //early stop
        }
        hashUpdate(hashIn); // backdoor update, hash function is bypassed
      }

      //update volatile theta, uniques estimate and propagation flag
      theta_ = getThetaLong();
      estimation_ = getEstimate();
      //propagation completed, not in-progress, set both local and shared flags
      propagationInProgress_.set(false);
      localThreadPropagationFlag_.set(false);
    }
  }

}
