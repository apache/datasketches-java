/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
public class ConcurrentDirectThetaSketch extends DirectQuickSelectSketch {
  static ExecutorService propagationExecutorService;
  private volatile long volatileThetaLong_;
  private volatile double volatileEstimate_;
  // A flag to coordinate between several propagation threads
  private AtomicBoolean sharedPropagationInProgress_;
  private int poolThreads;
  private int insertedIncremented = 0;
  private int rejectedDuplicate = 0;
  private int rejectedOverTheta = 0;
  private int other = 0;


  /**
   * Get a new sketch instance and initialize the given Memory as its backing store.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @param dstMem the given Memory object destination. It cannot be null.
   * The hash table area will be cleared prior to use.
   * @param poolThreads the number of ExecutorService, Executors.newWorkStealingPool poolThreads.
   */
  ConcurrentDirectThetaSketch(
      final int lgNomLongs,
      final long seed,
      final WritableMemory dstMem,
      final int poolThreads) {
    super(lgNomLongs,
        seed,
        1.0F, //p
        ResizeFactor.X1, //rf,
        null,
        dstMem,
        false);
    this.poolThreads = poolThreads;
    if (propagationExecutorService == null) {
      propagationExecutorService = Executors.newWorkStealingPool(poolThreads);
    }

    volatileThetaLong_ = Long.MAX_VALUE;
    volatileEstimate_ = 0;
    sharedPropagationInProgress_ = new AtomicBoolean(false);
  }

  //@Override
  //UpdateReturnState hashUpdate(final long hash) {
  //  final UpdateReturnState state = super.hashUpdate(hash);
  //  switch (state) {
  //    case InsertedCountIncremented :
  //      insertedIncremented++;
  //      break;
  //    case RejectedDuplicate :
  //      rejectedDuplicate++;
  //      break;
  //    case RejectedOverTheta :
  //      rejectedOverTheta++;
  //      break;
  //    default:
  //      other++;
  //      break;
  //  }
  //  return state;
  //}

  //Concurrent methods

  public double getEstimationSnapshot() {
    return volatileEstimate_;
  }

  public long getVolatileTheta() {
    return volatileThetaLong_;
  }

  AtomicBoolean getPropagationInProgress() {
    return sharedPropagationInProgress_;
  }

  int getPoolThreads() {
    return poolThreads;
  }

  int[] getCounts() {
    return new int[] {insertedIncremented, rejectedDuplicate, rejectedOverTheta, other};
  }

  /**
   * Propogate the ConcurrentHeapThetaBuffer into this sketch
   * @param bufferIn the given ConcurrentHeapThetaBuffer
   * @param compactSketch an optional, ordered compact sketch with the data
   * @param singleHash a single hash value
   */
  public void propagate(
      final ConcurrentHeapThetaBuffer bufferIn,
      final CompactSketch compactSketch,
      final long singleHash) {
    final BackgroundThetaPropagation job =
        new BackgroundThetaPropagation(bufferIn, compactSketch, singleHash);
    propagationExecutorService.execute(job);
  }

  private class BackgroundThetaPropagation implements Runnable {
    private ConcurrentHeapThetaBuffer bufferIn;
    private CompactSketch compactSketch;
    private long singleHash;

    public BackgroundThetaPropagation(
        final ConcurrentHeapThetaBuffer bufferIn,
        final CompactSketch compactSketch,
        final long singleHash) {
      this.bufferIn = bufferIn;
      this.compactSketch = compactSketch;
      this.singleHash = singleHash;
    }

    @Override
    public void run() {
      while (!sharedPropagationInProgress_.compareAndSet(false,true)) {} ///busy wait till free

      //At this point we are sure only a single thread is propagating data to the shared sketch

      final long vT = getVolatileTheta();
      final long bT = bufferIn.getThetaLong();
      assert vT <= bT : "VT = " + vT + ", bT = " + bT;

      if (singleHash > 0) {
        hashUpdate(singleHash); // backdoor update, hash function is bypassed
      } else {
        // propagate values from input sketch one by one
        if ((compactSketch != null) && (compactSketch.isOrdered())) { //Use early stop
          final long[] cacheIn = compactSketch.getCache();
          for (int i = 0; i < cacheIn.length; i++) {
            final long hashIn = cacheIn[i];
            if (hashIn >= getVolatileTheta()) {
              break; //early stop
            }
            hashUpdate(hashIn); // backdoor update, hash function is bypassed
          }
        } else {
          final long[] cacheIn = bufferIn.getCache();
          for (int i = 0; i < cacheIn.length; i++) {
            final long hashIn = cacheIn[i];
            hashUpdate(hashIn); // backdoor update, hash function is bypassed
          }
        }
      }

      //update volatile theta, uniques estimate and propagation flag
      final long sharedThetaLong = getThetaLong();
      volatileThetaLong_ = sharedThetaLong;
      volatileEstimate_ = getEstimate();
      //propagation completed
      bufferIn.reset(); //reset & clear local propagation flag
      sharedPropagationInProgress_.set(false);
    }

  }

}
