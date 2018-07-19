/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static java.lang.Math.floor;
import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentHeapThetaBuffer extends HeapUpdateSketch {
  private final Family MY_FAMILY;

  private final int preambleLongs_;
  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  private int curCount_;
  private long thetaLong_;
  private boolean empty_;

  private long[] cache_;

  private final ConcurrentDirectThetaSketch shared;
  private final AtomicBoolean propagationInProgress;
  private final boolean propagateOrderedCompact;

  //used only by factory
  ConcurrentHeapThetaBuffer(
      final int lgNomLongs,
      final long seed,
      final int cacheLimit,
      final ConcurrentDirectThetaSketch shared,
      final boolean propagateOrderedCompact) {
    super(lgNomLongs,
        seed,
        1.0F, //p
        ResizeFactor.X1); //rf
    MY_FAMILY = Family.QUICKSELECT;
    preambleLongs_ = Family.QUICKSELECT.getMinPreLongs();
    lgArrLongs_ = lgNomLongs + 1;
    final int maxLimit = (int) floor(REBUILD_THRESHOLD * (1 << lgArrLongs_));
    hashTableThreshold_ = max(min(cacheLimit, maxLimit), 0);
    curCount_ = 0;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = new long[1 << lgArrLongs_];

    this.shared = shared;
    propagationInProgress = shared.getPropogationInProgress();
    this.propagateOrderedCompact = propagateOrderedCompact;
  }

  //Sketch

  @Override //specifically overridden
  public double getEstimate() {
    return shared.getEstimationSnapshot();
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public byte[] toByteArray() { //TODO Don't need this
    return toByteArray(preambleLongs_, (byte) MY_FAMILY.getID());
  }

  @Override
  public Family getFamily() {
    return MY_FAMILY;
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() { //TODO Don't need this
    //this buffer never rebuilds
    return this;
  }

  @Override
  public final void reset() { //TODO Don't need this
    //do nothing
  }

  //restricted methods

  @Override
  int getCurrentPreambleLongs(final boolean compact) { //TODO Don't need this
    if (!compact) { return preambleLongs_; }
    return computeCompactPreLongs(thetaLong_, empty_, curCount_);
  }

  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }

  @Override
  WritableMemory getMemory() {
    return null;
  }

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  @Override
  boolean isDirty() {
    return false;
  }

  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > hashTableThreshold_;
  }

  void reset(final long thetaLong) {
    Arrays.fill(cache_, 0, 1 << lgArrLongs_, 0L);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  thetaLong;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) { //Simplified
    HashOperations.checkHashCorruption(hash);
    empty_ = false;

    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) {
      return RejectedOverTheta; //signal that hash was rejected due to theta.
    }

    //The duplicate test
    if (HashOperations.hashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0) {
      return RejectedDuplicate; //Duplicate, not inserted
    }
    //insertion occurred, must increment curCount
    curCount_++;

    if (isOutOfSpace(curCount_ + 1)) {
        propagateToSharedSketch();
    }
    return InsertedCountIncremented;
  }

  private void propagateToSharedSketch() { //Added
    while (propagationInProgress.get()) {} //busy wait until free. TODO compareAndSet( ??
    propagationInProgress.set(true);
    final HeapCompactOrderedSketch compactOrderedSketch = propagateOrderedCompact
        ? (HeapCompactOrderedSketch) compact()
        : null;
    shared.propagate(this,  compactOrderedSketch);
  }

}
