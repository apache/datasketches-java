/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;

/**
 * @author eshcar
 * @author Lee Rhodes
 */
final class ConcurrentHeapThetaBuffer extends HeapUpdateSketch {
  private final Family MY_FAMILY;
  private final int preambleLongs_;
  private int lgArrLongs_;
  private int cacheLimit_;  //never serialized
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
    lgArrLongs_ = lgNomLongs;
    cacheLimit_ = cacheLimit;
    curCount_ = 0;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    cache_ = new long[1 << lgArrLongs_];

    this.shared = shared;
    propagationInProgress = shared.getPropogationInProgress();
    this.propagateOrderedCompact = propagateOrderedCompact;
  }

  //Sketch

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public byte[] toByteArray() { //TODO NEED THIS?
    return toByteArray(preambleLongs_, (byte) MY_FAMILY.getID());
  }

  @Override
  public Family getFamily() {
    return MY_FAMILY;
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    //this buffer never rebuilds
    return this;
  }

  @Override
  public final void reset() {
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  Long.MAX_VALUE;
  }

  //restricted methods

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    if (!compact) { return preambleLongs_; }
    return computeCompactPreLongs(thetaLong_, empty_, curCount_);
  }

  @Override
  WritableMemory getMemory() {
    return null;
  }

  void setThetaLong(final long theta) {
    thetaLong_ = theta;
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
    return numEntries > cacheLimit_;
  }

  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    final UpdateReturnState ret = hashUpdate(hash);
    if ((curCount_ + 1) > cacheLimit_) {
        propagateToSharedSketch();
        Arrays.fill(cache_, 0, 1 << lgArrLongs_, 0L);
    }
    return ret;
  }

  private void propagateToSharedSketch() {
    while (propagationInProgress.get()) {} //busy wait
    propagationInProgress.set(true);
    final HeapCompactOrderedSketch compactOrderedSketch = propagateOrderedCompact
        ? (HeapCompactOrderedSketch) compact()
        : null;
    shared.propagate(this,  compactOrderedSketch);
  }

}
