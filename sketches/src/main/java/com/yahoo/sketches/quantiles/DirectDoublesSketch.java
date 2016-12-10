/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DoublesUtil.checkMemCapacity;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;

/**
 * Implements the DoublesSketch off-heap.
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 */
public final class DirectDoublesSketch extends DoublesSketch {
  private static final int DIRECT_PRE_LONGS = 4; //includes min and max values
  private Memory mem_;
  private Object memObj_;
  private long memAdd_; //

  //**CONSTRUCTORS**********************************************************
  private DirectDoublesSketch(final int k) {
    super(k);
  }

  static DirectDoublesSketch newInstance(final int k, final Memory dstMem) {
    final DirectDoublesSketch dds = new DirectDoublesSketch(k);
    final long memCap = dstMem.getCapacity();
    final long minCap = (DIRECT_PRE_LONGS + 2 * k) << 3; //require at least full base buffer
    if (memCap < minCap) {
      throw new SketchesArgumentException(
          "Destination Memory too small: " + memCap + " < " + minCap);
    }
    final Object memObj = dstMem.array();
    final long memAdd = dstMem.getCumulativeOffset(0L);

    //initialize dstMem
    insertPreLongs(memObj, memAdd, 2);
    insertSerVer(memObj, memAdd, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(memObj, memAdd, Family.QUANTILES.getID());
    final int flags = EMPTY_FLAG_MASK; //empty
    insertFlags(memObj, memAdd, flags);
    insertK(memObj, memAdd, k);
    insertN(memObj, memAdd, 0L);
    insertMinDouble(memObj, memAdd, Double.POSITIVE_INFINITY);
    insertMaxDouble(memObj, memAdd, Double.NEGATIVE_INFINITY);
    //dstMem.clear(32, k << 1); //clear the base buffer only
    dds.mem_ = dstMem;
    dds.memObj_ = memObj;
    dds.memAdd_ = memAdd;
    return dds;
  }

  static DirectDoublesSketch wrapInstance(final Memory srcMem) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) { //initially require enough for the first long
      throw new SketchesArgumentException(
          "Destination Memory too small: " + memCapBytes + " < 8");
    }
    final Object memObj = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    //Extract the preamble first 8 bytes
    final int preLongs = extractPreLongs(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int familyID = extractFamilyID(memObj, memAdd);
    final int flags = extractFlags(memObj, memAdd);
    final int k = extractK(memObj, memAdd);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer);
    final boolean empty = Util.checkPreLongsFlagsCap(preLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);

    final boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);
    if (compact) {
      throw new SketchesArgumentException("Compact Memory is not supported for Direct.");
    }
    final DirectDoublesSketch dds = new DirectDoublesSketch(k);
    if (empty) { return dds; }

    //check if srcMem has required capacity given k, n, compact, memCapBytes
    final long n = extractN(memObj, memAdd);
    checkMemCapacity(k, n, compact, memCapBytes);

    return dds;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }
    final double maxValue = getMaxValue();
    final double minValue = getMinValue();

    if (dataItem > maxValue) { putMaxValue(dataItem); }
    if (dataItem < minValue) { putMinValue(dataItem); }

    final int curBBCount = getBaseBufferCount();
    final int newBBCount = curBBCount + 1;
    final long newN = getN() + 1;
    insertFlags(memObj_, memAdd_, 0); //not compact, not ordered, not empty

    if (newBBCount == 2 * k_) { //Propogate
      final int curCombBufCap = getCombinedBufferItemCapacity();

      // make sure there will be enough levels for the propagation
      final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(newN, k_);
      final double[] combinedBuffer;

      if (spaceNeeded > curCombBufCap) {
        // copies base buffer plus old levels, adds space for new level
        combinedBuffer = growCombinedBuffer(spaceNeeded);
      } else {
        combinedBuffer = getCombinedBuffer();
      }

      Arrays.sort(combinedBuffer, 0, k_ << 1); //sort only the BB portion, which is full

      final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
          0,               //starting level
          null,            //sizeKbuf,   not needed here
          0,               //sizeKStart, not needed here
          combinedBuffer,  //size2Kbuf, the base buffer = the Combined Buffer
          0,               //size2KStart
          true,            //doUpdateVersion
          k_,
          combinedBuffer,  //the base buffer = the Combined Buffer
          getBitPattern()  //current bitPattern prior to updating n
      );
      assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check

      putCombinedBuffer(combinedBuffer);
    }
    putN(newN);
  }

  @Override
  public int getK() {
    return extractK(memObj_, memAdd_);
  }

  @Override
  public long getN() {
    return extractN(memObj_, memAdd_);
  }

  @Override
  public boolean isEmpty() {
    return ((extractFlags(memObj_, memAdd_) & EMPTY_FLAG_MASK) > 0);
  }

  @Override
  public double getMinValue() {
    return extractMinDouble(memObj_, memAdd_);
  }

  @Override
  public double getMaxValue() {
    return extractMaxDouble(memObj_, memAdd_);
  }

  @Override
  public void reset() {
    insertN(memObj_, memAdd_, 0L);
    insertMinDouble(memObj_, memAdd_, Double.POSITIVE_INFINITY);
    insertMaxDouble(memObj_, memAdd_, Double.NEGATIVE_INFINITY);
    insertFlags(memObj_, memAdd_, EMPTY_FLAG_MASK); //not compact, not ordered
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return Util.computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return Util.computeCombinedBufferItemCapacity(getK(), getN(), false); //no partial BB allowed
  }

  @Override
  double[] getCombinedBuffer() {
    final int k = getK();
    if (isEmpty()) { return new double[k << 1]; }
    final long n = getN();
    final int itemCap = Util.computeCombinedBufferItemCapacity(k, n, false);
    final double[] combinedBuffer = new double[itemCap];
    mem_.getDoubleArray(32, combinedBuffer, 0, itemCap);
    return combinedBuffer;
  }

  @Override
  long getBitPattern() {
    final int k = getK();
    final long n = getN();
    return Util.computeBitPattern(k, n);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  //Puts

  @Override
  void putMinValue(final double minValue) {
    insertMinDouble(memObj_, memAdd_, minValue);
  }

  @Override
  void putMaxValue(final double maxValue) {
    insertMaxDouble(memObj_, memAdd_, maxValue);
  }

  @Override
  void putN(final long n) {
    insertN(memObj_, memAdd_, n);
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    mem_.putDoubleArray(32, combinedBuffer, 0, combinedBuffer.length);
  }

  @Override
  void putCombinedBufferItemCapacity(final int combBufItemCap) {
    //intentionally a no-op, not kept on-heap, always derived.
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    //intentionally a no-op, not kept on-heap, always derived.
  }

  @Override
  void putBitPattern(final long bitPattern) {
    //intentionally a no-op, not kept on-heap, always derived.
  }

  @Override
  double[] growCombinedBuffer(final int spaceNeeded) {
    final long memBytes = mem_.getCapacity();
    final int needBytes = spaceNeeded << 3;
    if ((needBytes) < memBytes) {
      throw new SketchesException("Insufficient Memory: mem: " + memBytes + ", need: " + needBytes);
    }
    return getCombinedBuffer();
  }

}
