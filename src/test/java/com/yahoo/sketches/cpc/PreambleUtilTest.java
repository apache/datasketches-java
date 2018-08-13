/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.cpc.PreambleUtil.EMPTY_PREINTS;
import static com.yahoo.sketches.cpc.PreambleUtil.MERGED_FLAG_MASK;
import static com.yahoo.sketches.cpc.PreambleUtil.PINNED_SLIDING_HIP_PREINTS;
import static com.yahoo.sketches.cpc.PreambleUtil.PINNED_SLIDING_MERGED_PREINTS;
import static com.yahoo.sketches.cpc.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.cpc.PreambleUtil.SER_VER;
import static com.yahoo.sketches.cpc.PreambleUtil.SPARSE_HYBRID_HIP_PREINTS;
import static com.yahoo.sketches.cpc.PreambleUtil.SPARSE_HYBRID_MERGED_PREINTS;
import static com.yahoo.sketches.cpc.PreambleUtil.getCsvLength;
import static com.yahoo.sketches.cpc.PreambleUtil.getFamily;
import static com.yahoo.sketches.cpc.PreambleUtil.getFiCol;
import static com.yahoo.sketches.cpc.PreambleUtil.getFlags;
import static com.yahoo.sketches.cpc.PreambleUtil.getHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.getKxP;
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getMode;
import static com.yahoo.sketches.cpc.PreambleUtil.getNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.getPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getSerVer;
import static com.yahoo.sketches.cpc.PreambleUtil.getWinOffset;
import static com.yahoo.sketches.cpc.PreambleUtil.initEmpty;
import static com.yahoo.sketches.cpc.PreambleUtil.initPinnedSlidingHip;
import static com.yahoo.sketches.cpc.PreambleUtil.initPinnedSlidingMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.initSparseHybridHip;
import static com.yahoo.sketches.cpc.PreambleUtil.initSparseHybridMerged;
import static com.yahoo.sketches.cpc.PreambleUtil.putCsvLength;
import static com.yahoo.sketches.cpc.PreambleUtil.putCwLength;
import static com.yahoo.sketches.cpc.PreambleUtil.putFiCol;
import static com.yahoo.sketches.cpc.PreambleUtil.putHipAccum;
import static com.yahoo.sketches.cpc.PreambleUtil.putKxP;
import static com.yahoo.sketches.cpc.PreambleUtil.putNumCoupons;
import static com.yahoo.sketches.cpc.PreambleUtil.putWinOffset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkInits() {
    int maxPreInts = 9;
    WritableMemory wmem = WritableMemory.allocate(4 * maxPreInts);
    byte lgK = 12;

    initEmpty(wmem, lgK);
    assertEquals(getPreInts(wmem), EMPTY_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFlags(wmem), EMPTY_FLAG_MASK | READ_ONLY_FLAG_MASK);
    assertEquals(getMode(wmem), Mode.EMPTY);
    println(PreambleUtil.toString(wmem));

    initSparseHybridMerged(wmem, lgK, true); //hybrid
    assertEquals(getPreInts(wmem), SPARSE_HYBRID_MERGED_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFlags(wmem), MERGED_FLAG_MASK | READ_ONLY_FLAG_MASK);
    assertEquals(getMode(wmem), Mode.HYBRID_MERGED);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    println(PreambleUtil.toString(wmem));

    initSparseHybridMerged(wmem, lgK, false); //Sparse
    assertEquals(getPreInts(wmem), SPARSE_HYBRID_MERGED_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFlags(wmem), MERGED_FLAG_MASK | READ_ONLY_FLAG_MASK);
    assertEquals(getMode(wmem), Mode.SPARSE_MERGED);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    println(PreambleUtil.toString(wmem));

    initSparseHybridHip(wmem, lgK, true); //hybrid
    assertEquals(getPreInts(wmem), SPARSE_HYBRID_HIP_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFlags(wmem), READ_ONLY_FLAG_MASK);
    assertEquals(getMode(wmem), Mode.HYBRID_HIP);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    assertEquals(getKxP(wmem), (double) (1 << lgK));
    assertEquals(getHipAccum(wmem), 0.0);
    println(PreambleUtil.toString(wmem));

    initSparseHybridHip(wmem, lgK, false); //Sparse
    assertEquals(getPreInts(wmem), SPARSE_HYBRID_HIP_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFlags(wmem), READ_ONLY_FLAG_MASK);
    assertEquals(getMode(wmem), Mode.SPARSE_HIP);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    assertEquals(getKxP(wmem), (double) (1 << lgK));
    assertEquals(getHipAccum(wmem), 0.0);
    println(PreambleUtil.toString(wmem));


    initPinnedSlidingMerged(wmem, lgK, true); //sliding
    //check the normal puts
    putFiCol(wmem, (byte) 0);
    putWinOffset(wmem, (byte) 0);
    putNumCoupons(wmem, 0);
    putCsvLength(wmem, 0);
    putCwLength(wmem, 0);


    assertEquals(getPreInts(wmem), PINNED_SLIDING_MERGED_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), 0);
    assertEquals(getFlags(wmem), MERGED_FLAG_MASK | READ_ONLY_FLAG_MASK);
    assertEquals(getWinOffset(wmem), 0);
    assertEquals(getMode(wmem), Mode.SLIDING_MERGED);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    println(PreambleUtil.toString(wmem));

    initPinnedSlidingMerged(wmem, lgK, false); //pinned
    assertEquals(getPreInts(wmem), PINNED_SLIDING_MERGED_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), 0);
    assertEquals(getFlags(wmem), MERGED_FLAG_MASK | READ_ONLY_FLAG_MASK);
    assertEquals(getWinOffset(wmem), 0);
    assertEquals(getMode(wmem), Mode.PINNED_MERGED);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    println(PreambleUtil.toString(wmem));

    initPinnedSlidingHip(wmem, lgK, true); //sliding
    //check normal puts
    putKxP(wmem, 1 << lgK);
    putHipAccum(wmem, 0.0);
    putCwLength(wmem, 0);

    assertEquals(getPreInts(wmem), PINNED_SLIDING_HIP_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), 0);
    assertEquals(getFlags(wmem), READ_ONLY_FLAG_MASK);
    assertEquals(getWinOffset(wmem), 0);
    assertEquals(getMode(wmem), Mode.SLIDING_HIP);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    assertEquals(getKxP(wmem), (double) (1 << lgK));
    assertEquals(getHipAccum(wmem), 0.0);
    println(PreambleUtil.toString(wmem));

    initPinnedSlidingHip(wmem, lgK, false); //pinned
    assertEquals(getPreInts(wmem), PINNED_SLIDING_HIP_PREINTS);
    assertEquals(getSerVer(wmem), SER_VER);
    assertEquals(getFamily(wmem), Family.CPC);
    assertEquals(getLgK(wmem), lgK);
    assertEquals(getFiCol(wmem), 0);
    assertEquals(getFlags(wmem), READ_ONLY_FLAG_MASK);
    assertEquals(getWinOffset(wmem), 0);
    assertEquals(getMode(wmem), Mode.PINNED_HIP);
    assertEquals(getNumCoupons(wmem), 0);
    assertEquals(getCsvLength(wmem), 0);
    assertEquals(getKxP(wmem), (double) (1 << lgK));
    assertEquals(getHipAccum(wmem), 0.0);
    println(PreambleUtil.toString(wmem));

    byte[] byteArr = (byte[]) wmem.getArray();
    println("Duplicate");
    println(PreambleUtil.toString(byteArr));
  }

  @Test
  public void checkErrorModes() {
    int maxPreInts = 9;
    WritableMemory wmem = WritableMemory.allocate(4 * maxPreInts);
    byte lgK = 12;
    initEmpty(wmem, lgK);
    try { putNumCoupons(wmem, 1); fail(); } catch (SketchesArgumentException e) { }
    try { putCsvLength(wmem, 1); fail(); } catch (SketchesArgumentException e) { }
    try { putCwLength(wmem, 1); fail(); } catch (SketchesArgumentException e) { }
    try { putKxP(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }
    try { putHipAccum(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }

    initSparseHybridMerged(wmem, lgK, true); //hybrid
    try { putCwLength(wmem, 1); fail(); } catch (SketchesArgumentException e) { }
    try { putKxP(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }
    try { putHipAccum(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }

    initSparseHybridHip(wmem, lgK, true); //hybrid
    try { putCwLength(wmem, 1); fail(); } catch (SketchesArgumentException e) { }

    initPinnedSlidingMerged(wmem, lgK, true); //sliding
    try { putKxP(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }
    try { putHipAccum(wmem, 1.0); fail(); } catch (SketchesArgumentException e) { }
  }






  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }

}
