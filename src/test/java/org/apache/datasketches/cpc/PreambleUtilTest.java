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

package org.apache.datasketches.cpc;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.cpc.PreambleUtil.COMPRESSED_FLAG_MASK;
import static org.apache.datasketches.cpc.PreambleUtil.SER_VER;
import static org.apache.datasketches.cpc.PreambleUtil.getDefinedPreInts;
import static org.apache.datasketches.cpc.PreambleUtil.getFamily;
import static org.apache.datasketches.cpc.PreambleUtil.getFiCol;
import static org.apache.datasketches.cpc.PreambleUtil.getFlags;
import static org.apache.datasketches.cpc.PreambleUtil.getFormat;
import static org.apache.datasketches.cpc.PreambleUtil.getHipAccum;
import static org.apache.datasketches.cpc.PreambleUtil.getKxP;
import static org.apache.datasketches.cpc.PreambleUtil.getLgK;
import static org.apache.datasketches.cpc.PreambleUtil.getNumCoupons;
import static org.apache.datasketches.cpc.PreambleUtil.getNumSv;
import static org.apache.datasketches.cpc.PreambleUtil.getPreInts;
import static org.apache.datasketches.cpc.PreambleUtil.getSeedHash;
import static org.apache.datasketches.cpc.PreambleUtil.getSerVer;
import static org.apache.datasketches.cpc.PreambleUtil.getSvLengthInts;
import static org.apache.datasketches.cpc.PreambleUtil.getSvStreamOffset;
import static org.apache.datasketches.cpc.PreambleUtil.getWLengthInts;
import static org.apache.datasketches.cpc.PreambleUtil.getWStreamOffset;
import static org.apache.datasketches.cpc.PreambleUtil.hasHip;
import static org.apache.datasketches.cpc.PreambleUtil.putEmptyMerged;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingHip;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingHipNoSv;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingMerged;
import static org.apache.datasketches.cpc.PreambleUtil.putPinnedSlidingMergedNoSv;
import static org.apache.datasketches.cpc.PreambleUtil.putSparseHybridHip;
import static org.apache.datasketches.cpc.PreambleUtil.putSparseHybridMerged;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.testng.annotations.Test;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.cpc.PreambleUtil.HiField;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {
  static final short defaultSeedHash = Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED) ;

  private static void checkFirst8(final MemorySegment wseg, final Format format, final int lgK, final int fiCol) {
    assertEquals(getFormat(wseg), format);
    assertEquals(getPreInts(wseg), getDefinedPreInts(format));
    assertEquals(getSerVer(wseg), SER_VER);
    assertEquals(getFamily(wseg), Family.CPC);
    assertEquals(getLgK(wseg), lgK);
    assertEquals(getFiCol(wseg), fiCol);
    assertEquals(getFlags(wseg), (format.ordinal() << 2) | COMPRESSED_FLAG_MASK);
    assertEquals(getSeedHash(wseg), defaultSeedHash);
  }

  @Test
  public void checkNormalPutSegment() {
    final int lgK = 12;
    final double kxp = lgK;
    final double hipAccum = 1005;
    final int fiCol = 1;
    final int[] csvStream = {1, 2, 3};
    final int numCoupons = csvStream.length;
    final int csvLength = csvStream.length;
    final short seedHash = defaultSeedHash;
    final int[] cwStream = {4, 5, 6};
    final int cwLength = cwStream.length;
    final int numSv = cwStream.length;
    final int maxInts = 10 + csvLength + cwLength;
    final MemorySegment wseg = MemorySegment.ofArray(new byte[4 * maxInts]);

    Format format;

    format = Format.EMPTY_MERGED;
    putEmptyMerged(wseg, lgK, seedHash);
    println(CpcSketch.toString((byte[])wseg.toArray(JAVA_BYTE), true));
    checkFirst8(wseg, format, lgK, (byte) 0);
    assertFalse(hasHip(wseg));

    format = Format.SPARSE_HYBRID_MERGED;
    putSparseHybridMerged(wseg, lgK, numCoupons, csvLength, seedHash, csvStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getSvLengthInts(wseg), csvLength);

    format = Format.SPARSE_HYBRID_HIP;
    putSparseHybridHip(wseg, lgK, numCoupons, csvLength, kxp, hipAccum, seedHash, csvStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, (byte) 0);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getSvLengthInts(wseg), csvLength);
    assertEquals(getKxP(wseg), kxp);
    assertEquals(getHipAccum(wseg), hipAccum);
    assertTrue(hasHip(wseg));

    format = Format.PINNED_SLIDING_MERGED_NOSV;
    putPinnedSlidingMergedNoSv(wseg, lgK, fiCol, numCoupons, cwLength, seedHash, cwStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, fiCol);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getWLengthInts(wseg), cwLength);

    format = Format.PINNED_SLIDING_HIP_NOSV;
    putPinnedSlidingHipNoSv(wseg, lgK, fiCol, numCoupons, cwLength, kxp, hipAccum, seedHash,
        cwStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, fiCol);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getWLengthInts(wseg), cwLength);
    assertEquals(getKxP(wseg), kxp);
    assertEquals(getHipAccum(wseg), hipAccum);

    format = Format.PINNED_SLIDING_MERGED;
    putPinnedSlidingMerged(wseg, lgK, fiCol, numCoupons, numSv, csvLength, cwLength, seedHash,
        csvStream, cwStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, fiCol);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getNumSv(wseg), numSv);
    assertEquals(getSvLengthInts(wseg), csvLength);
    assertEquals(getWLengthInts(wseg), cwLength);

    format = Format.PINNED_SLIDING_HIP;
    putPinnedSlidingHip(wseg, lgK, fiCol, numCoupons, numSv, kxp, hipAccum, csvLength, cwLength,
        seedHash, csvStream, cwStream);
    println(CpcSketch.toString(wseg, true));
    println(CpcSketch.toString(wseg, false));
    checkFirst8(wseg, format, lgK, fiCol);
    assertEquals(getNumCoupons(wseg), numCoupons);
    assertEquals(getNumSv(wseg), numSv);
    assertEquals(getSvLengthInts(wseg), csvLength);
    assertEquals(getWLengthInts(wseg), cwLength);
    assertEquals(getKxP(wseg), kxp);
    assertEquals(getHipAccum(wseg), hipAccum);
  }

  @Test
  public void checkStreamErrors() {
    final MemorySegment wseg = MemorySegment.ofArray(new byte[4 * 10]);
    putEmptyMerged(wseg, (byte) 12, defaultSeedHash);
    try { getSvStreamOffset(wseg); fail(); } catch (final SketchesArgumentException e) { }
    wseg.set(JAVA_BYTE, 5, (byte) (7 << 2));
    try { getSvStreamOffset(wseg); fail(); } catch (final SketchesStateException e) { }
    wseg.set(JAVA_BYTE, 5, (byte) 0);
    try { getWStreamOffset(wseg); fail(); } catch (final SketchesArgumentException e) { }
    wseg.set(JAVA_BYTE, 5, (byte) (7 << 2));
    try { getWStreamOffset(wseg); fail(); } catch (final SketchesStateException e) { }
  }

  @Test
  public void checkStreamErrors2() {
    final MemorySegment wseg = MemorySegment.ofArray(new byte[4 * 10]);
    final int[] svStream = { 1 };
    final int[] wStream = { 2 };
    try {
      putPinnedSlidingMerged(wseg, 4, 0, 1, 1, 1, 0, (short) 0, svStream, wStream);
      fail();
    } catch (final SketchesStateException e) { }
    assertTrue(PreambleUtil.isCompressed(wseg));
  }

  @Test
  public void checkEmptySegment() {
    final MemorySegment wseg = MemorySegment.ofArray(new byte[4 * 10]);
    wseg.set(JAVA_BYTE, 2, (byte) 16); //legal Family
    wseg.set(JAVA_BYTE, 5, (byte) (1 << 2)); //select NONE
    println(CpcSketch.toString(wseg, false));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkFieldError() {
    PreambleUtil.fieldError(Format.EMPTY_MERGED, HiField.NUM_COUPONS);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCapacity() {
    PreambleUtil.checkCapacity(100, 101);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkHiFieldError() {
    PreambleUtil.getHiFieldOffset(Format.EMPTY_MERGED, HiField.NUM_COUPONS);
  }

  @Test
  public void checkWindowOffset() {
    final long offset = CpcUtil.determineCorrectOffset(4, 54);
    assertEquals(offset, 1L);
  }

  @Test
  public void checkFormatEnum() {
    assertEquals(Format.EMPTY_MERGED, Format.ordinalToFormat(0));
    assertEquals(Format.EMPTY_HIP, Format.ordinalToFormat(1));
    assertEquals(Format.SPARSE_HYBRID_MERGED, Format.ordinalToFormat(2));
    assertEquals(Format.SPARSE_HYBRID_HIP, Format.ordinalToFormat(3));
    assertEquals(Format.PINNED_SLIDING_MERGED_NOSV, Format.ordinalToFormat(4));
    assertEquals(Format.PINNED_SLIDING_HIP_NOSV, Format.ordinalToFormat(5));
    assertEquals(Format.PINNED_SLIDING_MERGED, Format.ordinalToFormat(6));
    assertEquals(Format.PINNED_SLIDING_HIP, Format.ordinalToFormat(7));
  }

  @Test
  public void checkFlavorEnum() {
    assertEquals(Flavor.EMPTY, Flavor.ordinalToFlavor(0));
    assertEquals(Flavor.SPARSE, Flavor.ordinalToFlavor(1));
    assertEquals(Flavor.HYBRID, Flavor.ordinalToFlavor(2));
    assertEquals(Flavor.PINNED, Flavor.ordinalToFlavor(3));
    assertEquals(Flavor.SLIDING, Flavor.ordinalToFlavor(4));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
