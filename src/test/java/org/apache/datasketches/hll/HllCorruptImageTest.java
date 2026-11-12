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

package org.apache.datasketches.hll;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.function.Function;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt HLL images are rejected without large allocations.
 */
public class HllCorruptImageTest {
  private static final int LG_K_BYTE = 3;
  private static final int LG_ARR_BYTE = 4;
  private static final int LIST_COUNT_BYTE = 6;
  private static final int HASH_SET_COUNT_INT = 8;
  private static final int AUX_COUNT_INT = 36;
  private static final int SET_ARR_START = 12;

  private static HllSketch sketch(final TgtHllType type, final int n) {
    final HllSketch sk = new HllSketch(12, type);
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  private static void checkRejected(final byte[] bytes, final Function<MemorySegment, Object> deser) {
    final long allocated = AllocationTracker.allocatedBytes(() -> deser.apply(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> deser.apply(MemorySegment.ofArray(bytes)));
  }

  private static void checkAllPaths(final byte[] bytes) {
    checkRejected(bytes, HllSketch::heapify);
    checkRejected(bytes, HllSketch::wrap);
    checkRejected(bytes, HllSketch::writableWrap);
    checkRejected(bytes, HllUnion::heapify);
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final TgtHllType type : TgtHllType.values()) {
      for (final int n : new int[] {0, 5, 200, 10_000}) {
        final HllSketch sk = sketch(type, n);
        for (final byte[] b : new byte[][] {sk.toCompactByteArray(), sk.toUpdatableByteArray()}) {
          assertEquals(HllSketch.heapify(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
          assertEquals(HllSketch.wrap(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
        }
      }
    }
  }

  @Test
  public void checkLgKTooLarge() {
    for (final TgtHllType type : TgtHllType.values()) {
      for (final int n : new int[] {5, 200, 10_000}) { //list, set and HLL modes
        final byte[] bytes = sketch(type, n).toUpdatableByteArray();
        bytes[LG_K_BYTE] = 30;
        checkAllPaths(bytes);
      }
    }
  }

  @Test
  public void checkListCount() {
    final byte[] bytes = sketch(TgtHllType.HLL_8, 5).toUpdatableByteArray();
    bytes[LIST_COUNT_BYTE] = (byte) 200;
    checkAllPaths(bytes);
  }

  @Test
  public void checkSetSizes() {
    final byte[] lgArr = sketch(TgtHllType.HLL_8, 200).toUpdatableByteArray();
    lgArr[LG_ARR_BYTE] = 28;
    checkAllPaths(lgArr);
    final byte[] count = sketch(TgtHllType.HLL_8, 200).toCompactByteArray();
    ByteArrayUtil.putIntLE(count, HASH_SET_COUNT_INT, 1 << 28);
    checkAllPaths(count);
  }

  @Test
  public void checkAuxSizes() {
    final byte[] bytes = sketch(TgtHllType.HLL_4, 10_000).toUpdatableByteArray();
    ByteArrayUtil.putIntLE(bytes, AUX_COUNT_INT, 1 << 28);
    checkAllPaths(bytes);
    final byte[] lgArr = sketch(TgtHllType.HLL_4, 10_000).toUpdatableByteArray();
    if (ByteArrayUtil.getIntLE(lgArr, AUX_COUNT_INT) == 0) { ByteArrayUtil.putIntLE(lgArr, AUX_COUNT_INT, 1); }
    lgArr[LG_ARR_BYTE] = 28;
    checkAllPaths(lgArr);
  }

  //lgK 12: the set table grows to at most 2^9 ints and holds at most 3/4 of that, 384 coupons
  private static final int MAX_SET_LG_ARR = 9;
  private static final int MAX_SET_COUNT = 384;

  private static byte[] setImage(final boolean compact) {
    final HllSketch sk = sketch(TgtHllType.HLL_8, 200);
    assertEquals(sk.getCurMode(), CurMode.SET);
    return compact ? sk.toCompactByteArray() : sk.toUpdatableByteArray();
  }

  @Test
  public void checkValidSetImagesAcrossLgK() {
    for (int lgK = 8; lgK <= 14; lgK++) {
      final int maxCount = (3 << (lgK - 3)) / 4;
      for (final int n : new int[] {9, maxCount / 2, maxCount}) {
        final HllSketch heap = new HllSketch(lgK, TgtHllType.HLL_8);
        final MemorySegment wseg = MemorySegment.ofArray(
            new byte[HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_8)]);
        final HllSketch direct = new HllSketch(lgK, TgtHllType.HLL_8, wseg);
        for (int i = 0; i < n; i++) { heap.update(i); direct.update(i); }
        for (final HllSketch sk : new HllSketch[] {heap, direct}) {
          if (sk.getCurMode() != CurMode.SET) { continue; } //a few hash collisions can leave it a list
          for (final byte[] b : new byte[][] {sk.toCompactByteArray(), sk.toUpdatableByteArray()}) {
            assertEquals(HllSketch.heapify(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
            assertEquals(HllSketch.wrap(MemorySegment.ofArray(b)).getEstimate(), sk.getEstimate());
          }
          final byte[] upd = sk.toUpdatableByteArray();
          final byte[] full = new byte[HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_8)];
          System.arraycopy(upd, 0, full, 0, upd.length);
          assertEquals(HllSketch.writableWrap(MemorySegment.ofArray(full)).getEstimate(), sk.getEstimate());
        }
      }
    }
  }

  @Test
  public void checkCompactSetCountAboveLimit() {
    final byte[] image = setImage(true);
    final byte[] bytes = new byte[image.length + (4 * MAX_SET_COUNT)]; //room for the declared coupons
    System.arraycopy(image, 0, bytes, 0, image.length);
    ByteArrayUtil.putIntLE(bytes, HASH_SET_COUNT_INT, MAX_SET_COUNT + 1);
    checkAllPaths(bytes);
  }

  @Test
  public void checkCompactSetTruncated() {
    final byte[] image = setImage(true);
    final byte[] bytes = new byte[image.length - 4];
    System.arraycopy(image, 0, bytes, 0, bytes.length);
    checkAllPaths(bytes);
  }

  @Test
  public void checkUpdatableSetLgArrRange() {
    for (final int lgArr : new int[] {MAX_SET_LG_ARR + 1, 12}) {
      final byte[] image = setImage(false);
      final byte[] bytes = new byte[image.length + (4 << lgArr)]; //room for the declared table
      System.arraycopy(image, 0, bytes, 0, image.length);
      bytes[LG_ARR_BYTE] = (byte) lgArr;
      checkAllPaths(bytes);
    }
  }

  @Test
  public void checkUpdatableSetCountMatchesSlots() {
    for (final int delta : new int[] {-1, 1, 100}) {
      final byte[] bytes = setImage(false);
      ByteArrayUtil.putIntLE(bytes, HASH_SET_COUNT_INT, ByteArrayUtil.getIntLE(bytes, HASH_SET_COUNT_INT) + delta);
      checkAllPaths(bytes);
    }
  }

  @Test
  public void checkUpdatableSetOverfull() {
    //every slot filled and the count to match: valid writers grow or promote long before this
    final byte[] bytes = setImage(false);
    final int arrInts = 1 << bytes[LG_ARR_BYTE];
    for (int i = 0; i < arrInts; i++) { ByteArrayUtil.putIntLE(bytes, SET_ARR_START + (4 * i), i + 1); }
    ByteArrayUtil.putIntLE(bytes, HASH_SET_COUNT_INT, arrInts);
    checkAllPaths(bytes);
  }

  @Test
  public void checkSetModeNeedsLgK8() {
    final HllSketch sk = new HllSketch(8, TgtHllType.HLL_8);
    for (int i = 0; i < 20; i++) { sk.update(i); }
    assertEquals(sk.getCurMode(), CurMode.SET); //table of 2^5 ints, the only size lgK 8 allows
    for (final boolean compact : new boolean[] {true, false}) {
      final byte[] bytes = compact ? sk.toCompactByteArray() : sk.toUpdatableByteArray();
      bytes[LG_K_BYTE] = 7;
      checkAllPaths(bytes);
    }
  }
}
