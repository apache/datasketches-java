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


package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that KLL images whose n is negative, or less than the number of retained items, are rejected,
 * and that a non-empty image that retains no items is rejected.
 */
public class KllCorruptNTest {
  private static final int K = 20;

  private static byte[] withN(final byte[] image, final long n) {
    final byte[] bytes = image.clone();
    ByteArrayUtil.putLongLE(bytes, N_LONG_ADR, n);
    return bytes;
  }

  private static void checkNValues(final byte[] image, final int retained,
      final Consumer<MemorySegment> heapify) {
    heapify.accept(MemorySegment.ofArray(withN(image, retained))); //the least valid n
    for (final long n : new long[] {retained - 1L, -1L, Long.MIN_VALUE}) {
      final MemorySegment seg = MemorySegment.ofArray(withN(image, n));
      expectThrows(SketchesArgumentException.class, () -> heapify.accept(seg));
    }
  }

  @Test
  public void doublesRejectsBadN() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    final int retained = sk.getNumRetained();
    checkNValues(sk.toByteArray(), retained, KllDoublesSketch::heapify);
    checkNValues(sk.toByteArray(), retained, KllDoublesSketch::wrap);
    checkNValues(KllHelper.toByteArray(sk, true), retained, KllDoublesSketch::heapify);
    checkNValues(KllHelper.toByteArray(sk, true), retained, KllDoublesSketch::wrap);
    assertEquals(KllDoublesSketch.heapify(MemorySegment.ofArray(sk.toByteArray())).getN(), 1000);
  }

  @Test
  public void floatsAndLongsRejectBadN() {
    final KllFloatsSketch fsk = KllFloatsSketch.newHeapInstance(K);
    final KllLongsSketch lsk = KllLongsSketch.newHeapInstance(K);
    for (int i = 0; i < 1000; i++) { fsk.update(i); lsk.update(i); }
    checkNValues(fsk.toByteArray(), fsk.getNumRetained(), KllFloatsSketch::heapify);
    checkNValues(KllHelper.toByteArray(fsk, true), fsk.getNumRetained(), KllFloatsSketch::wrap);
    checkNValues(lsk.toByteArray(), lsk.getNumRetained(), KllLongsSketch::heapify);
    checkNValues(KllHelper.toByteArray(lsk, true), lsk.getNumRetained(), KllLongsSketch::wrap);
  }

  @Test
  public void itemsRejectsBadN() {
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final KllItemsSketch<String> sk = KllItemsSketch.newHeapInstance(K, String::compareTo, serDe);
    for (int i = 0; i < 1000; i++) { sk.update(Integer.toString(i)); }
    checkNValues(sk.toByteArray(), sk.getNumRetained(),
        seg -> KllItemsSketch.heapify(seg, String::compareTo, serDe));
    checkNValues(sk.toByteArray(), sk.getNumRetained(),
        seg -> KllItemsSketch.wrap(seg, String::compareTo, serDe));
  }

  //Sets levelsArr[0] of a single-level image to the capacity, so the image retains no items.
  private static byte[] withNoItems(final byte[] image) {
    final byte[] bytes = image.clone();
    ByteArrayUtil.putIntLE(bytes, DATA_START_ADR, K); //one level: capacity = k
    return bytes;
  }

  private static void checkNoItemsRejected(final byte[] image, final Consumer<MemorySegment> heapify) {
    heapify.accept(MemorySegment.ofArray(image)); //the unchanged image is valid
    final MemorySegment seg = MemorySegment.ofArray(withNoItems(image));
    final SketchesArgumentException e = expectThrows(SketchesArgumentException.class, () -> heapify.accept(seg));
    assertTrue(e.getMessage().startsWith(KllMemorySegmentValidate.NOT_EMPTY_NO_ITEMS));
  }

  @Test
  public void rejectsNonEmptyWithNoItems() {
    final int n = 10; //fewer than k: one level
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    final KllDoublesSketch dsk = KllDoublesSketch.newHeapInstance(K);
    final KllFloatsSketch fsk = KllFloatsSketch.newHeapInstance(K);
    final KllLongsSketch lsk = KllLongsSketch.newHeapInstance(K);
    final KllItemsSketch<String> isk = KllItemsSketch.newHeapInstance(K, String::compareTo, serDe);
    for (int i = 0; i < n; i++) { dsk.update(i); fsk.update(i); lsk.update(i); isk.update(Integer.toString(i)); }
    assertEquals(dsk.getNumLevels(), 1);
    for (final boolean updatable : new boolean[] {false, true}) {
      checkNoItemsRejected(KllHelper.toByteArray(dsk, updatable), KllDoublesSketch::heapify);
      checkNoItemsRejected(KllHelper.toByteArray(dsk, updatable), KllDoublesSketch::wrap);
      checkNoItemsRejected(KllHelper.toByteArray(fsk, updatable), KllFloatsSketch::heapify);
      checkNoItemsRejected(KllHelper.toByteArray(lsk, updatable), KllLongsSketch::wrap);
    }
    checkNoItemsRejected(isk.toByteArray(), seg -> KllItemsSketch.heapify(seg, String::compareTo, serDe));
    checkNoItemsRejected(isk.toByteArray(), seg -> KllItemsSketch.wrap(seg, String::compareTo, serDe));
  }

  //A compact full image with n = 0 and no items is not empty by its layout, so it is rejected too.
  @Test
  public void rejectsCompactFullWithZeroNAndNoItems() {
    final KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(K);
    for (int i = 0; i < 10; i++) { sk.update(i); }
    final MemorySegment seg = MemorySegment.ofArray(withN(withNoItems(sk.toByteArray()), 0));
    expectThrows(SketchesArgumentException.class, () -> KllDoublesSketch.heapify(seg));
  }

  //An empty updatable image has n = 0 and no items, and stays valid.
  @Test
  public void acceptsEmptyUpdatable() {
    final byte[] image = KllHelper.toByteArray(KllDoublesSketch.newHeapInstance(K), true);
    assertTrue(KllDoublesSketch.heapify(MemorySegment.ofArray(image)).isEmpty());
    assertTrue(KllDoublesSketch.wrap(MemorySegment.ofArray(image)).isEmpty());
    final byte[] fimage = KllHelper.toByteArray(KllFloatsSketch.newHeapInstance(K), true);
    assertTrue(KllFloatsSketch.wrap(MemorySegment.ofArray(fimage)).isEmpty());
  }
}
