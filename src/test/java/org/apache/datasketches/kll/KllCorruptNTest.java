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

import static org.apache.datasketches.kll.KllPreambleUtil.N_LONG_ADR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that KLL images whose n is negative, or less than the number of retained items, are rejected.
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
}
