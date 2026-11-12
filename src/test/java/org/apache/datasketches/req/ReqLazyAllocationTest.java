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


package org.apache.datasketches.req;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.testng.annotations.Test;

/**
 * Checks that a ReqSketch allocates the storage of its first compactor on first use, or when
 * ensureCapacity() is called, with the same results either way.
 */
public class ReqLazyAllocationTest {
  private static final int K = 12;

  // A silent ReqDebug: with one, the sketch uses a fixed random seed, so two sketches can be compared byte for byte.
  private static final ReqDebug QUIET = (ReqDebug) Proxy.newProxyInstance(
      ReqDebug.class.getClassLoader(), new Class<?>[] {ReqDebug.class}, (proxy, method, args) -> null);

  private static ReqSketch newSketch(final boolean hra) {
    return ReqSketch.builder().setK(K).setHighRankAccuracy(hra).setReqDebug(QUIET).build();
  }

  private static float value(final int i) { return (i * 7919) % 100_003; }

  private static void checkIterator(final ReqSketch sk) {
    final QuantilesFloatsSketchIterator it = sk.iterator();
    long weight = 0;
    int count = 0;
    while (it.next()) {
      it.getQuantile();
      weight += it.getWeight();
      count++;
    }
    assertFalse(it.next()); //stays exhausted
    assertEquals(count, sk.getNumRetained());
    assertEquals(weight, sk.getN());
  }

  @Test
  public void emptySketchHoldsNoStorage() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch sk = newSketch(hra);
      assertFalse(sk.hasItemStorage());
      sk.update(Float.NaN); //ignored, so not a first use
      sk.merge(newSketch(hra)); //merging an empty sketch is not a first use
      sk.merge(null);
      assertTrue(sk.isEmpty());
      checkIterator(sk);
      sk.toString();
      sk.viewCompactorDetail("%4.0f", true);
      final ReqSketch reserved = newSketch(hra);
      reserved.ensureCapacity();
      assertEquals(sk.toByteArray(), reserved.toByteArray());
      assertFalse(sk.hasItemStorage());
      final ReqSketch copy = ReqSketch.heapify(MemorySegment.ofArray(sk.toByteArray()));
      assertFalse(copy.hasItemStorage());
      assertTrue(copy.isEmpty());
    }
  }

  @Test
  public void ensureCapacity() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch sk = newSketch(hra);
      sk.ensureCapacity();
      assertTrue(sk.hasItemStorage());
      final ReqCompactor c0 = sk.getCompactors().get(0);
      final FloatBuffer storage = c0.getBuffer();
      assertEquals(storage.getCapacity(), 2 * c0.getNomCapacity()); //the size a new sketch used to allocate
      sk.ensureCapacity(); //no effect once allocated
      assertEquals(storage.getCapacity(), 2 * c0.getNomCapacity());

      final ReqSketch lazy = newSketch(hra);
      for (int i = 0; i < 100_000; i++) { sk.update(value(i)); lazy.update(value(i)); }
      assertEquals(sk.toByteArray(), lazy.toByteArray());

      final byte[] image = sk.toByteArray();
      sk.ensureCapacity(); //no effect after updates
      assertEquals(sk.toByteArray(), image);
    }
  }

  @Test
  public void firstUseAllocates() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch updated = newSketch(hra);
      updated.update(1);
      assertTrue(updated.hasItemStorage());
      assertEquals(updated.getMinItem(), 1f);

      final ReqSketch other = newSketch(hra);
      for (int i = 0; i < 10_000; i++) { other.update(value(i)); }
      final ReqSketch lazy = newSketch(hra);
      final ReqSketch reserved = newSketch(hra);
      reserved.ensureCapacity();
      lazy.merge(other);
      reserved.merge(other);
      assertTrue(lazy.hasItemStorage());
      assertEquals(lazy.toByteArray(), reserved.toByteArray());
      assertEquals(lazy.getN(), other.getN());
      assertEquals(lazy.getMinItem(), other.getMinItem());
      assertEquals(lazy.getMaxItem(), other.getMaxItem());
      checkIterator(lazy);
    }
  }

  @Test
  public void resetReleasesStorage() {
    final ReqSketch sk = newSketch(true);
    for (int i = 0; i < 10_000; i++) { sk.update(value(i)); }
    sk.reset();
    assertFalse(sk.hasItemStorage());
    assertTrue(sk.isEmpty());
    checkIterator(sk);
    sk.update(5);
    assertTrue(sk.hasItemStorage());
    assertEquals(sk.getN(), 1);
    assertEquals(sk.getMaxItem(), 5f);
  }

  @Test
  public void iteratorCoversRetainedItems() {
    for (final boolean hra : new boolean[] {true, false}) {
      for (final int n : new int[] {0, 1, 23, 24, 25, 100, 1000, 10_000}) {
        final ReqSketch sk = newSketch(hra);
        for (int i = 0; i < n; i++) { sk.update(value(i)); }
        checkIterator(sk);
        final ReqSketch merged = newSketch(hra);
        merged.merge(sk);
        merged.merge(sk);
        checkIterator(merged);
      }
    }
  }

  @Test
  public void iteratorSkipsLevelsWithoutItems() {
    //levels 0 and 2 hold items, levels 1 and 3 hold none
    final boolean hra = true;
    final List<ReqCompactor> compactors = new ArrayList<>();
    final float[][] items = { {1, 2, 3}, {}, {4, 5}, {} };
    long n = 0;
    for (int h = 0; h < items.length; h++) {
      final FloatBuffer buf = FloatBuffer.reconstruct(items[h], items[h].length, 64, 32, true, hra);
      compactors.add(new ReqCompactor((byte) h, hra, 0, K, (byte) 3, buf));
      n += (long) items[h].length << h;
    }
    final ReqSketch sk = new ReqSketch(K, hra, n, 1, 5, compactors);
    sk.setMaxNomSize(sk.computeMaxNomSize());
    sk.setRetainedItems(sk.computeTotalRetainedItems());
    checkIterator(sk);
    assertEquals(sk.getN(), 3 + (2 << 2));
  }
}
