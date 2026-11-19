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
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Proxy;
import java.util.Random;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.testng.annotations.Test;

/**
 * Checks that a ReqSketch stays consistent when an error interrupts an update.
 */
public class ReqUpdateFailureTest {
  private static final int K = 12;

  private static float value(final int i) { return (i * 7919) % 100_003; }

  /**
   * A compaction allocates the space in the next level before it changes anything. Here the merge into the
   * next level fails instead, because that buffer is not sorted. Both compactors must be left as they were.
   */
  @Test
  public void compactorUnchangedWhenPromotionFails() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqCompactor c = new ReqCompactor((byte) 0, hra, K, null);
      final FloatBuffer buf = c.getBuffer();
      for (int i = 0; i < c.getNomCapacity(); i++) { buf.append(value(i)); }
      buf.sort();
      final FloatBuffer next = new FloatBuffer(4, 4, hra);
      next.append(2f);
      next.append(1f); //not sorted
      final byte[] before = c.toByteArray();
      final int nextCount = next.getCount();
      final boolean coin = c.getCoin();
      try {
        c.compact(new ReqSketch.CompactorReturn(), new Random(1), next);
        fail();
      } catch (final SketchesArgumentException e) {
        //expected
      }
      assertEquals(c.toByteArray(), before);
      assertEquals(c.getCoin(), coin);
      assertEquals(next.getCount(), nextCount);
    }
  }

  /**
   * An error during compression leaves the item that started it in the sketch, with counts, bounds and
   * min and max that agree with the compactors, and the sketch keeps working.
   */
  @Test
  public void sketchConsistentWhenCompressionFails() {
    for (final boolean hra : new boolean[] {true, false}) {
      final int[] calls = {0};
      final ReqDebug failOnThirdLevel = (ReqDebug) Proxy.newProxyInstance(ReqDebug.class.getClassLoader(),
          new Class<?>[] {ReqDebug.class}, (proxy, method, args) -> {
            if (method.getName().equals("emitNewCompactor") && (++calls[0] == 3)) {
              throw new IllegalStateException("simulated failure");
            }
            return null;
          });
      final ReqSketch sk = ReqSketch.builder().setK(K).setHighRankAccuracy(hra).setReqDebug(failOnThirdLevel).build();
      int n = 0;
      boolean failed = false;
      while (!failed) {
        final float item = -n;
        try {
          sk.update(item);
        } catch (final IllegalStateException e) {
          failed = true;
        }
        n++;
        assertEquals(sk.getN(), n);
        assertEquals(sk.getMinItem(), item); //each item is the new min
        checkConsistent(sk);
      }
      for (int i = 0; i < 10_000; i++) { sk.update(value(i)); }
      checkConsistent(sk);
      final ReqSketch copy = ReqSketch.heapify(MemorySegment.ofArray(sk.toByteArray()));
      assertEquals(copy.toByteArray(), sk.toByteArray());
    }
  }

  private static void checkConsistent(final ReqSketch sk) {
    assertEquals(sk.getNumRetained(), sk.computeTotalRetainedItems());
    assertEquals(sk.getMaxNomSize(), sk.computeMaxNomSize());
    final QuantilesFloatsSketchIterator it = sk.iterator();
    long weight = 0;
    while (it.next()) { weight += it.getWeight(); }
    assertEquals(weight, sk.getN());
  }
}
