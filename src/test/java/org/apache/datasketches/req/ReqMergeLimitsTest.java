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
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;
import org.testng.annotations.Test;

/**
 * Checks the limit on N, and that a merge interrupted partway leaves N consistent with the retained items.
 */
public class ReqMergeLimitsTest {
  private static final int K = 12;

  private static ReqSketch newSketch(final boolean hra, final int n, final int offset) {
    final ReqSketch sk = ReqSketch.builder().setK(K).setHighRankAccuracy(hra).build();
    for (int i = 0; i < n; i++) { sk.update(i + offset); }
    return sk;
  }

  //the same compactors and items as sk, but with the given N
  private static ReqSketch withN(final ReqSketch sk, final long n) {
    return new ReqSketch(sk.getK(), sk.getHighRankAccuracyMode(), n, sk.getMinItem(), sk.getMaxItem(),
        sk.getCompactors());
  }

  @Test
  public void mergeRejectsNAboveMax() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch other = newSketch(hra, 100, 0);
      final ReqSketch sk = withN(newSketch(hra, 1000, 0), Long.MAX_VALUE - 99);
      final byte[] before = sk.toByteArray();
      try {
        sk.merge(other);
        fail();
      } catch (final SketchesArgumentException e) {
        //expected
      }
      assertEquals(sk.toByteArray(), before);
    }
  }

  @Test
  public void updateRejectsNAboveMax() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch sk = withN(newSketch(hra, 1000, 0), Long.MAX_VALUE - 100);
      sk.merge(newSketch(hra, 100, 0)); //reaches Long.MAX_VALUE exactly
      assertEquals(sk.getN(), Long.MAX_VALUE);
      final byte[] before = sk.toByteArray();
      try {
        sk.update(1f);
        fail();
      } catch (final SketchesStateException e) {
        //expected
      }
      assertEquals(sk.toByteArray(), before);
      sk.update(Float.NaN); //ignored, so allowed
    }
  }

  /**
   * The other sketch's third compactor has the wrong lgWeight, so the merge fails after merging the
   * first two levels. N must then count exactly the items that were merged.
   */
  @Test
  public void partialMergeKeepsNConsistent() {
    for (final boolean hra : new boolean[] {true, false}) {
      final ReqSketch sk = newSketch(hra, 500, 0);
      final long startN = sk.getN();
      final List<ReqCompactor> otherComps = new ArrayList<>();
      final int[] lgWeights = {0, 1, 5};
      long otherN = 0;
      for (int h = 0; h < lgWeights.length; h++) {
        final ReqCompactor c = new ReqCompactor((byte) lgWeights[h], hra, K, null);
        for (int i = 0; i < 10; i++) { c.getBuffer().append(-1000 - (h * 10) - i); }
        c.getBuffer().sort();
        otherComps.add(c);
        otherN += 10L << lgWeights[h];
      }
      final ReqSketch other = new ReqSketch(K, hra, otherN, -1029f, -1000f, otherComps);
      try {
        sk.merge(other);
        fail();
      } catch (final AssertionError e) {
        //expected: compactor lgWeights differ
      }
      assertEquals(sk.getN(), startN + 10 + 20); //levels 0 and 1 were merged
      assertEquals(sk.getMinItem(), -1029f); //other's min, since some of its items were merged
      assertEquals(sk.getMaxItem(), 499f);
      checkConsistent(sk);
      for (int i = 0; i < 10_000; i++) { sk.update(i); }
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
