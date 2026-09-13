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

import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.extractCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractHipAccum;
import static org.apache.datasketches.hll.PreambleUtil.extractNumAtCurMin;
import static org.apache.datasketches.hll.PreambleUtil.extractOooFlag;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.testng.annotations.Test;

/**
 * The union defers rebuilding curMin, numAtCurMin and the KxQ registers after a merge. These
 * tests pin the two properties that deferral must not break: the rebuilt state has to be the
 * same state the incremental update path maintains, and when the rebuild happens must not be
 * observable in the result.
 */
public class HllKxqRebuildTest {

  private static HllSketch build(final int lgK, final TgtHllType type, final long lo, final long hi) {
    final HllSketch sk = new HllSketch(lgK, type);
    for (long i = lo; i < hi; i++) { sk.update(i); }
    return sk;
  }

  @Test
  public void checkResultIsMergeOrderIndependent() {
    //c stays in SET mode, exercising the coupon-update path into a gadget with a rebuild pending
    final HllSketch a = build(12, HLL_4, 20000, 30364);
    final HllSketch b = build(10, HLL_8, 5000, 14699);
    final HllSketch c = build(17, HLL_4, 70000, 72598);
    final HllSketch[] in = {a, b, c};

    final int[][] orders = {{0,1,2},{0,2,1},{1,0,2},{1,2,0},{2,0,1},{2,1,0}};
    byte[] ref = null;
    for (final int[] order: orders) {
      final HllUnion u = new HllUnion(7);
      for (final int i: order) { u.update(in[i]); }
      final byte[] img = u.getResult(HLL_8).toUpdatableByteArray();
      if (ref == null) { ref = img; } else { assertEquals(img, ref); }
    }
  }

  @Test
  public void checkReadingAnEstimateDoesNotChangeTheResult() {
    //the rebuild is lazy; reading an estimate forces it early, which must not be observable
    final HllSketch p = build(13, HLL_8, 0, 50000);
    final HllSketch q = build(13, HLL_8, 50000, 100000);

    for (final int lgMaxK: new int[] {7, 8, 9}) {
      final HllUnion peeked = new HllUnion(lgMaxK);
      peeked.update(p); peeked.update(q);
      peeked.getEstimate();                                   //forces the rebuild here
      for (long v = 9000000; v < 9400000; v++) { peeked.update(v); }

      final HllUnion plain = new HllUnion(lgMaxK);
      plain.update(p); plain.update(q);
      for (long v = 9000000; v < 9400000; v++) { plain.update(v); }

      assertEquals(peeked.getResult(HLL_8).toUpdatableByteArray(),
                   plain.getResult(HLL_8).toUpdatableByteArray());
    }
  }

  @Test
  public void checkCurMinAndNumAtCurMinAgreeWithTheRegisters() {
    //HLL_8 convention: curMin is always 0 and numAtCurMin counts the zero registers
    final HllUnion u = new HllUnion(8);
    u.update(build(15, HLL_8, 0, 100000));
    u.update(build(8, HLL_8, 100000, 200000));
    final byte[] img = u.getResult(HLL_8).toUpdatableByteArray();
    final MemorySegment seg = MemorySegment.ofArray(img);

    int zeros = 0;
    for (int i = HLL_BYTE_ARR_START; i < img.length; i++) { if (img[i] == 0) { zeros++; } }

    assertEquals(extractCurMin(seg), 0);
    assertEquals(extractNumAtCurMin(seg), zeros);
  }

  @Test
  public void checkHipAccumIsZeroOnceOutOfOrder() {
    //HIP is meaningless once the out-of-order flag is set. It must not keep accumulating, or
    //the serialized image depends on the update history after the merge.
    final HllSketch p = build(13, HLL_8, 0, 50000);
    final HllSketch q = build(13, HLL_8, 50000, 100000);

    final HllUnion few = new HllUnion(8);
    few.update(p); few.update(q);
    for (long v = 9000000; v < 9000010; v++) { few.update(v); }

    final HllUnion many = new HllUnion(8);
    many.update(p); many.update(q);
    for (long v = 9000000; v < 9200000; v++) { many.update(v); }

    for (final HllUnion u: Arrays.asList(few, many)) {
      final MemorySegment seg = MemorySegment.ofArray(u.getResult(HLL_8).toUpdatableByteArray());
      assertTrue(extractOooFlag(seg));
      assertEquals(extractHipAccum(seg), 0.0, 0.0);
    }
  }

  @Test
  public void checkRelativeErrorConstantsAreFullPrecision() {
    //lg_k > 12 uses the closed form rather than the interpolation table, so a constant rounded
    //to seven digits is directly observable in the bounds
    final double hip = sqrt(log(2.0));
    final double nonHip = sqrt((3.0 * log(2.0)) - 1.0);
    assertEquals(HllUtil.HLL_HIP_RSE_FACTOR, hip, 1e-15);
    assertEquals(HllUtil.HLL_NON_HIP_RSE_FACTOR, nonHip, 1e-15);

    for (final int lgK: new int[] {13, 16, 21}) {
      final double k = 1 << lgK;
      for (int sd = 1; sd <= 3; sd++) {
        assertEquals(BaseHllSketch.getRelErr(false, false, lgK, sd), sd * hip / sqrt(k), 1e-15);
        assertEquals(BaseHllSketch.getRelErr(false, true, lgK, sd), sd * nonHip / sqrt(k), 1e-15);
      }
    }
  }
}
