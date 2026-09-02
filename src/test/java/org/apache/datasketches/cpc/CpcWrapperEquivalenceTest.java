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

import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.testng.annotations.Test;

/**
 * A CpcWrapper answers from the serialized preamble and a CpcSketch answers from a decompressed
 * sketch, so the two must agree for every lgK, flavor and empty form.
 */
public class CpcWrapperEquivalenceTest {

  private static void assertAgrees(final String what, final CpcSketch sk) {
    final byte[] bytes = sk.toByteArray();
    final CpcSketch heapified = CpcSketch.heapify(MemorySegment.ofArray(bytes));
    final CpcWrapper wrapped = new CpcWrapper(MemorySegment.ofArray(bytes));

    assertEquals(wrapped.getLgK(), heapified.getLgK(), what + " lgK");
    assertEquals(wrapped.getEstimate(), heapified.getEstimate(), 0.0, what + " estimate");
    for (final int kappa : new int[] {1, 2, 3}) {
      assertEquals(wrapped.getLowerBound(kappa), heapified.getLowerBound(kappa), 0.0,
          what + " lowerBound kappa=" + kappa);
      assertEquals(wrapped.getUpperBound(kappa), heapified.getUpperBound(kappa), 0.0,
          what + " upperBound kappa=" + kappa);
    }
    assertEquals(new CpcWrapper(bytes).getEstimate(), wrapped.getEstimate(), 0.0,
        what + " byte[] ctor");
  }

  @Test
  public void checkEmptySketchesAreReadable() {
    for (int lgK = 4; lgK <= 16; lgK++) {
      final CpcSketch fresh = new CpcSketch(lgK); //EMPTY_HIP
      assertEquals(new CpcWrapper(fresh.toByteArray()).getEstimate(), 0.0, 0.0);
      assertAgrees("empty fresh lgK=" + lgK, fresh);

      final CpcSketch merged = new CpcUnion(lgK).getResult(); //EMPTY_MERGED
      assertEquals(new CpcWrapper(merged.toByteArray()).getEstimate(), 0.0, 0.0);
      assertAgrees("empty merged lgK=" + lgK, merged);

      final CpcSketch reset = new CpcSketch(lgK);
      for (int i = 0; i < 1000; i++) { reset.update(i); }
      reset.reset();
      assertAgrees("empty reset lgK=" + lgK, reset);
    }
  }

  @Test
  public void checkAgreementAcrossLgKAndFlavors() {
    for (int lgK = 4; lgK <= 16; lgK++) {
      final int k = 1 << lgK;
      final int[] counts = {1, 2, 3, 10, k / 32, k / 16, k / 8, k / 4, k / 2, k, 2 * k, 4 * k};
      for (final int n : counts) {
        if (n <= 0) { continue; }
        final CpcSketch sk = new CpcSketch(lgK);
        for (int i = 0; i < n; i++) { sk.update(i); }
        assertAgrees("lgK=" + lgK + " n=" + n, sk);
      }
    }
  }

  /** Union results take the merged (ICON) path rather than the HIP path. */
  @Test
  public void checkAgreementForUnionResults() {
    for (final int lgK : new int[] {8, 11, 12, 14}) {
      for (final int parts : new int[] {1, 2, 8, 64}) {
        final CpcUnion u = new CpcUnion(lgK);
        long v = 0;
        for (int p = 0; p < parts; p++) {
          final CpcSketch s = new CpcSketch(lgK);
          for (int i = 0; i < 3000; i++) { s.update(++v); }
          u.update(s);
        }
        assertAgrees("union lgK=" + lgK + " parts=" + parts, u.getResult());
      }
    }
  }

  @Test
  public void checkAgreementForStringAndRandomInput() {
    for (final int lgK : new int[] {11, 12, 14}) {
      for (final int n : new int[] {1, 50, 5000, 100_000}) {
        final CpcSketch sk = new CpcSketch(lgK);
        for (int i = 0; i < n; i++) { sk.update("user-" + i + "-abcdefabcdefabcdef"); }
        assertAgrees("string lgK=" + lgK + " n=" + n, sk);
      }
      final Random rnd = new Random(1234567L + lgK);
      final CpcSketch rs = new CpcSketch(lgK);
      for (int i = 0; i < 50_000; i++) { rs.update(rnd.nextLong()); }
      assertAgrees("random lgK=" + lgK, rs);
    }
  }
}
