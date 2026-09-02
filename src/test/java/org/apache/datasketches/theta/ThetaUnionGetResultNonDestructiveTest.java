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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * getResult() reads the gadget's hash table in place unless it needs to run quickSelect,
 * so it must leave the union usable afterwards.
 */
public class ThetaUnionGetResultNonDestructiveTest {

  private static ThetaUnion heapUnion(final int k) {
    return ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
  }

  private static ThetaUnion segUnion(final int k) {
    return ThetaSetOperation.builder().setNominalEntries(k)
        .buildUnion(MemorySegment.ofArray(new byte[ThetaSetOperation.getMaxUnionBytes(k)]));
  }

  @Test
  public void checkReturnedSketchIsIndependentOfLaterUpdates() {
    final int k = 1 << 10;
    for (final boolean seg : new boolean[] {false, true}) {
      final ThetaUnion u = seg ? segUnion(k) : heapUnion(k);
      for (int i = 0; i < (k / 4); i++) { u.update(i); }
      final CompactThetaSketch snapshot = u.getResult(true, null);
      final byte[] snapshotBytes = snapshot.toByteArray();
      final double snapshotEstimate = snapshot.getEstimate();

      for (int i = k; i < (16 * k); i++) { u.update(i); }

      assertEquals(snapshot.toByteArray(), snapshotBytes, "seg=" + seg);
      assertEquals(snapshot.getEstimate(), snapshotEstimate, 0.0);
      assertTrue(u.getResult().getEstimate() > snapshotEstimate);
    }
  }

  /** Probing part way through must not change what the union goes on to produce. */
  @Test
  public void checkUnionStillCorrectAfterGetResult() {
    final int k = 1 << 10;
    for (final int firstBatch : new int[] {1, k / 2, k, 2 * k}) {
      for (final boolean seg : new boolean[] {false, true}) {
        final ThetaUnion probed = seg ? segUnion(k) : heapUnion(k);
        final ThetaUnion control = seg ? segUnion(k) : heapUnion(k);

        for (int i = 0; i < firstBatch; i++) {
          probed.update(i);
          control.update(i);
        }
        probed.getResult(true, null);
        probed.getResult(false, null);

        for (int i = firstBatch; i < (firstBatch + (4 * k)); i++) {
          probed.update(i);
          control.update(i);
        }
        // Any divergence means getResult mutated state it shouldn't have
        assertEquals(probed.getResult(true, null).toByteArray(),
            control.getResult(true, null).toByteArray(),
            "firstBatch=" + firstBatch + " seg=" + seg);
      }
    }
  }

  /**
   * quickSelect permutes the table it is given. Were that the gadget's own table, the only
   * symptom would be that values the sketch already holds stop counting as duplicates.
   */
  @Test
  public void checkDuplicatesStillDetectedAfterGetResultRanQuickSelect() {
    final int k = 1 << 10;
    for (final boolean seg : new boolean[] {false, true}) {
      final ThetaUnion u = seg ? segUnion(k) : heapUnion(k);
      final int n = (2 * k) - 10; //above k, so getResult must run quickSelect
      for (int i = 0; i < n; i++) { u.update(i); }

      final CompactThetaSketch res = u.getResult(true, null);
      final int countBefore = res.getRetainedEntries(true);
      assertTrue(countBefore > 0);

      for (int i = 0; i < n; i++) { u.update(i); }

      assertEquals(u.getResult(true, null).getRetainedEntries(true), countBefore, "seg=" + seg);
      assertEquals(u.getResult(true, null).toByteArray(), res.toByteArray(), "seg=" + seg);
    }
  }
}
