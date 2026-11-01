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
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that heapify() rejects a compactor whose lgWeight does not match its level.
 */
public class ReqSerDeTest {
  private static final int EST_LG_WEIGHT_BYTE = 36; //first compactor, estimation mode
  private static final int EST_COUNT_INT = 40;
  private static final int EST_ITEMS = 44;
  private static final int EXACT_LG_WEIGHT_BYTE = 20; //the only compactor, exact mode

  private static byte[] image(final int n) {
    final ReqSketch sk = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk.toByteArray();
  }

  private static void checkRejected(final byte[] bytes) {
    expectThrows(SketchesArgumentException.class, () -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {0, 5, 50, 10_000, 1_000_000}) {
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(image(n)));
      assertEquals(sk.getN(), n);
    }
  }

  @Test
  public void checkWrongLgWeight() {
    final byte[] large = image(10_000);
    assertEquals(large[EST_LG_WEIGHT_BYTE], 0);
    large[EST_LG_WEIGHT_BYTE] = 70;
    checkRejected(large);
    final byte[] first = image(10_000);
    first[EST_LG_WEIGHT_BYTE] = 1;
    checkRejected(first);
    final byte[] second = image(10_000); //the second compactor repeats the first one's lgWeight
    final int count0 = ByteArrayUtil.getIntLE(second, EST_COUNT_INT);
    final int secondLgWeight = EST_ITEMS + (count0 * Float.BYTES) + 12; //after state(8) and sectionSize(4)
    assertEquals(second[secondLgWeight], 1);
    second[secondLgWeight] = 0;
    checkRejected(second);
    final byte[] exact = image(50);
    assertEquals(exact[EXACT_LG_WEIGHT_BYTE], 0);
    exact[EXACT_LG_WEIGHT_BYTE] = 1;
    checkRejected(exact);
  }
}
