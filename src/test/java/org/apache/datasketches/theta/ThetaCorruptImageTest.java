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
import static org.testng.Assert.expectThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * Checks that corrupt theta sketch, union and intersection images are rejected with a
 * SketchesArgumentException before any large allocation (CVE-2026-122096).
 * Ported from ThetaCorruptImageTest in 9.1.0, plus checks for the serial version 1 and 2
 * images that 7.x still reads.
 */
public class ThetaCorruptImageTest {
  private static final int LG_ARR_BYTE = 4;
  private static final int CUR_COUNT_INT = 8;

  private static UpdateSketch sketch(final int n) {
    final UpdateSketch sk = UpdateSketch.builder().build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk;
  }

  private static void checkRejected(final byte[] bytes, final Function<Memory, Object> deser) {
    expectThrows(SketchesArgumentException.class, () -> deser.apply(Memory.wrap(bytes)));
  }

  @Test
  public void checkValidImagesStillWork() {
    final UpdateSketch sk = sketch(1000);
    final byte[] c = sk.compact().toByteArray();
    assertEquals(CompactSketch.heapify(Memory.wrap(c)).getRetainedEntries(), 1000);
    assertEquals(CompactSketch.wrap(Memory.wrap(c)).getRetainedEntries(), 1000);
    assertEquals(Sketch.heapify(Memory.wrap(c)).getRetainedEntries(), 1000);
    final byte[] z = sk.compact().toByteArrayCompressed();
    assertEquals(CompactSketch.heapify(Memory.wrap(z)).getRetainedEntries(), 1000);
    assertEquals(CompactSketch.wrap(Memory.wrap(z)).getRetainedEntries(), 1000);
    assertEquals(Sketches.heapifyUpdateSketch(Memory.wrap(sk.toByteArray())).getRetainedEntries(), 1000);
    assertEquals(Sketch.heapify(Memory.wrap(sk.toByteArray())).getRetainedEntries(), 1000);
    assertEquals(Sketch.wrap(Memory.wrap(sk.toByteArray())).getRetainedEntries(), 1000);
    final Union union = SetOperation.builder().buildUnion();
    union.union(sk);
    assertEquals(((Union) SetOperation.heapify(Memory.wrap(union.toByteArray())))
        .getResult().getRetainedEntries(), 1000);
    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk);
    assertEquals(((Intersection) SetOperation.heapify(Memory.wrap(inter.toByteArray())))
        .getResult().getRetainedEntries(), 1000);
    //empty and single-item compact images
    assertEquals(Sketch.heapify(Memory.wrap(UpdateSketch.builder().build().compact().toByteArray()))
        .getRetainedEntries(), 0);
    assertEquals(Sketch.heapify(Memory.wrap(sketch(1).compact().toByteArray())).getRetainedEntries(), 1);
  }

  @Test
  public void checkCompactCountBeyondImage() {
    final byte[] bytes = sketch(100).compact().toByteArray();
    ByteArrayUtil.putIntLE(bytes, CUR_COUNT_INT, 1 << 28);
    checkRejected(bytes, CompactSketch::heapify);
    checkRejected(bytes, Sketch::heapify);
    checkRejected(bytes, mem -> CompactSketch.wrap(mem).getCache());
    final byte[] negative = sketch(100).compact().toByteArray();
    ByteArrayUtil.putIntLE(negative, CUR_COUNT_INT, -1);
    checkRejected(negative, CompactSketch::heapify);
  }

  @Test
  public void checkCompressedCountBeyondImage() {
    final byte[] bytes = sketch(100).compact().toByteArrayCompressed();
    final int numEntriesBytes = bytes[PreambleUtil.NUM_ENTRIES_BYTES_BYTE_V4];
    final int offset = (bytes[0] & 0x3F) << 3;
    for (int i = 0; i < numEntriesBytes; i++) { bytes[offset + i] = (byte) 0xFF; }
    checkRejected(bytes, CompactSketch::heapify);
    checkRejected(bytes, Sketch::heapify);
    checkRejected(bytes, mem -> CompactSketch.wrap(mem).getCache());
    final byte[] badBits = sketch(100).compact().toByteArrayCompressed();
    badBits[PreambleUtil.ENTRY_BITS_BYTE_V4] = 0;
    checkRejected(badBits, CompactSketch::heapify);
  }

  @Test
  public void checkUpdatableLargeTable() {
    for (final int lgArr : new int[] {28, 29, 30}) { //(8 << lgArr) overflows int
      final byte[] bytes = sketch(100).toByteArray();
      bytes[LG_ARR_BYTE] = (byte) lgArr;
      checkRejected(bytes, Sketches::heapifyUpdateSketch);
      checkRejected(bytes, Sketch::heapify);
      checkRejected(bytes, Sketch::wrap);
      checkRejected(bytes, mem -> Sketches.wrapUpdateSketch(WritableMemory.writableWrap(bytes)));
    }
    final byte[] count = sketch(100).toByteArray();
    ByteArrayUtil.putIntLE(count, CUR_COUNT_INT, 1 << 28);
    checkRejected(count, Sketches::heapifyUpdateSketch);
  }

  @Test
  public void checkUnionLargeTable() {
    final Union union = SetOperation.builder().buildUnion();
    union.union(sketch(100));
    final byte[] bytes = union.toByteArray();
    bytes[LG_ARR_BYTE] = 30;
    checkRejected(bytes, SetOperation::heapify);
    checkRejected(bytes, mem -> Sketches.wrapUnion(mem).getResult());
  }

  @Test
  public void checkIntersectionLargeTable() {
    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(sketch(100));
    final byte[] bytes = inter.toByteArray();
    bytes[LG_ARR_BYTE] = 30;
    checkRejected(bytes, SetOperation::heapify);
    checkRejected(bytes, mem -> Sketches.wrapIntersection(mem).getResult());
    final byte[] count = inter.toByteArray();
    ByteArrayUtil.putIntLE(count, CUR_COUNT_INT, 1 << 20); //more than the table holds
    checkRejected(count, SetOperation::heapify);
  }

  @Test
  public void checkLegacySerVer1And2CountOverflow() {
    //7.x only: (curCount + preLongs) << 3 overflowed int, so the size check passed
    for (final int serVer : new int[] {1, 2}) {
      final byte[] bytes = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN)
          .put((byte) 3).put((byte) serVer).put((byte) 3).put(new byte[5])
          .putInt(0x1FFFFFFF).putInt(0).putLong(Long.MAX_VALUE / 2).array();
      checkRejected(bytes, CompactSketch::heapify);
      checkRejected(bytes, Sketch::heapify);
      final byte[] negative = bytes.clone();
      ByteArrayUtil.putIntLE(negative, CUR_COUNT_INT, -1);
      checkRejected(negative, CompactSketch::heapify);
    }
  }
}
