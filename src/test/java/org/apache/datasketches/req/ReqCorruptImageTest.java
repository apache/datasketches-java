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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that corrupt ReqSketch images are rejected without large allocations.
 */
public class ReqCorruptImageTest {
  private static final int EST_SECTION_SIZE_FLOAT = 32; //first compactor, estimation mode
  private static final int EST_NUM_SECTIONS_BYTE = 37;
  private static final int EST_COUNT_INT = 40;
  private static final int EXACT_COUNT_INT = 24;
  private static final int EST_N_LONG = 8; //estimation mode //the only compactor, exact mode
  private static final int NUM_COMPACTORS_BYTE = 6;
  private static final int COMPACTOR_HEADER_BYTES = 20; //state, sectionSize, lgWeight, numSections, unused, count

  private static byte[] image(final int n) {
    final ReqSketch sk = ReqSketch.builder().setK(12).build();
    for (int i = 0; i < n; i++) { sk.update(i); }
    return sk.toByteArray();
  }

  private static void checkRejected(final byte[] bytes) {
    final long allocated = AllocationTracker.allocatedBytes(() -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    expectThrows(SketchesArgumentException.class, () -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
  }

  @Test
  public void checkValidImagesStillWork() {
    for (final int n : new int[] {0, 5, 50, 10_000, 1_000_000}) {
      final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(image(n)));
      assertEquals(sk.getN(), n);
      if (n > 0) {
        sk.update(-1f); //the reconstructed buffers grow as needed
        assertEquals(sk.getN(), n + 1);
      }
    }
  }

  @Test
  public void checkCountBeyondImage() {
    final byte[] est = image(10_000);
    ByteArrayUtil.putIntLE(est, EST_COUNT_INT, 1 << 28);
    checkRejected(est);
    final byte[] exact = image(50);
    ByteArrayUtil.putIntLE(exact, EXACT_COUNT_INT, 1 << 28);
    checkRejected(exact);
  }

  @Test
  public void checkSectionsTooLarge() {
    final byte[] size = image(10_000);
    ByteArrayUtil.putFloatLE(size, EST_SECTION_SIZE_FLOAT, 1e8f);
    checkRejected(size);
    final byte[] sections = image(10_000);
    sections[EST_NUM_SECTIONS_BYTE] = 127;
    ByteArrayUtil.putFloatLE(sections, EST_SECTION_SIZE_FLOAT, 12f);
    checkRejected(sections);
    final byte[] negative = image(10_000);
    negative[EST_NUM_SECTIONS_BYTE] = -1;
    checkRejected(negative);
  }

  @Test
  public void checkTooManySections() {
    final byte[] bytes = image(10_000);
    bytes[EST_NUM_SECTIONS_BYTE] = 49; //49 * 2 is within k * k, but above the maximum of 48
    ByteArrayUtil.putFloatLE(bytes, EST_SECTION_SIZE_FLOAT, 2f);
    checkRejected(bytes);
    bytes[EST_NUM_SECTIONS_BYTE] = 48; //the maximum is accepted
    assertEquals(ReqSketch.heapify(MemorySegment.ofArray(bytes)).getN(), 10_000);
  }

  // A compactor whose state allows another doubling stops at the maximum number of sections,
  // so it still fits the byte that holds it and the image can be read back.
  @Test
  public void checkSectionDoublingStopsAtMax() {
    for (final boolean hra : new boolean[] {true, false}) {
      final FloatBuffer buf = FloatBuffer.reconstruct(new float[0], 0, 0, 2 * 12 * 48, true, hra);
      final ReqCompactor c = new ReqCompactor((byte) 0, hra, 1L << 50, 64f, ReqSketch.MAX_NUM_SECTIONS, buf);
      c.merge(new ReqCompactor((byte) 0, hra, 64, null));
      assertEquals(c.getNumSections(), ReqSketch.MAX_NUM_SECTIONS);
      assertEquals(c.getSectionSize(), 64);
    }
  }

  @Test
  public void checkNNotEqualToWeight() {
    final byte[] bytes = image(10_000);
    assertEquals(ByteArrayUtil.getLongLE(bytes, EST_N_LONG), 10_000L);
    ByteArrayUtil.putLongLE(bytes, EST_N_LONG, 10_001L);
    checkRejected(bytes);
    ByteArrayUtil.putLongLE(bytes, EST_N_LONG, -10_000L);
    checkRejected(bytes);
    ByteArrayUtil.putLongLE(bytes, EST_N_LONG, Long.MAX_VALUE);
    checkRejected(bytes);
  }

  @Test
  public void checkInvalidK() {
    final byte[] bytes = image(10_000);
    ByteArrayUtil.putShortLE(bytes, 4, (short) 60_000);
    checkRejected(bytes);
  }

  //Appends empty compactors to an estimation-mode image until it has numLevels levels. Each has lgWeight equal
  //to its level and no items, so it passes the lgWeight and N checks.
  private static byte[] withLevels(final byte[] image, final int numLevels) {
    final int levels = image[NUM_COMPACTORS_BYTE] & 0xFF;
    final byte[] bytes = Arrays.copyOf(image, image.length + ((numLevels - levels) * COMPACTOR_HEADER_BYTES));
    for (int lvl = levels; lvl < numLevels; lvl++) {
      final int off = image.length + ((lvl - levels) * COMPACTOR_HEADER_BYTES);
      ByteArrayUtil.putLongLE(bytes, off, 0L);          //state
      ByteArrayUtil.putFloatLE(bytes, off + 8, 12f);    //sectionSize = k
      bytes[off + 12] = (byte) lvl;                     //lgWeight
      bytes[off + 13] = 3;                              //numSections
      ByteArrayUtil.putIntLE(bytes, off + 16, 0);       //count
    }
    bytes[NUM_COMPACTORS_BYTE] = (byte) numLevels;
    return bytes;
  }

  //The total weight exceeds Long.MAX_VALUE, so no N is valid, including -1.
  @Test
  public void checkWeightOverflow() {
    for (final int[] levelsAndCount : new int[][] {{63, 2}, {64, 1}}) { //2 items at level 62, 1 item at level 63
      final byte[] image = withLevels(image(10_000), levelsAndCount[0]);
      final int count = levelsAndCount[1];
      final byte[] bytes = Arrays.copyOf(image, image.length + (count * Float.BYTES));
      ByteArrayUtil.putIntLE(bytes, image.length - Integer.BYTES, count); //the top compactor
      for (final long n : new long[] {-1L, 10_000L, Long.MAX_VALUE, Long.MIN_VALUE}) {
        ByteArrayUtil.putLongLE(bytes, EST_N_LONG, n);
        checkRejected(bytes);
        final SketchesArgumentException e = expectThrows(SketchesArgumentException.class,
            () -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
        assertTrue(e.getMessage().contains("exceeds Long.MAX_VALUE"), e.getMessage());
      }
    }
  }

  @Test
  public void checkTooManyLevels() {
    final byte[] image = image(10_000);
    final ReqSketch sk = ReqSketch.heapify(MemorySegment.ofArray(withLevels(image, ReqSketch.MAX_NUM_LEVELS)));
    assertEquals(sk.getN(), 10_000L);
    assertEquals(sk.getNumLevels(), ReqSketch.MAX_NUM_LEVELS);
    for (final int numLevels : new int[] {ReqSketch.MAX_NUM_LEVELS + 1, 127, 255}) {
      final byte[] bytes = withLevels(image, numLevels);
      checkRejected(bytes);
      final SketchesArgumentException e = expectThrows(SketchesArgumentException.class,
          () -> ReqSketch.heapify(MemorySegment.ofArray(bytes)));
      assertTrue(e.getMessage().contains("number of levels"), e.getMessage());
    }
  }
}
