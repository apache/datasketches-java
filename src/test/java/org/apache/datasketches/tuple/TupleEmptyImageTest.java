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

package org.apache.datasketches.tuple;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * Checks that the CompactTupleSketch reader accepts every known form of an empty serial version 3 image
 * and rejects an empty image whose first 8 bytes do not match that form.
 */
public class TupleEmptyImageTest {

  // PreLongs=1, SerVer=3, Family=9, SketchType=1, unused=0, flags, seed hash (2 bytes, little-endian)
  private static byte[] emptyImage(final int flags, final int seedHash) {
    return new byte[] {1, 3, 9, 1, 0, (byte) flags, (byte) seedHash, (byte) (seedHash >>> 8)};
  }

  private static TupleSketch<IntegerSummary> heapify(final byte[] bytes) {
    return TupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new IntegerSummaryDeserializer());
  }

  private static TupleSketch<IntegerSummary> heapify(final byte[] bytes, final long seed) {
    return TupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new IntegerSummaryDeserializer(), seed);
  }

  private static void assertEmpty(final TupleSketch<IntegerSummary> sk) {
    assertTrue(sk.isEmpty());
    assertEquals(sk.getRetainedEntries(), 0);
    assertEquals(sk.getThetaLong(), Long.MAX_VALUE);
  }

  private static void assertRejected(final byte[] bytes) {
    try {
      heapify(bytes);
      fail("expected the empty image to be rejected");
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void acceptsKnownEmptyForms() {
    final int defaultSeedHash = Short.toUnsignedInt(Util.DEFAULT_SEED_HASH);
    assertEmpty(heapify(emptyImage(0x0e, 0)));               //Java since 9.1.0
    assertEmpty(heapify(emptyImage(0x0e, defaultSeedHash))); //Java 3.1.0 to 9.0.0
    assertEmpty(heapify(emptyImage(0x1e, 0)));               //C++, and Java with the ordered flag
    assertEmpty(heapify(emptyImage(0x1e, defaultSeedHash))); //Go
  }

  @Test
  public void seedHashOfEmptyImageIsIgnored() {
    assertEmpty(heapify(emptyImage(0x0e, 0x1234), 12345L));
    assertEmpty(heapify(emptyImage(0x1e, 0xffff), 12345L));
  }

  @Test
  public void acceptsWhatJavaWrites() {
    final byte[] bytes = new UpdatableTupleSketchBuilder<>(new IntegerSummaryFactory()).build()
        .compact().toByteArray();
    assertEmpty(heapify(bytes));
    assertEmpty(heapify(TupleSketch.<IntegerSummary>createEmptySketch().toByteArray()));
  }

  @Test
  public void acceptsTrailingBytes() {
    final byte[] bytes = new byte[24];
    System.arraycopy(emptyImage(0x0e, 0), 0, bytes, 0, 8);
    assertEmpty(heapify(bytes));
  }

  @Test
  public void rejectsWrongPreambleLongs() {
    for (final int preLongs : new int[] {2, 3}) {
      final byte[] bytes = new byte[preLongs * Long.BYTES];
      System.arraycopy(emptyImage(0x0e, 0), 0, bytes, 0, 8);
      bytes[0] = (byte) preLongs;
      assertRejected(bytes);
    }
  }

  @Test
  public void rejectsNonZeroUnusedByte() {
    final byte[] bytes = emptyImage(0x0e, 0);
    bytes[4] = 1;
    assertRejected(bytes);
  }

  @Test
  public void rejectsMissingFlags() {
    assertRejected(emptyImage(0x0c, 0)); //not read-only
    assertRejected(emptyImage(0x06, 0)); //not compact
  }

  @Test
  public void rejectsReservedFlags() {
    for (final int bit : new int[] {0, 5, 6, 7}) {
      assertRejected(emptyImage(0x0e | (1 << bit), 0));
    }
  }
}
