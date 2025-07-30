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

package org.apache.datasketches.quantiles;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HeapCompactDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  @Test
  public void heapifyFromUpdateSketch() {
    final int k = 4;
    final int n = 45;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n);
    final byte[] qsBytes = qs.toByteArray();
    final MemorySegment qsSeg = MemorySegment.ofArray(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsSeg);
    DoublesSketchTest.testSketchEquality(qs, compactQs);

    assertNull(compactQs.getMemorySegment());
  }

  @Test
  public void createFromUnsortedUpdateSketch() {
    final int k = 4;
    final int n = 13;
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
    for (int i = n; i > 0; --i) {
      qs.update(i);
    }
    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.createFromUpdateSketch(qs);

    // don't expect equal but new base buffer should be sorted
    checkBaseBufferIsSorted(compactQs);
  }

  @Test
  public void heapifyFromCompactSketch() {
    final int k = 8;
    final int n = 177;
    final UpdateDoublesSketch qs = buildAndLoadQS(k, n); // assuming reverse ordered inserts

    final byte[] qsBytes = qs.compact().toByteArray();
    final MemorySegment qsSeg = MemorySegment.ofArray(qsBytes);

    final HeapCompactDoublesSketch compactQs = HeapCompactDoublesSketch.heapifyInstance(qsSeg);
    DoublesSketchTest.testSketchEquality(qs, compactQs);
  }

  @Test
  public void checkHeapifyUnsortedCompactV2() {
    final int k = 64;
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(64).build();
    for (int i = 0; i < (3 * k); ++i) {
      qs.update(i);
    }
    assertEquals(qs.getBaseBufferCount(), k);
    final byte[] sketchBytes = qs.toByteArray(true);
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);

    // modify to make v2, clear compact flag, and insert a -1 in the middle of the base buffer
    PreambleUtil.insertSerVer(seg, 2);
    PreambleUtil.insertFlags(seg, 0);
    final long tgtAddr = COMBINED_BUFFER + ((Double.BYTES * (long)k) / 2);
    seg.set(JAVA_DOUBLE_UNALIGNED, tgtAddr, -1.0);
    assert seg.get(JAVA_DOUBLE_UNALIGNED, tgtAddr - Double.BYTES) > seg.get(JAVA_DOUBLE_UNALIGNED, tgtAddr);

    // ensure the heapified base buffer is sorted
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(seg);
    checkBaseBufferIsSorted(qs2);
  }

  @Test
  public void checkEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final UpdateDoublesSketch qs1 = buildAndLoadQS(k, 0);
    final byte[] byteArr = qs1.compact().toByteArray();
    final byte[] byteArr2 = qs1.toByteArray(true);
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final HeapCompactDoublesSketch qs2 = HeapCompactDoublesSketch.heapifyInstance(seg);
    assertTrue(qs2.isEmpty());
    assertEquals(byteArr.length, qs1.getSerializedSizeBytes());
    assertEquals(byteArr, byteArr2);
    try { qs2.getQuantile(0.5); fail(); } catch (final IllegalArgumentException e) { }
    try { qs2.getQuantiles(new double[] {0.0, 0.5, 1.0}); fail(); } catch (final IllegalArgumentException e) { }
    try { qs2.getRank(0); fail(); } catch (final IllegalArgumentException e) { }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmall1() {
    final MemorySegment seg = MemorySegment.ofArray(new byte[7]);
    HeapCompactDoublesSketch.heapifyInstance(seg);
  }

  static void checkBaseBufferIsSorted(final HeapCompactDoublesSketch qs) {
    final double[] combinedBuffer = qs.getCombinedBuffer();
    final int bbCount = qs.getBaseBufferCount();

    for (int i = 1; i < bbCount; ++i) {
      assert combinedBuffer[i - 1] <= combinedBuffer[i];
    }
  }

  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n) {
    return buildAndLoadQS(k, n, 0);
  }

  static UpdateDoublesSketch buildAndLoadQS(final int k, final int n, final int startV) {
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(k).build();
    for (int i = 1; i <= n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
    print("PRINTING: " + this.getClass().getName() + LS);
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.err.print(s); //disable here
  }

}
