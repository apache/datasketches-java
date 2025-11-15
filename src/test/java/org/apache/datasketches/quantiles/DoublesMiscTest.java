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

import static org.apache.datasketches.common.MemorySegmentStatus.isSameResource;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.common.SketchesStateException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DoublesMiscTest {

  @Test
  public void wrapAndUpdating() {
    final UpdatableQuantilesDoublesSketch sk1 = QuantilesDoublesSketch.builder().build();
    sk1.update(1);
    sk1.update(2);
    final byte[] bytes = sk1.toByteArray(false);
    final int curBytes = sk1.getCurrentUpdatableSerializedSizeBytes();
    Assert.assertEquals(bytes.length, curBytes);
    //convert to MemorySegment
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final UpdatableQuantilesDoublesSketch sk2 = (UpdatableQuantilesDoublesSketch) QuantilesDoublesSketch.writableWrap(seg, null);
    assertEquals(seg.byteSize(), curBytes);
    sk2.update(3);
    sk2.update(4);
    assertEquals(sk2.getMinItem(), 1.0);
    assertEquals(sk2.getMaxItem(), 4.0);
    //check the size for just 4 elements
    final MemorySegment seg2 = sk2.getMemorySegment();
    assertEquals(seg2.byteSize(), QuantilesDoublesSketch.getUpdatableStorageBytes(sk2.getK(), sk2.getN()));
  }

  @Test
  public void wrapCompactSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.compact().toByteArray());
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.wrap(seg); // compact, so this is ok
    assertEquals(s2.getMinItem(), 1.0);
    assertEquals(s2.getMaxItem(), 2.0);
    assertEquals(s2.getN(), 2);
  }

  @Test
  public void heapifySparseSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.heapify(seg);
    assertEquals(s2.getMinItem(), 1.0);
    assertEquals(s2.getMaxItem(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final UpdatableQuantilesDoublesSketch s2 = (UpdatableQuantilesDoublesSketch) QuantilesDoublesSketch.heapify(seg);
    s2.update(3);
    assertEquals(s2.getMinItem(), 1.0);
    assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void heapifyCompactSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.heapify(seg);
    assertEquals(s2.getMinItem(), 1.0);
    assertEquals(s2.getMaxItem(), 2.0);
  }

  @Test
  public void heapifyEmptyUpdateSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray());
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.heapify(seg);
    assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyEmptyCompactSketch() {
    final CompactQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build().compact();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray());
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.heapify(seg);
    assertTrue(s2.isEmpty());
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray()).asReadOnly();
    final UpdatableQuantilesDoublesSketch s2 = (UpdatableQuantilesDoublesSketch) QuantilesDoublesSketch.writableWrap(seg, null);
    assertTrue(s2.isEmpty());

    // ensure the various put calls fail
    try {
      s2.putMinItem(-1.0);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putMaxItem(1.0);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putN(1);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putBitPattern(1);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.reset();
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putBaseBufferCount(5);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putCombinedBuffer(new double[16]);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      final int currCap = s2.getCombinedBufferItemCapacity();
      s2.growCombinedBuffer(currCap, 2 * currCap);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }
  }

  @Test
  public void wrapEmptyCompactSketch() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.compact().toByteArray());
    final QuantilesDoublesSketch s2 = QuantilesDoublesSketch.wrap(seg); // compact, so this is ok
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyUnionFromSparse() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final QuantilesDoublesUnion u = QuantilesDoublesUnion.heapify(seg);
    u.update(3);
    final QuantilesDoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void initializeUnionFromCompactSegment() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    final QuantilesDoublesUnion u = QuantilesDoublesUnion.heapify(seg);
    u.update(3);
    final QuantilesDoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void unionFromUpdatableSegment() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final QuantilesDoublesUnion u = QuantilesDoublesUnion.wrap(seg);
    u.update(3);
    final QuantilesDoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void wrapUnionFromHeap() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false)).asReadOnly();
    try {
      final QuantilesDoublesUnion u = QuantilesDoublesUnion.wrap(seg, null);
    } catch (final SketchesReadOnlyException e) {
      //expected
    }

  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapUnionFromCompact() {
    final UpdatableQuantilesDoublesSketch s1 = QuantilesDoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    QuantilesDoublesUnion.wrap(seg, null); //compact seg
    fail();
  }

  /**
   * println
   * @param o object to print
   */
  private static void println(final Object o) {
    //System.out.println(o.toString());
  }

}
