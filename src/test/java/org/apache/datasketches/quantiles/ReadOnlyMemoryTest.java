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

import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingSparseSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final byte[] bytes = s1.toByteArray(false);
    Assert.assertEquals(bytes.length, 64); // 32 + MIN_K(=2) * 2 * 8 = 64
    //final MemorySegment seg = MemorySegment.ofArray(ByteBuffer.wrap(bytes)
    // .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(seg);
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 2.0);

    try {
      s2.update(3);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }
  }

  @Test
  public void wrapCompactSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    //MemorySegment seg = MemorySegment.ofArray(ByteBuffer.wrap(s1.compact().toByteArray())
    // .asReadOnlyBuffer().order(ByteOrder.nativeOrder())););
    final MemorySegment seg = MemorySegment.ofArray(s1.compact().toByteArray());
    final DoublesSketch s2 = DoublesSketch.wrap(seg); // compact, so this is ok
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 2.0);
    Assert.assertEquals(s2.getN(), 2);
  }

  @Test
  public void heapifySparseSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final DoublesSketch s2 = DoublesSketch.heapify(seg);
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.heapify(seg);
    s2.update(3);
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void heapifyCompactSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    final DoublesSketch s2 = DoublesSketch.heapify(seg);
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 2.0);
  }

  @Test
  public void heapifyEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray());
    final DoublesSketch s2 = DoublesSketch.heapify(seg);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyEmptyCompactSketch() {
    final CompactDoublesSketch s1 = DoublesSketch.builder().build().compact();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray());
    final DoublesSketch s2 = DoublesSketch.heapify(seg);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray());
    final UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(seg);
    Assert.assertTrue(s2.isEmpty());

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
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final MemorySegment seg = MemorySegment.ofArray(s1.compact().toByteArray());
    final DoublesSketch s2 = DoublesSketch.wrap(seg); // compact, so this is ok
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyUnionFromSparse() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false));
    final DoublesUnion u = DoublesUnion.heapify(seg);
    u.update(3);
    final DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void heapifyUnionFromCompact() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    final DoublesUnion u = DoublesUnion.heapify(seg);
    u.update(3);
    final DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 3.0);
  }

  @Test
  public void wrapUnionFromSparse() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(false)).asReadOnly();
    final DoublesUnion u = DoublesUnion.wrap(seg);
    final DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinItem(), 1.0);
    Assert.assertEquals(s2.getMaxItem(), 2.0);

    // ensure update and reset methods fail
    try {
      u.update(3);
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      u.union(s2);
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      u.union(seg);
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      u.reset();
      fail();
    } catch (final IllegalArgumentException e) {
      // expected
    }

    try {
      u.getResultAndReset();
      fail();
    } catch (final IllegalArgumentException e) { //null
      // expected
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapUnionFromCompact() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final MemorySegment seg = MemorySegment.ofArray(s1.toByteArray(true));
    DoublesUnion.wrap(seg);
    fail();
  }

}
