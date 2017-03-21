/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DoublesSketch.MIN_K;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.ReadOnlyMemoryException;
import com.yahoo.sketches.SketchesArgumentException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingSparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final byte[] bytes = s1.toByteArray(false);
    Assert.assertEquals(bytes.length, 64); // 32 + MIN_K(=2) * 2 * 8 = 64
    final Memory mem = NativeMemory.wrap(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
    final UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);

    try {
      s2.update(3);
      fail();
    } catch (final ReadOnlyMemoryException e) {
      // expected
    }
  }

  @Test
  public void wrapCompactSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.compact().toByteArray()).asReadOnlyBuffer());
    DoublesSketch.wrap(mem); // compact, so this is ok
  }

  @Test
  public void heapifySparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false)).asReadOnlyBuffer());
    UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.heapify(mem);
    s2.update(3);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

  @Test
  public void heapifyCompactSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final Memory mem
            = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray()).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyEmptyCompactSketch() {
    final CompactDoublesSketch s1 = DoublesSketch.builder().build().compact();
    final Memory mem
            = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray()).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final Memory mem
            = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray()).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.wrap(mem);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyUnionFromSparse() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false)).asReadOnlyBuffer());
    DoublesUnion u = DoublesUnionBuilder.heapify(mem);
    u.update(3);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

  @Test
  public void heapifyUinonFromCompact() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true)).asReadOnlyBuffer());
    DoublesUnion u = DoublesUnionBuilder.heapify(mem);
    u.update(3);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

  @Test
  public void wrapUnionFromSparse() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false)).asReadOnlyBuffer());
    DoublesUnion u = DoublesUnionBuilder.wrap(mem);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);

    boolean thrown = false;
    try {
      u.update(3);
    } catch (ReadOnlyMemoryException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
}

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapUnionFromCompact() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true)).asReadOnlyBuffer());
    DoublesUnion u = DoublesUnionBuilder.wrap(mem);
    fail();
  }

}
