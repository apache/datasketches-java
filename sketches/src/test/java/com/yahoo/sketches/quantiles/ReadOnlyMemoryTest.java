/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

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
    byte[] bytes = s1.toByteArray(false, false);
    Assert.assertEquals(bytes.length, 2080); // 32 + 128 * 2 * 8 = 2080
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
    UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);

    boolean thrown = false;
    try {
      s2.update(3);
    } catch (ReadOnlyMemoryException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapCompactSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
    DoublesSketch.wrap(mem);
  }

  @Test
  public void heapifySparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyUinonFromSparse() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
    DoublesUnion u = DoublesUnionBuilder.wrap(mem);
    u.update(3);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

}
