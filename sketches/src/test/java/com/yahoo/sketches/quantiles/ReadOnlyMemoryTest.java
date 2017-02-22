package com.yahoo.sketches.quantiles;

import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.ReadOnlyMemoryException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapSparseSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.wrap(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test(expectedExceptions = ReadOnlyMemoryException.class)
  public void wrapAndTryUpdateSparseSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.wrap(mem);
    s2.update(3);
  }

  @Test
  public void wrapCompactSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.wrap(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifySparseSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(false, false)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    s2.update(3);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

  @Test
  public void heapifyCompactSketch() {
    DoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(s1.toByteArray(true, true)).asReadOnlyBuffer());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

}
