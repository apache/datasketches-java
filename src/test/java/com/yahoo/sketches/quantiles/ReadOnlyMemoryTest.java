/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesReadOnlyException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingSparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    final byte[] bytes = s1.toByteArray(false);
    Assert.assertEquals(bytes.length, 64); // 32 + MIN_K(=2) * 2 * 8 = 64
    //final Memory mem = Memory.wrap(ByteBuffer.wrap(bytes)
    // .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Memory mem = Memory.wrap(bytes);
    final UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);

    try {
      s2.update(3);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }
  }

  @Test
  public void wrapCompactSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    //Memory mem = Memory.wrap(ByteBuffer.wrap(s1.compact().toByteArray())
    // .asReadOnlyBuffer().order(ByteOrder.nativeOrder())););
    final Memory mem = Memory.wrap(s1.compact().toByteArray());
    final DoublesSketch s2 = DoublesSketch.wrap(mem); // compact, so this is ok
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
    Assert.assertEquals(s2.getN(), 2);
  }

  @Test
  public void heapifySparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = Memory.wrap(s1.toByteArray(false));
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyAndUpdateSparseSketch() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = Memory.wrap(s1.toByteArray(false));
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
    Memory mem = Memory.wrap(s1.toByteArray(true));
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);
  }

  @Test
  public void heapifyEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final Memory mem = Memory.wrap(s1.toByteArray());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyEmptyCompactSketch() {
    final CompactDoublesSketch s1 = DoublesSketch.builder().build().compact();
    final Memory mem = Memory.wrap(s1.toByteArray());
    DoublesSketch s2 = DoublesSketch.heapify(mem);
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void wrapEmptyUpdateSketch() {
    final UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    final Memory mem = Memory.wrap(s1.toByteArray());
    UpdateDoublesSketch s2 = (UpdateDoublesSketch) DoublesSketch.wrap(mem);
    Assert.assertTrue(s2.isEmpty());

    // ensure the various put calls fail
    try {
      s2.putMinValue(-1.0);
      fail();
    } catch (final SketchesReadOnlyException e) {
      // expected
    }

    try {
      s2.putMaxValue(1.0);
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
    final Memory mem = Memory.wrap(s1.compact().toByteArray());
    DoublesSketch s2 = DoublesSketch.wrap(mem); // compact, so this is ok
    Assert.assertTrue(s2.isEmpty());
  }

  @Test
  public void heapifyUnionFromSparse() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = Memory.wrap(s1.toByteArray(false));
    DoublesUnion u = DoublesUnion.heapify(mem);
    u.update(3);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 3.0);
  }

  @Test
  public void heapifyUnionFromCompact() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = Memory.wrap(s1.toByteArray(true));
    DoublesUnion u = DoublesUnion.heapify(mem);
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
    Memory mem = Memory.wrap(s1.toByteArray(false));
    DoublesUnion u = DoublesUnion.wrap(mem);
    DoublesSketch s2 = u.getResult();
    Assert.assertEquals(s2.getMinValue(), 1.0);
    Assert.assertEquals(s2.getMaxValue(), 2.0);

    // ensure update and reset methods fail
    try {
      u.update(3);
      fail();
    } catch (SketchesReadOnlyException e) {
      // expected
    }

    try {
      u.update(s2);
      fail();
    } catch (SketchesReadOnlyException e) {
      // expected
    }

    try {
      u.update(mem);
      fail();
    } catch (SketchesReadOnlyException e) {
      // expected
    }

    try {
      u.reset();
      fail();
    } catch (SketchesReadOnlyException e) {
      // expected
    }

    try {
      u.getResultAndReset();
      fail();
    } catch (SketchesReadOnlyException e) {
      // expected
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void wrapUnionFromCompact() {
    UpdateDoublesSketch s1 = DoublesSketch.builder().build();
    s1.update(1);
    s1.update(2);
    Memory mem = Memory.wrap(s1.toByteArray(true));
    DoublesUnion.wrap(mem);
    fail();
  }

}
