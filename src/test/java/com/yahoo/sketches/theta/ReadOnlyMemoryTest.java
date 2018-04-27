/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesReadOnlyException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingUpdateSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(updateSketch.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    UpdateSketch sketch = (UpdateSketch) Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);

    boolean thrown = false;
    try {
      sketch.update(2);
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void wrapCompactUnorderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void wrapCompactOrderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyUpdateSketch() {
    UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(us1.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    // downcasting is not recommended, for testing only
    UpdateSketch us2 = (UpdateSketch) Sketch.heapify(mem);
    us2.update(2);
    assertEquals(us2.getEstimate(), 2.0);
  }

  @Test
  public void heapifyCompactUnorderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyCompactOrderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyUnion() {
    Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(u1.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Union u2 = (Union) SetOperation.heapify(mem);
    u2.update(2);
    Assert.assertEquals(u2.getResult().getEstimate(), 2.0);
  }

  @Test
  public void wrapAndTryUpdatingUnion() {
    Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    Memory mem = Memory.wrap(ByteBuffer.wrap(u1.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));

    Union u2 = (Union) Sketches.wrapSetOperation(mem);
    Union u3 = Sketches.wrapUnion(mem);
    Assert.assertEquals(u2.getResult().getEstimate(), 1.0);
    Assert.assertEquals(u3.getResult().getEstimate(), 1.0);

    try {
      u2.update(2);
      fail();
    } catch (SketchesReadOnlyException e) {
      //expected
    }

    try {
      u3.update(2);
      fail();
    } catch (SketchesReadOnlyException e) {
      //expected
    }
  }

  @Test
  public void heapifyIntersection() {
    UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    us1.update(2);
    UpdateSketch us2 = UpdateSketch.builder().build();
    us2.update(2);
    us2.update(3);

    Intersection i1 = SetOperation.builder().buildIntersection();
    i1.update(us1);
    i1.update(us2);
    Memory mem = Memory.wrap(ByteBuffer.wrap(i1.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Intersection i2 = (Intersection) SetOperation.heapify(mem);
    i2.update(us1);
    Assert.assertEquals(i2.getResult().getEstimate(), 1.0);
  }

  @Test
  public void wrapIntersection() {
    UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    us1.update(2);
    UpdateSketch us2 = UpdateSketch.builder().build();
    us2.update(2);
    us2.update(3);

    Intersection i1 = SetOperation.builder().buildIntersection();
    i1.update(us1);
    i1.update(us2);
    Memory mem = Memory.wrap(ByteBuffer.wrap(i1.toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    Intersection i2 = (Intersection) SetOperation.wrap(mem);
    Assert.assertEquals(i2.getResult().getEstimate(), 1.0);

    boolean thrown = false;
    try {
      i2.update(us1);
    } catch (SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
