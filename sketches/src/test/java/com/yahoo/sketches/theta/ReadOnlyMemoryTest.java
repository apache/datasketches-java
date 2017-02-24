/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.AllocMemory;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.memory.ReadOnlyMemoryException;

public class ReadOnlyMemoryTest {

  @Test
  public void wrapAndTryUpdatingUpdateSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.toByteArray()).asReadOnlyBuffer());
    UpdateSketch sketch = (UpdateSketch) Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);

    boolean thrown = false;
    try {
      sketch.update(2);
    } catch (ReadOnlyMemoryException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);

  }

  @Test
  public void wrapCompactUnorderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void wrapCompactOrderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.wrap(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyUpdateSketch() {
    UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(us1.toByteArray()).asReadOnlyBuffer());
    UpdateSketch us2 = (UpdateSketch) Sketch.heapify(mem);
    us2.update(2);
    assertEquals(us2.getEstimate(), 2.0);
  }

  @Test
  public void heapifyCompactUnorderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyCompactOrderedSketch() {
    UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(updateSketch.compact().toByteArray()).asReadOnlyBuffer());
    Sketch sketch = Sketch.heapify(mem);
    assertEquals(sketch.getEstimate(), 1.0);
  }

  @Test
  public void heapifyUnion() {
    Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(u1.toByteArray()).asReadOnlyBuffer());
    Union u2 = (Union) SetOperation.heapify(mem);
    u2.update(2);
    Assert.assertEquals(u2.getResult().getEstimate(), 2.0);
  }

  @Test
  public void wrapAndTryUpdatingUnion() {
    Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(u1.toByteArray()).asReadOnlyBuffer());
    Union u2 = (Union) SetOperation.wrap(mem);
    Assert.assertEquals(u2.getResult().getEstimate(), 1.0);

    boolean thrown = false;
    try {
      u2.update(2);
    } catch (ReadOnlyMemoryException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(i1.toByteArray()).asReadOnlyBuffer());
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
    Memory mem = NativeMemory.wrap(ByteBuffer.wrap(i1.toByteArray()).asReadOnlyBuffer());
    Intersection i2 = (Intersection) SetOperation.wrap(mem);
    Assert.assertEquals(i2.getResult().getEstimate(), 1.0);

    boolean thrown = false;
    try {
      i2.update(us1);
    } catch (ReadOnlyMemoryException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void checkHeapQuickSelect() {
    int k = 16;
    int u = k;

    //Heap Writable Memory
    UpdateSketch srcSk = UpdateSketch.builder().build(k);
    for (int i = 0; i < u; i++) { srcSk.update(i); }
    byte[] arr = srcSk.toByteArray();

    Memory mem = new NativeMemory(arr);
    Sketch tgtSk = Sketches.heapifySketch(mem);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Heap Read-Only Memory
    Memory memRO = mem.asReadOnlyMemory();
    tgtSk = Sketches.heapifySketch(memRO);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Direct Writable Memory
    int bytes = Sketch.getMaxUpdateSketchBytes(k);
    Memory memD = new AllocMemory(bytes);
    UpdateSketch srcSkD = UpdateSketch.builder().initMemory(memD).build(k);
    for (int i = 0; i < u; i++) { srcSkD.update(i); }

    tgtSk = Sketches.heapifySketch(memD);
    assertEquals(tgtSk.getEstimate(), (double)u);

    //Direct Read-Only Memory
    Memory memDRO = mem.asReadOnlyMemory();
    tgtSk = Sketches.heapifySketch(memDRO);
    assertEquals(tgtSk.getEstimate(), (double)u);
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
