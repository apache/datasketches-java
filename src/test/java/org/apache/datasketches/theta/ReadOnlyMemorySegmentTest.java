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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.common.SketchesReadOnlyException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReadOnlyMemorySegmentTest {

  @Test
  public void wrapAndTryUpdatingUpdateSketch() {
    final UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(updateSketch.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final UpdateSketch sketch = (UpdateSketch) Sketch.wrap(seg);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());

    boolean thrown = false;
    try {
      sketch.update(2);
    } catch (final SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
  }

  @Test
  public void wrapCompactUnorderedSketch() {
    final UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Sketch sketch = Sketch.wrap(seg);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void wrapCompactOrderedSketch() {
    final UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(ByteBuffer.wrap(updateSketch.compact().toByteArray())
        .asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Sketch sketch = Sketch.wrap(seg);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void heapifyUpdateSketch() {
    final UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(us1.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    // downcasting is not recommended, for testing only
    final UpdateSketch us2 = (UpdateSketch) Sketch.heapify(seg);
    us2.update(2);
    assertEquals(us2.getEstimate(), 2.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void heapifyCompactUnorderedSketch() {
    final UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(updateSketch.compact(false, null).toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Sketch sketch = Sketch.heapify(seg);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void heapifyCompactOrderedSketch() {
    final UpdateSketch updateSketch = UpdateSketch.builder().build();
    updateSketch.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(updateSketch.compact().toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Sketch sketch = Sketch.heapify(seg);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void heapifyUnion() {
    final Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(u1.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Union u2 = (Union) SetOperation.heapify(seg);
    u2.update(2);
    Assert.assertEquals(u2.getResult().getEstimate(), 2.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void wrapAndTryUpdatingUnion() {
    final Union u1 = SetOperation.builder().buildUnion();
    u1.update(1);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(u1.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));

    final Union u2 = (Union) SetOperation.wrap(seg);
    final Union u3 = Union.wrap(seg);
    Assert.assertEquals(u2.getResult().getEstimate(), 1.0);
    Assert.assertEquals(u3.getResult().getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());

    try {
      u2.update(2);
      fail();
    } catch (final SketchesReadOnlyException e) {
      //expected
    }

    try {
      u3.update(2);
      fail();
    } catch (final SketchesReadOnlyException e) {
      //expected
    }
  }

  @Test
  public void heapifyIntersection() {
    final UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    us1.update(2);
    final UpdateSketch us2 = UpdateSketch.builder().build();
    us2.update(2);
    us2.update(3);

    final Intersection i1 = SetOperation.builder().buildIntersection();
    i1.intersect(us1);
    i1.intersect(us2);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(i1.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Intersection i2 = (Intersection) SetOperation.heapify(seg);
    i2.intersect(us1);
    Assert.assertEquals(i2.getResult().getEstimate(), 1.0);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void wrapIntersection() {
    final UpdateSketch us1 = UpdateSketch.builder().build();
    us1.update(1);
    us1.update(2);
    final UpdateSketch us2 = UpdateSketch.builder().build();
    us2.update(2);
    us2.update(3);

    final Intersection i1 = SetOperation.builder().buildIntersection();
    i1.intersect(us1);
    i1.intersect(us2);
    final MemorySegment seg = MemorySegment.ofBuffer(
        ByteBuffer.wrap(i1.toByteArray()).asReadOnlyBuffer().order(ByteOrder.nativeOrder()));
    final Intersection i2 = (Intersection) SetOperation.wrap(seg);
    Assert.assertEquals(i2.getResult().getEstimate(), 1.0);

    boolean thrown = false;
    try {
      i2.intersect(us1);
    } catch (final SketchesReadOnlyException e) {
      thrown = true;
    }
    Assert.assertTrue(thrown);
    assertTrue(seg.isReadOnly());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
