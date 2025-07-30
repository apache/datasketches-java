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
import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * The concept for these tests is that the "MemorySegment Manager" classes below are proxies for the
 * implementation that <i>owns</i> the native MemorySegment allocations, thus is responsible for
 * allocating larger MemorySegment when requested and the actual freeing of the old MemorySegment allocations.
 */
public class DirectQuantilesMemoryRequestTest {

  @Test
  public void checkLimitedMemoryScenarios() { //Requesting application
    final int k = 128;
    final int u = 40 * k;
    final int initBytes = ((2 * k) + 4) << 3; //just the BaseBuffer

    //########## Owning Implementation
    // This part would actually be part of the application owning the MemorySegment so it is faked here
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(initBytes);
      println("Initial seg size: " + wseg.byteSize());

      //########## Receiving Application
      // The receiving application has been given wseg to use for a sketch,
      // but alas, it is not ultimately large enough.
      final UpdateDoublesSketch usk = DoublesSketch.builder().setK(k).build(wseg);
      assertTrue(usk.isEmpty());

      //Load the sketch
      for (int i = 0; i < u; i++) {
        // The sketch uses The MemorySegmentRequestto acquire more MemorySegment as needed,
        //  which, for the default, the new MemorySegments will be on-heap.
        usk.update(i);
      }
      final double result = usk.getQuantile(0.5);
      println("Result: " + result);
      assertEquals(result, u / 2.0, 0.05 * u); //Success

      //########## Owning Implementation
      //The actual MemorySegment has been re-allocated several times,
      // so the sketch is using a different object.
      final MemorySegment wseg2 = usk.getMemorySegment();
      println("\nFinal seg size: " + wseg2.byteSize());
    }
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkGrowBaseBuf() {
    final int k = 128;
    final int u = 32; // don't need the BB to fill here
    final int initBytes = (4 + (u / 2)) << 3; // not enough to hold everything
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(initBytes);
      println("Initial seg size: " + wseg.byteSize());
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wseg);
      for (int i = 1; i <= u; i++) {
        usk1.update(i);
      }
      final int currentSpace = usk1.getCombinedBufferItemCapacity();
      println("curCombBufItemCap: " + currentSpace);
      assertEquals(currentSpace, 2 * k);
      assertTrue(wseg.scope().isAlive());
    }
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkGrowCombBuf() {
    final int k = 128;
    final int u = (2 * k) - 1; //just to fill the BB
    final int initBytes = ((2 * k) + 4) << 3; //just room for BB
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(initBytes);
      println("Initial seg size: " + wseg.byteSize());
      final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wseg);
      for (int i = 1; i <= u; i++) {
        usk1.update(i);
      }
      final int currentSpace = usk1.getCombinedBufferItemCapacity();
      println("curCombBufItemCap: " + currentSpace);
      final double[] newCB = usk1.growCombinedBuffer(currentSpace, 3 * k);
      final int newSpace = usk1.getCombinedBufferItemCapacity();
      println("newCombBurItemCap: " + newSpace);
      assertEquals(newCB.length, 3 * k);
      assertTrue(wseg.scope().isAlive());
    }
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkUpdatableStorageBytes() {
    final int k = 16;
    final int initBytes = DoublesSketch.getUpdatableStorageBytes(k, 1);
    println("Predicted Updatable Storage Bytes: " + initBytes);
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build();
    usk1.update(1.0);
    final byte[] uarr = usk1.toByteArray();
    println("Actual Storage Bytes " + uarr.length);
    assertEquals(initBytes, uarr.length);
    assertEquals(initBytes, 64);
  }


  @Test
  public void checkGrowFromWrappedEmptySketch() {
    final int k = 16;
    final int n = 0;
    final int initBytes = DoublesSketch.getUpdatableStorageBytes(k, n); //empty: 8 bytes
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build();
    final MemorySegment origSketchSeg = MemorySegment.ofArray(usk1.toByteArray()); //on heap
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(initBytes); //off heap
      MemorySegment.copy(origSketchSeg, 0, wseg, 0, initBytes);
      final UpdateDoublesSketch usk2 = DirectUpdateDoublesSketch.wrapInstance(wseg, null);
      assertTrue(isSameResource(wseg, usk2.getMemorySegment()));
      assertEquals(wseg.byteSize(), initBytes);
      assertTrue(wseg.isNative());
      assertTrue(usk2.isEmpty());

      //update the sketch forcing it to grow on-heap
      usk2.update(1.0);
      assertEquals(usk2.getN(), 1);
      final MemorySegment seg2 = usk2.getMemorySegment();
      assertFalse(isSameResource(wseg, seg2));
      assertFalse(seg2.isNative()); //should now be on-heap

      final int expectedSize = COMBINED_BUFFER + ((2 * k) << 3);
      assertEquals(seg2.byteSize(), expectedSize);
      assertTrue(wseg.scope().isAlive());
    }
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
