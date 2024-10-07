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

import static org.apache.datasketches.quantiles.PreambleUtil.COMBINED_BUFFER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteOrder;

import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The concept for these tests is that the "MemoryManager" classes below are proxies for the
 * implementation that <i>owns</i> the native memory allocations, thus is responsible for
 * allocating larger Memory when requested and the actual freeing of the old memory allocations.
 */
public class DirectQuantilesMemoryRequestTest {

  @Test
  public void checkLimitedMemoryScenarios() { //Requesting application
    final int k = 128;
    final int u = 40 * k;
    final int initBytes = ((2 * k) + 4) << 3; //just the BB

    //########## Owning Implementation
    // This part would actually be part of the Memory owning implementation so it is faked here
    WritableMemory wmem = WritableMemory.allocateDirect(initBytes, ByteOrder.nativeOrder(), new DefaultMemoryRequestServer());
    WritableMemory wmemCopy = wmem;
    println("Initial mem size: " + wmem.getCapacity());

    //########## Receiving Application
    // The receiving application has been given wmem to use for a sketch,
    // but alas, it is not ultimately large enough.
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wmem);
    assertTrue(usk1.isEmpty());

    //Load the sketch
    for (int i = 0; i < u; i++) {
      // The sketch uses The MemoryRequest, acquired from wmem, to acquire more memory as
      // needed, and requests via the MemoryRequest to free the old allocations.
      usk1.update(i);
    }
    final double result = usk1.getQuantile(0.5);
    println("Result: " + result);
    assertEquals(result, u / 2.0, 0.05 * u); //Success

    //The actual Memory has been re-allocated several times,
    // so the the wmem reference is invalid. Use the sketch to get the last memory reference.
    WritableMemory lastMem = usk1.getMemory();
    println("Final mem size: " + usk1.getMemory().getCapacity());
    assertTrue(wmemCopy.isDirect());
    assertFalse(wmemCopy.isAlive());
  }

  @Test
  public void checkGrowBaseBuf() {
    final int k = 128;
    final int u = 32; // don't need the BB to fill here
    final int initBytes = (4 + (u / 2)) << 3; // not enough to hold everything

    WritableMemory wmem = WritableMemory.allocateDirect(initBytes, ByteOrder.nativeOrder(), new DefaultMemoryRequestServer());
    WritableMemory wmemCopy = wmem;
    println("Initial mem size: " + wmem.getCapacity());
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wmem);
    for (int i = 1; i <= u; i++) {
      usk1.update(i);
    }
    final int currentSpace = usk1.getCombinedBufferItemCapacity();
    println("curCombBufItemCap: " + currentSpace);
    assertEquals(currentSpace, 2 * k);
    println("last Mem Cap: " + usk1.getMemory().getCapacity());
    assertTrue(wmemCopy.isDirect());
    assertFalse(wmemCopy.isAlive());
  }

  @Test
  public void checkGrowCombBuf() {
    final int k = 128;
    final int u = (2 * k) - 1; //just to fill the BB
    final int initBytes = ((2 * k) + 4) << 3; //just room for BB

    WritableMemory wmem = WritableMemory.allocateDirect(initBytes, ByteOrder.nativeOrder(), new DefaultMemoryRequestServer());
    WritableMemory wmemCopy = wmem;
    println("Initial mem size: " + wmem.getCapacity());
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build(wmem);
    for (int i = 1; i <= u; i++) {
      usk1.update(i);
    }
    final int currentSpace = usk1.getCombinedBufferItemCapacity();
    println("curCombBufItemCap: " + currentSpace);
    final double[] newCB = usk1.growCombinedBuffer(currentSpace, 3 * k);
    final int newSpace = usk1.getCombinedBufferItemCapacity();
    println("newCombBurItemCap: " + newSpace);
    assertEquals(newCB.length, 3 * k);
    assertTrue(wmemCopy.isDirect());
    assertFalse(wmemCopy.isAlive());
  }

  @Test
  public void checkGrowFromWrappedEmptySketch() {
    final int k = 16;
    final int n = 0;
    final int initBytes = DoublesSketch.getUpdatableStorageBytes(k, n); //8 bytes
    final UpdateDoublesSketch usk1 = DoublesSketch.builder().setK(k).build();
    final Memory origSketchMem = Memory.wrap(usk1.toByteArray());

    WritableMemory wmem = WritableMemory.allocateDirect(initBytes, ByteOrder.nativeOrder(), new DefaultMemoryRequestServer());
    WritableMemory wmemCopy = wmem;
    origSketchMem.copyTo(0, wmem, 0, initBytes);
    UpdateDoublesSketch usk2 = DirectUpdateDoublesSketch.wrapInstance(wmem);
    assertTrue(wmem.isSameResource(usk2.getMemory()));
    assertEquals(wmem.getCapacity(), initBytes);
    assertTrue(wmem.isDirect());
    assertTrue(usk2.isEmpty());

    //update the sketch forcing it to grow on-heap
    for (int i = 1; i <= 5; i++) { usk2.update(i); }
    assertEquals(usk2.getN(), 5);
    WritableMemory mem2 = usk2.getMemory();
    assertFalse(wmem.isAlive()); //
    assertFalse(mem2.isDirect()); //should now be on-heap

    final int expectedSize = COMBINED_BUFFER + ((2 * k) << 3);
    assertEquals(mem2.getCapacity(), expectedSize);
    assertTrue(wmemCopy.isDirect());
    assertFalse(wmemCopy.isAlive());
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
