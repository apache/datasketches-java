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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteOrder;

import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DoublesSketchTest {

  @Test
  public void heapToDirect() {
    UpdateDoublesSketch heapSketch = DoublesSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i);
    }
    DoublesSketch directSketch = DoublesSketch.wrap(WritableMemory.writableWrap(heapSketch.toByteArray(false)));

    assertEquals(directSketch.getMinValue(), 0.0);
    assertEquals(directSketch.getMaxValue(), 999.0);
    assertEquals(directSketch.getQuantile(0.5), 500.0, 4.0);
  }

  @Test
  public void directToHeap() {
    int sizeBytes = 10000;
    UpdateDoublesSketch directSketch = DoublesSketch.builder().build(WritableMemory.writableWrap(new byte[sizeBytes]));
    for (int i = 0; i < 1000; i++) {
      directSketch.update(i);
    }
    UpdateDoublesSketch heapSketch;
    heapSketch = (UpdateDoublesSketch) DoublesSketch.heapify(WritableMemory.writableWrap(directSketch.toByteArray()));
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i + 1000);
    }
    assertEquals(heapSketch.getMinValue(), 0.0);
    assertEquals(heapSketch.getMaxValue(), 1999.0);
    assertEquals(heapSketch.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void checkToByteArray() {
    UpdateDoublesSketch ds = DoublesSketch.builder().build(); //k = 128
    ds.update(1);
    ds.update(2);
    byte[] arr = ds.toByteArray(false);
    assertEquals(arr.length, ds.getUpdatableStorageBytes());
  }

  /**
   * Checks 2 DoublesSketches for equality, triggering an assert if unequal. Handles all
   * input sketches and compares only values on valid levels, allowing it to be used to compare
   * Update and Compact sketches.
   * @param sketch1 input sketch 1
   * @param sketch2 input sketch 2
   */
  static void testSketchEquality(final DoublesSketch sketch1,
                                 final DoublesSketch sketch2) {
    assertEquals(sketch1.getK(), sketch2.getK());
    assertEquals(sketch1.getN(), sketch2.getN());
    assertEquals(sketch1.getBitPattern(), sketch2.getBitPattern());
    assertEquals(sketch1.getMinValue(), sketch2.getMinValue());
    assertEquals(sketch1.getMaxValue(), sketch2.getMaxValue());

    final DoublesSketchAccessor accessor1 = DoublesSketchAccessor.wrap(sketch1);
    final DoublesSketchAccessor accessor2 = DoublesSketchAccessor.wrap(sketch2);

    // Compare base buffers. Already confirmed n and k match.
    for (int i = 0; i < accessor1.numItems(); ++i) {
      assertEquals(accessor1.get(i), accessor2.get(i));
    }

    // Iterate over levels comparing items
    long bitPattern = sketch1.getBitPattern();
    for (int lvl = 0; bitPattern != 0; ++lvl, bitPattern >>>= 1) {
      if ((bitPattern & 1) > 0) {
        accessor1.setLevel(lvl);
        accessor2.setLevel(lvl);
        for (int i = 0; i < accessor1.numItems(); ++i) {
          assertEquals(accessor1.get(i), accessor2.get(i));
        }
      }
    }
  }

  @Test
  public void checkIsSameResource() {
    int k = 16;
    WritableMemory mem = WritableMemory.writableWrap(new byte[(k*16) +24]);
    WritableMemory cmem = WritableMemory.writableWrap(new byte[8]);
    DirectUpdateDoublesSketch duds =
            (DirectUpdateDoublesSketch) DoublesSketch.builder().setK(k).build(mem);
    assertTrue(duds.isSameResource(mem));
    DirectCompactDoublesSketch dcds = (DirectCompactDoublesSketch) duds.compact(cmem);
    assertTrue(dcds.isSameResource(cmem));

    UpdateDoublesSketch uds = DoublesSketch.builder().setK(k).build();
    assertFalse(uds.isSameResource(mem));
  }

  @Test
  public void checkEmptyNullReturns() {
    int k = 16;
    UpdateDoublesSketch uds = DoublesSketch.builder().setK(k).build();
    assertNull(uds.getQuantiles(5));
    assertNull(uds.getPMF(new double[] { 0, 0.5, 1.0 }));
    assertNull(uds.getCDF(new double[] { 0, 0.5, 1.0 }));
  }

  @Test
  public void directSketchShouldMoveOntoHeapEventually() {
    try (WritableHandle wdh = WritableMemory.allocateDirect(1000,
            ByteOrder.nativeOrder(), new DefaultMemoryRequestServer())) {
      WritableMemory mem = wdh.getWritable();
      UpdateDoublesSketch sketch = DoublesSketch.builder().build(mem);
      Assert.assertTrue(sketch.isSameResource(mem));
      for (int i = 0; i < 1000; i++) {
        sketch.update(i);
      }
      Assert.assertFalse(sketch.isSameResource(mem));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void directSketchShouldMoveOntoHeapEventually2() {
    int i = 0;
    try (WritableHandle wdh =
        WritableMemory.allocateDirect(50, ByteOrder.LITTLE_ENDIAN, new DefaultMemoryRequestServer())) {
      WritableMemory mem = wdh.getWritable();
      UpdateDoublesSketch sketch = DoublesSketch.builder().build(mem);
      Assert.assertTrue(sketch.isSameResource(mem));
      for (; i < 1000; i++) {
        if (sketch.isSameResource(mem)) {
          sketch.update(i);
        } else {
          //println("MOVED OUT at i = " + i);
          break;
        }
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkEmptyDirect() {
    try (WritableHandle wdh = WritableMemory.allocateDirect(1000)) {
      WritableMemory mem = wdh.getWritable();
      UpdateDoublesSketch sketch = DoublesSketch.builder().build(mem);
      sketch.toByteArray(); //exercises a specific path
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
