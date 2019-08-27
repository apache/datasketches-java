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

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CompactSketchTest {

  @Test
  public void checkHeapifyWrap() {
    int k = 4096;
    final boolean ordered = true;
    checkHeapifyWrap(k, 0, ordered);
    checkHeapifyWrap(k, 1, ordered);
    checkHeapifyWrap(k, 1, !ordered);
    checkHeapifyWrap(k, k, ordered);  //exact
    checkHeapifyWrap(k, k, !ordered); //exact
    checkHeapifyWrap(k, 4 * k, ordered); //estimating
    checkHeapifyWrap(k, 4 * k, !ordered); //estimating
  }

  //test combinations of compact ordered/not ordered and heap/direct
  public void checkHeapifyWrap(int k, int u, boolean ordered) {
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) { //populate update sketch
      usk.update(i);
    }

    /****ON HEAP MEMORY -- HEAPIFY****/
    CompactSketch refSk = usk.compact(ordered, null);
    byte[] barr = refSk.toByteArray();
    Memory srcMem = Memory.wrap(barr);
    CompactSketch testSk = (CompactSketch) Sketch.heapify(srcMem);

    checkByRange(refSk, testSk, u, ordered);

    /**Via byte[]**/
    byte[] byteArray = refSk.toByteArray();
    Memory heapROMem = Memory.wrap(byteArray);
    testSk = (CompactSketch)Sketch.heapify(heapROMem);

    checkByRange(refSk, testSk, u, ordered);

    /****OFF HEAP MEMORY -- WRAP****/
    //Prepare Memory for direct
    int bytes = usk.getCurrentBytes(true); //for Compact

    try (WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes)) {
      WritableMemory directMem = wdh.get();

      /**Via CompactSketch.compact**/
      refSk = usk.compact(ordered, directMem);
      testSk = (CompactSketch)Sketch.wrap(directMem);

      checkByRange(refSk, testSk, u, ordered);

      /**Via CompactSketch.compact**/
      testSk = (CompactSketch)Sketch.wrap(directMem);
      checkByRange(refSk, testSk, u, ordered);
    }
  }

  private static void checkByRange(Sketch refSk, Sketch testSk, int u, boolean ordered) {
    if (u == 0) {
      checkEmptySketch(testSk);
    } else if (u == 1) {
      checkSingleItemSketch(testSk, refSk);
    } else {
      checkOtherCompactSketch(testSk, refSk, ordered);
    }
  }

  private static void checkEmptySketch(Sketch testSk) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertTrue(testSk instanceof EmptyCompactSketch);
    assertTrue(testSk.isEmpty());
    assertTrue(testSk.isOrdered());
    assertNull(testSk.getMemory());
    assertFalse(testSk.isDirect());
    assertFalse(testSk.hasMemory());
    assertEquals(testSk.getSeedHash(), 0);
    assertEquals(testSk.getRetainedEntries(true), 0);
    assertEquals(testSk.getEstimate(), 0.0, 0.0);
    assertEquals(testSk.getCurrentBytes(true), 8);
    assertNotNull(testSk.iterator());
    assertEquals(testSk.toByteArray().length, 8);
    assertEquals(testSk.getCache().length, 0);
    assertEquals(testSk.getCurrentPreambleLongs(true), 1);
  }

  private static void checkSingleItemSketch(Sketch testSk, Sketch refSk) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertTrue(testSk instanceof SingleItemSketch);
    assertFalse(testSk.isEmpty());
    assertTrue(testSk.isOrdered());
    assertNull(testSk.getMemory());
    assertFalse(testSk.isDirect());
    assertFalse(testSk.hasMemory());
    assertEquals(testSk.getSeedHash(), refSk.getSeedHash());
    assertEquals(testSk.getRetainedEntries(true), 1);
    assertEquals(testSk.getEstimate(), 1.0, 0.0);
    assertEquals(testSk.getCurrentBytes(true), 16);
    assertNotNull(testSk.iterator());
    assertEquals(testSk.toByteArray().length, 16);
    assertEquals(testSk.getCache().length, 1);
    assertEquals(testSk.getCurrentPreambleLongs(true), 1);
  }

  private static void checkOtherCompactSketch(Sketch testSk, Sketch refSk, boolean ordered) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertFalse(testSk.isEmpty());
    assertNotNull(testSk.iterator());
    assertEquals(testSk.isOrdered(), ordered);
    if (refSk.hasMemory()) {
      assertTrue(testSk.hasMemory());
      assertNotNull(testSk.getMemory());
      if (ordered) {
        assertTrue(testSk instanceof DirectCompactOrderedSketch);
      } else {
        assertTrue(testSk instanceof DirectCompactUnorderedSketch);
      }
      if (refSk.isDirect()) {
        assertTrue(testSk.isDirect());
      } else {
        assertFalse(testSk.isDirect());
      }
    } else {
      assertFalse(testSk.hasMemory());
      if (ordered) {
        assertTrue(testSk instanceof HeapCompactOrderedSketch);
      } else {
        assertTrue(testSk instanceof HeapCompactUnorderedSketch);
      }
    }
    assertEquals(testSk.getSeedHash(), refSk.getSeedHash());
    assertEquals(testSk.getRetainedEntries(true), refSk.getRetainedEntries());
    assertEquals(testSk.getEstimate(), refSk.getEstimate(), 0.0);
    assertEquals(testSk.getCurrentBytes(true), refSk.getCurrentBytes(true));
    assertEquals(testSk.toByteArray().length, refSk.toByteArray().length);
    assertEquals(testSk.getCache().length, refSk.getCache().length);
    assertEquals(testSk.getCurrentPreambleLongs(true), refSk.getCurrentPreambleLongs(true));
  }

  @Test
  public void checkDirectSingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    int bytes = sk.getCurrentBytes(true);
    WritableMemory wmem = WritableMemory.allocate(bytes);
    sk.compact(true, wmem);
    Sketch csk2 = Sketch.heapify(wmem);
    assertTrue(csk2 instanceof SingleItemSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmall() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = false;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    WritableMemory mem = WritableMemory.wrap(byteArray);
    usk.compact(ordered, mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmallOrdered() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = true;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    WritableMemory mem = WritableMemory.wrap(byteArray);
    usk.compact(ordered, mem);
  }

  @Test
  public void checkCompactCachePart() {
    //phony values except for curCount = 0.
    long[] result = CompactSketch.compactCachePart(null, 4, 0, 0L, false);
    assertEquals(result.length, 0);
  }

  @Test
  public void checkDirectCompactSingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    CompactSketch csk = sk.compact(true, WritableMemory.allocate(16));
    int bytes = csk.getCurrentBytes(true);
    assertEquals(bytes, 8);
    sk.update(1);
    csk = sk.compact(true, WritableMemory.allocate(16));
    bytes = csk.getCurrentBytes(true);
    assertEquals(bytes, 16);
    assertTrue(csk == csk.compact());
    assertTrue(csk == csk.compact(true, null));
  }

  @Test
  public void checkHeapifySingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    int bytes = Sketches.getMaxCompactSketchBytes(2); //1 more than needed
    WritableMemory wmem = WritableMemory.allocate(bytes);
    sk.compact(false, wmem);
    Sketch csk = Sketch.heapify(wmem);
    assertTrue(csk instanceof SingleItemSketch);
  }

  @Test
  public void checkHeapifyEmptySketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    WritableMemory wmem = WritableMemory.allocate(16); //extra bytes
    sk.compact(false, wmem);
    PreambleUtil.clearEmpty(wmem); //corrupt to simulate missing empty flag
    Sketch csk = Sketch.heapify(wmem);
    assertTrue(csk instanceof EmptyCompactSketch);
  }

  @Test
  public void checkGetCache() {
    UpdateSketch sk = Sketches.updateSketchBuilder().setP((float).5).build();
    sk.update(7);
    int bytes = sk.getCurrentBytes(true);
    CompactSketch csk = sk.compact(true, WritableMemory.allocate(bytes));
    long[] cache = csk.getCache();
    assertTrue(cache.length == 0);
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
