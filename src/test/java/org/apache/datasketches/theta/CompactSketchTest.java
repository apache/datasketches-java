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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

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
        assertTrue(testSk.isOrdered());
      } else {
        assertFalse(testSk.isOrdered());
      }
      if (refSk.isDirect()) {
        assertTrue(testSk.isDirect());
      } else {
        assertFalse(testSk.isDirect());
      }
    } else {
      assertFalse(testSk.hasMemory());
      assertTrue(testSk instanceof HeapCompactSketch);
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
    long[] result = IntersectionImplR.compactCachePart(null, 4, 0, 0L, false);
    assertEquals(result.length, 0);
  }

  @Test
  public void checkDirectCompactSingleItemSketch() {
    State state;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    CompactSketch csko; //ordered
    CompactSketch csku; //unordered

    WritableMemory wmem = WritableMemory.allocate(16);
    csko = sk.compact(true, wmem); //empty, direct, ordered
    //ClassType, Count, Bytes, Compact, Empty, Direct, Memory, Ordered, Estimation
    state = new State("DirectCompactSketch", 0, 8, true, true, false, true, true, false);
    state.check(csko);

    wmem = WritableMemory.allocate(16);
    csku = sk.compact(false, wmem); //empty, direct, unordered
    state = new State("DirectCompactSketch", 0, 8, true, true, false, true, true, false);
    state.check(csku);

    sk.update(1);
    wmem = WritableMemory.allocate(16);
    csko = sk.compact(true, wmem); //Single, direct, ordered
    state = new State("DirectCompactSketch", 1, 16, true, false, false, true, true, false);
    state.check(csko);

    wmem = WritableMemory.allocate(16);
    csku = sk.compact(false, wmem); //Single, direct, unordered
    state = new State("DirectCompactSketch", 1, 16, true, false, false, true, true, false);
    state.check(csku);

    CompactSketch csk2o; //ordered
    CompactSketch csk2u; //unordered

    csk2o = csku.compact(); //single, heap, ordered
    state = new State("SingleItemSketch", 1, 16, true, false, false, false, true, false);
    state.check(csk2o);

    csk2o = csku.compact(true, null); //single, heap, ordered
    state.check(csk2o);

    csk2o = csku.compact(false, null); //single, heap, ordered
    state.check(csk2o);

    csk2o = csko.compact(true, null); //single, heap, ordered
    state.check(csk2o);

    csk2o = csko.compact(false, null); //single, heap, ordered
    state.check(csk2o);

    wmem = WritableMemory.allocate(16);
    csk2o = csku.compact(true, wmem);
    state.classType = "DirectCompactSketch";
    state.memory = true;
    state.check(csk2o);

    wmem = WritableMemory.allocate(16);
    csk2u = csku.compact(false, wmem);
    state.classType = "DirectCompactSketch";
    state.check(csk2u);

    wmem = WritableMemory.allocate(16);
    csk2o = csko.compact(true, wmem);
    state.classType = "DirectCompactSketch";
    state.memory = true;
    state.check(csk2o);

    wmem = WritableMemory.allocate(16);
    csk2u = csko.compact(false, wmem);
    state.classType = "DirectCompactSketch";
    state.check(csk2u);
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
    WritableMemory wmem = WritableMemory.allocate(16); //empty, but extra bytes
    CompactSketch csk = sk.compact(false, wmem); //ignores order because it is empty
    assertTrue(csk instanceof DirectCompactSketch);
    Sketch csk2 = Sketch.heapify(wmem);
    assertTrue(csk2 instanceof EmptyCompactSketch);
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

  private static class State {
    String classType = null;
    int count = 0;
    int bytes = 0;
    boolean compact = false;
    boolean empty = false;
    boolean direct = false;
    boolean memory = false;
    boolean ordered = false;
    boolean estimation = false;


    State(String classType, int count, int bytes, boolean compact, boolean empty, boolean direct,
        boolean memory, boolean ordered, boolean estimation) {
      this.classType = classType;
      this.count = count;
      this.bytes = bytes;
      this.compact = compact;
      this.empty = empty;
      this.direct = direct;
      this.memory = memory;
      this.ordered = ordered;
      this.estimation = estimation;
    }

    void check(CompactSketch csk) {
      assertEquals(csk.getClass().getSimpleName(), classType, "ClassType");
      assertEquals(csk.getRetainedEntries(), count, "curCount");
      assertEquals(csk.getCurrentBytes(true), bytes, "Bytes" );
      assertEquals(csk.isCompact(), compact, "Compact");
      assertEquals(csk.isEmpty(), empty, "Empty");
      assertEquals(csk.isDirect(), direct, "Direct");
      assertEquals(csk.hasMemory(), memory, "Memory");
      assertEquals(csk.isOrdered(), ordered, "Ordered");
      assertEquals(csk.isEstimationMode(), estimation, "Estimation");
    }
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
