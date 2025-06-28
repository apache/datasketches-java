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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.DirectCompactSketch;
import org.apache.datasketches.theta.EmptyCompactSketch;
import org.apache.datasketches.theta.HashIterator;
import org.apache.datasketches.theta.HeapCompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SingleItemSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.WrappedCompactCompressedSketch;
import org.apache.datasketches.theta.WrappedCompactSketch;
import org.testng.annotations.Test;

import java.lang.foreign.Arena;

/**
 * @author Lee Rhodes
 */
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

    /****ON HEAP MemorySegment -- HEAPIFY****/
    CompactSketch refSk = usk.compact(ordered, null);
    byte[] barr = refSk.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(barr);
    CompactSketch testSk = (CompactSketch) Sketch.heapify(srcSeg);

    checkByRange(refSk, testSk, u, ordered);

    /**Via byte[]**/
    byte[] byteArray = refSk.toByteArray();
    MemorySegment heapROSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    testSk = (CompactSketch)Sketch.heapify(heapROSeg);

    checkByRange(refSk, testSk, u, ordered);

    /****OFF HEAP MemorySegment -- WRAP****/
    //Prepare MemorySegment for direct
    int bytes = usk.getCompactBytes(); //for Compact

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment directSeg = arena.allocate(bytes);

      /**Via CompactSketch.compact**/
      refSk = usk.compact(ordered, directSeg);
      testSk = (CompactSketch)Sketch.wrap(directSeg);

      checkByRange(refSk, testSk, u, ordered);

      /**Via CompactSketch.compact**/
      testSk = (CompactSketch)Sketch.wrap(directSeg);
      checkByRange(refSk, testSk, u, ordered);
    } catch (final Exception e) {
      throw new RuntimeException(e);
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
    assertNull(testSk.getMemorySegment());
    assertFalse(testSk.isDirect());
    assertFalse(testSk.hasMemorySegment());
    assertEquals(testSk.getSeedHash(), 0);
    assertEquals(testSk.getRetainedEntries(true), 0);
    assertEquals(testSk.getEstimate(), 0.0, 0.0);
    assertEquals(testSk.getCurrentBytes(), 8);
    assertNotNull(testSk.iterator());
    assertEquals(testSk.toByteArray().length, 8);
    assertEquals(testSk.getCache().length, 0);
    assertEquals(testSk.getCompactPreambleLongs(), 1);
  }

  private static void checkSingleItemSketch(Sketch testSk, Sketch refSk) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertTrue(testSk instanceof SingleItemSketch);
    assertFalse(testSk.isEmpty());
    assertTrue(testSk.isOrdered());
    assertNull(testSk.getMemorySegment());
    assertFalse(testSk.isDirect());
    assertFalse(testSk.hasMemorySegment());
    assertEquals(testSk.getSeedHash(), refSk.getSeedHash());
    assertEquals(testSk.getRetainedEntries(true), 1);
    assertEquals(testSk.getEstimate(), 1.0, 0.0);
    assertEquals(testSk.getCurrentBytes(), 16);
    assertNotNull(testSk.iterator());
    assertEquals(testSk.toByteArray().length, 16);
    assertEquals(testSk.getCache().length, 1);
    assertEquals(testSk.getCompactPreambleLongs(), 1);
  }

  private static void checkOtherCompactSketch(Sketch testSk, Sketch refSk, boolean ordered) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertFalse(testSk.isEmpty());
    assertNotNull(testSk.iterator());
    assertEquals(testSk.isOrdered(), ordered);
    if (refSk.hasMemorySegment()) {
      assertTrue(testSk.hasMemorySegment());
      assertNotNull(testSk.getMemorySegment());
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
      assertFalse(testSk.hasMemorySegment());
      assertTrue(testSk instanceof HeapCompactSketch);
    }
    assertEquals(testSk.getSeedHash(), refSk.getSeedHash());
    assertEquals(testSk.getRetainedEntries(true), refSk.getRetainedEntries(true));
    assertEquals(testSk.getEstimate(), refSk.getEstimate(), 0.0);
    assertEquals(testSk.getCurrentBytes(), refSk.getCurrentBytes());
    assertEquals(testSk.toByteArray().length, refSk.toByteArray().length);
    assertEquals(testSk.getCache().length, refSk.getCache().length);
    assertEquals(testSk.getCompactPreambleLongs(), refSk.getCompactPreambleLongs());
  }

  @Test
  public void checkDirectSingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    int bytes = sk.getCompactBytes();
    MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    sk.compact(true, wseg);
    Sketch csk2 = Sketch.heapify(wseg);
    assertTrue(csk2 instanceof SingleItemSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmall() {
    int k = 512;
    int u = k;
    boolean ordered = false;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCompactBytes();
    byte[] byteArray = new byte[bytes -8]; //too small
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    usk.compact(ordered, seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmallOrdered() {
    int k = 512;
    int u = k;
    boolean ordered = true;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCompactBytes();
    byte[] byteArray = new byte[bytes -8]; //too small
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    usk.compact(ordered, seg);
  }

  @Test
  public void checkCompactCachePart() {
    //phony values except for curCount = 0.
    long[] result = Intersection.compactCachePart(null, 4, 0, 0L, false);
    assertEquals(result.length, 0);
  }

  //See class State
  //Class Name
  //Count
  //Bytes
  private static final boolean COMPACT = true;
  private static final boolean EMPTY = true;
  private static final boolean DIRECT = true;
  private static final boolean SEGMENT = true;
  private static final boolean ORDERED = true;
  private static final boolean ESTIMATION = true;

  @Test
  /**
   * Empty, segment-based Compact sketches are always ordered
   */
  public void checkEmptyMemorySegmentCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    MemorySegment wseg1 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk1 = sk.compact(false, wseg1); //the first parameter is ignored when empty
    State state1 = new State("DirectCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    MemorySegment wseg2 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk2 = sk.compact(false, wseg2);
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //different object because MemorySegment is valid
    assertFalse(csk1 == csk2);

    MemorySegment wseg3 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk3 = csk1.compact(false, wseg3);
    state1.check(csk3);

    assertNotEquals(csk1, csk3); //different object because MemorySegment is valid
    assertFalse(csk1 == csk3);

    CompactSketch csk4 = csk1.compact(false, null);
    State state4 = new State("EmptyCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state4.check(csk4);

    assertNotEquals(csk1, csk4); //different object because on heap
    assertFalse(csk1 == csk4);

    CompactSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  /**
   * Single-Item, segment-based Compact sketches are always ordered:
   */
  public void checkSingleItemMemorySegmentCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);

    MemorySegment wseg1 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk1 = sk.compact(false, wseg1); //the first parameter is ignored when single item
    State state1 = new State("DirectCompactSketch", 1, 16, COMPACT, !EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    MemorySegment wseg2 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk2 = sk.compact(false, wseg2); //the first parameter is ignored when single item
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //different object because segment is valid
    assertFalse(csk1 == csk2);

    MemorySegment wseg3 = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk3 = csk1.compact(false, wseg3);
    state1.check(csk3);

    assertNotEquals(csk1, csk3); //different object because segment is valid
    assertFalse(csk1 == csk3);

    CompactSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkMultipleItemMemorySegmentCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    //This sequence is naturally out-of-order by the hash values.
    sk.update(1);
    sk.update(2);
    sk.update(3);

    MemorySegment wseg1 = MemorySegment.ofArray(new byte[50]);
    CompactSketch csk1 = sk.compact(true, wseg1);
    State state1 = new State("DirectCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    MemorySegment wseg2 = MemorySegment.ofArray(new byte[50]);
    CompactSketch csk2 = sk.compact(false, wseg2);
    State state2 = new State("DirectCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, SEGMENT, !ORDERED, !ESTIMATION);
    state2.check(csk2);

    assertNotEquals(csk1, csk2); //different object because segment is valid
    assertFalse(csk1 == csk2);

    MemorySegment wseg3 = MemorySegment.ofArray(new byte[50]);
    CompactSketch csk3 = csk1.compact(false, wseg3);
    state2.check(csk3);

    assertNotEquals(csk1, csk3); //different object because segment is valid
    assertFalse(csk1 == csk3);

    CompactSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  /**
   * Empty, heap-based Compact sketches are always ordered.
   * All empty, heap-based, compact sketches point to the same static, final constant of 8 bytes.
   */
  public void checkEmptyHeapCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    CompactSketch csk1 = sk.compact(false, null); //the first parameter is ignored when empty
    State state1 = new State("EmptyCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    CompactSketch csk2 = sk.compact(false, null); //the first parameter is ignored when empty
    state1.check(csk1);

    assertEquals(csk1, csk2);
    assertTrue(csk1 == csk2);

    CompactSketch csk3 = csk1.compact(false, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //The same object
    assertTrue(csk1 == csk3);

    CompactSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //The same object
    assertTrue(csk1 == cskc);
  }

  @Test
  /**
   * Single-Item, heap-based Compact sketches are always ordered.
   */
  public void checkSingleItemHeapCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);

    CompactSketch csk1 = sk.compact(false, null); //the first parameter is ignored when single item
    State state1 = new State("SingleItemSketch", 1, 16, COMPACT, !EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    CompactSketch csk2 = sk.compact(false, null); //the first parameter is ignored when single item
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //calling the compact(boolean, null) method creates a new object
    assertFalse(csk1 == csk2);

    CompactSketch csk3 = csk1.compact(false, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //The same object
    assertTrue(csk1 == csk3);

    CompactSketch cskc = csk1.compact(); //this, however just returns the same object.
    state1.check(csk1);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkMultipleItemHeapCompactSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    //This sequence is naturally out-of-order by the hash values.
    sk.update(1);
    sk.update(2);
    sk.update(3);

    CompactSketch csk1 = sk.compact(true, null); //creates a new object
    State state1 = new State("HeapCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    CompactSketch csk2 = sk.compact(false, null); //creates a new object, unordered
    State state2 = new State("HeapCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, !SEGMENT, !ORDERED, !ESTIMATION);
    state2.check(csk2);

    assertNotEquals(csk1, csk2); //order is different and different objects
    assertFalse(csk1 == csk2);

    CompactSketch csk3 = csk1.compact(true, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //the same object because wseg = null and csk1.ordered = dstOrdered
    assertTrue(csk1 == csk3);

    assertNotEquals(csk2, csk3); //different object because wseg = null and csk2.ordered = false && dstOrdered = true
    assertFalse(csk2 == csk3);

    CompactSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkHeapifySingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    int bytes = Sketches.getMaxCompactSketchBytes(2); //1 more than needed
    MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    sk.compact(false, wseg);
    Sketch csk = Sketch.heapify(wseg);
    assertTrue(csk instanceof SingleItemSketch);
  }

  @Test
  public void checkHeapifyEmptySketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    MemorySegment wseg = MemorySegment.ofArray(new byte[16]); //empty, but extra bytes
    CompactSketch csk = sk.compact(false, wseg); //ignores order because it is empty
    assertTrue(csk instanceof DirectCompactSketch);
    Sketch csk2 = Sketch.heapify(wseg);
    assertTrue(csk2 instanceof EmptyCompactSketch);
  }

  @Test
  public void checkGetCache() {
    UpdateSketch sk = Sketches.updateSketchBuilder().setP((float).5).build();
    sk.update(7);
    int bytes = sk.getCompactBytes();
    CompactSketch csk = sk.compact(true, MemorySegment.ofArray(new byte[bytes]));
    long[] cache = csk.getCache();
    assertTrue(cache.length == 0);
  }

  @Test
  public void checkHeapCompactSketchCompact() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    sk.update(1);
    sk.update(2);
    CompactSketch csk = sk.compact();
    assertTrue(csk.isOrdered());
    assertEquals(csk.getCurrentPreambleLongs(), 2);
  }

  /**
   * This is checking the empty, single, exact and estimating cases of an off-heap
   * sketch to make sure they are being stored properly and to check the new capability
   * of calling compact(boolean, MemorySegment) on an already compact sketch. This allows the
   * user to be able to change the order and heap status of an already compact sketch.
   */
  @Test
  public void checkDirectCompactSketchCompact() {
    MemorySegment wseg1, wseg2;
    CompactSketch csk1, csk2;
    int bytes;
    int lgK = 6;

    //empty
    UpdateSketch sk = Sketches.updateSketchBuilder().setLogNominalEntries(lgK).build();
    bytes = sk.getCompactBytes();         //empty, 8 bytes
    wseg1 = MemorySegment.ofArray(new byte[bytes]);
    wseg2 = MemorySegment.ofArray(new byte[bytes]);
    csk1 = sk.compact(false, wseg1);      //place into MemorySegment as unordered
    assertTrue(csk1 instanceof DirectCompactSketch);
    assertTrue(csk1.isOrdered());         //empty is always ordered
    csk2 = csk1.compact(false, wseg2);    //set to unordered again
    assertTrue(csk2 instanceof DirectCompactSketch);
    assertTrue(csk2.isOrdered());         //empty is always ordered
    assertTrue(csk2.getSeedHash() == 0);  //empty has no seed hash
    assertEquals(csk2.getCompactBytes(), 8);

    //single
    sk.update(1);
    bytes = sk.getCompactBytes();         //single, 16 bytes
    wseg1 = MemorySegment.ofArray(new byte[bytes]);
    wseg2 = MemorySegment.ofArray(new byte[bytes]);
    csk1 = sk.compact(false, wseg1);      //place into MemorySegment as unordered
    assertTrue(csk1 instanceof DirectCompactSketch);
    assertTrue(csk1.isOrdered());         //single is always ordered
    csk2 = csk1.compact(false, wseg2);    //set to unordered again
    assertTrue(csk2 instanceof DirectCompactSketch);
    assertTrue(csk2.isOrdered());         //single is always ordered
    assertTrue(csk2.getSeedHash() != 0);  //has a seed hash
    assertEquals(csk2.getCompactBytes(), 16);

    //exact
    sk.update(2);
    bytes = sk.getCompactBytes(); //exact, 16 bytes preamble, 16 bytes data
    wseg1 = MemorySegment.ofArray(new byte[bytes]);
    wseg2 = MemorySegment.ofArray(new byte[bytes]);
    csk1 = sk.compact(false, wseg1);      //place into MemorySegment as unordered
    assertTrue(csk1 instanceof DirectCompactSketch);
    assertFalse(csk1.isOrdered());        //should be unordered
    csk2 = csk1.compact(true, wseg2);     //set to ordered
    assertTrue(csk2 instanceof DirectCompactSketch);
    assertTrue(csk2.isOrdered());         //should be ordered
    assertTrue(csk2.getSeedHash() != 0);  //has a seed hash
    assertEquals(csk2.getCompactBytes(), 32);

    //estimating
    int n = 1 << (lgK + 1);
    for (int i = 2; i < n; i++) { sk.update(i); }
    bytes = sk.getCompactBytes(); //24 bytes preamble + curCount * 8,
    wseg1 = MemorySegment.ofArray(new byte[bytes]);
    wseg2 = MemorySegment.ofArray(new byte[bytes]);
    csk1 = sk.compact(false, wseg1);      //place into MemorySegment as unordered
    assertTrue(csk1 instanceof DirectCompactSketch);
    assertFalse(csk1.isOrdered());        //should be unordered
    csk2 = csk1.compact(true, wseg2);    //set to ordered
    assertTrue(csk2 instanceof DirectCompactSketch);
    assertTrue(csk2.isOrdered());        //should be ordered
    assertTrue(csk2.getSeedHash() != 0);  //has a seed hash
    int curCount = csk2.getRetainedEntries();
    assertEquals(csk2.getCompactBytes(), 24 + (curCount * 8));
  }

  @Test
  public void serializeDeserializeHeapV4() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    CompactSketch cs1 = sk.compact();
    byte[] bytes = cs1.toByteArrayCompressed();
    CompactSketch cs2 = CompactSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    HashIterator it1 = cs1.iterator();
    HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
  }

  @Test
  public void serializeDeserializeDirectV4() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    CompactSketch cs1 = sk.compact(true, MemorySegment.ofArray(new byte[sk.getCompactBytes()]));
    byte[] bytes = cs1.toByteArrayCompressed();
    CompactSketch cs2 = CompactSketch.wrap(MemorySegment.ofArray(bytes));
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    HashIterator it1 = cs1.iterator();
    HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
  }

  @Test
  public void serializeWrapBytesV3() {
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactSketch cs1 = sk.compact();
    final byte[] bytes = cs1.toByteArray();
    final CompactSketch cs2 = new WrappedCompactSketch(bytes);
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    final HashIterator it1 = cs1.iterator();
    final HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
    assertEquals(bytes, cs2.toByteArray());
  }

  @Test
  public void serializeWrapBytesV4() {
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactSketch cs1 = sk.compact();
    final byte[] bytes = cs1.toByteArrayCompressed();
    final CompactSketch cs2 = new WrappedCompactCompressedSketch(bytes);
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    final HashIterator it1 = cs1.iterator();
    final HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
    assertEquals(bytes, cs2.toByteArray());
  }

  private static class State {
    String classType = null;
    int count = 0;
    int bytes = 0;
    boolean compact = false;
    boolean empty = false;
    boolean direct = false;
    boolean hasSeg = false;
    boolean ordered = false;
    boolean estimation = false;

    State(String classType, int count, int bytes, boolean compact, boolean empty, boolean direct,
        boolean hasSeg, boolean ordered, boolean estimation) {
      this.classType = classType;
      this.count = count;
      this.bytes = bytes;
      this.compact = compact;
      this.empty = empty;
      this.direct = direct;
      this.hasSeg = hasSeg;
      this.ordered = ordered;
      this.estimation = estimation;
    }

    void check(CompactSketch csk) {
      assertEquals(csk.getClass().getSimpleName(), classType, "ClassType");
      assertEquals(csk.getRetainedEntries(true), count, "curCount");
      assertEquals(csk.getCurrentBytes(), bytes, "Bytes" );
      assertEquals(csk.isCompact(), compact, "Compact");
      assertEquals(csk.isEmpty(), empty, "Empty");
      assertEquals(csk.isDirect(), direct, "Direct");
      assertEquals(csk.hasMemorySegment(), hasSeg, "MemorySegment");
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
