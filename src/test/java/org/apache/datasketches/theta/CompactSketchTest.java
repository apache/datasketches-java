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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class CompactSketchTest {

  @Test
  public void checkHeapifyWrap() {
    final int k = 4096;
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
  public void checkHeapifyWrap(final int k, final int u, final boolean ordered) {
    final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) { //populate update sketch
      usk.update(i);
    }

    /****ON HEAP MemorySegment -- HEAPIFY****/
    CompactThetaSketch refSk = usk.compact(ordered, null);
    final byte[] barr = refSk.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(barr);
    CompactThetaSketch testSk = (CompactThetaSketch) ThetaSketch.heapify(srcSeg);

    checkByRange(refSk, testSk, u, ordered);

    /**Via byte[]**/
    final byte[] byteArray = refSk.toByteArray();
    final MemorySegment heapROSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    testSk = (CompactThetaSketch)ThetaSketch.heapify(heapROSeg);

    checkByRange(refSk, testSk, u, ordered);

    /****OFF HEAP MemorySegment -- WRAP****/
    //Prepare MemorySegment for direct
    final int bytes = usk.getCompactBytes(); //for Compact

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment directSeg = arena.allocate(bytes);

      /**Via CompactThetaSketch.compact**/
      refSk = usk.compact(ordered, directSeg);
      testSk = (CompactThetaSketch)ThetaSketch.wrap(directSeg);

      checkByRange(refSk, testSk, u, ordered);

      /**Via CompactThetaSketch.compact**/
      testSk = (CompactThetaSketch)ThetaSketch.wrap(directSeg);
      checkByRange(refSk, testSk, u, ordered);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void checkByRange(final ThetaSketch refSk, final ThetaSketch testSk, final int u, final boolean ordered) {
    if (u == 0) {
      checkEmptySketch(testSk);
    } else if (u == 1) {
      checkSingleItemSketch(testSk, refSk);
    } else {
      checkOtherCompactSketch(testSk, refSk, ordered);
    }
  }

  private static void checkEmptySketch(final ThetaSketch testSk) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertTrue(testSk instanceof EmptyCompactSketch);
    assertTrue(testSk.isEmpty());
    assertTrue(testSk.isOrdered());
    assertNull(testSk.getMemorySegment());
    assertFalse(testSk.isOffHeap());
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

  private static void checkSingleItemSketch(final ThetaSketch testSk, final ThetaSketch refSk) {
    assertEquals(testSk.getFamily(), Family.COMPACT);
    assertTrue(testSk instanceof SingleItemSketch);
    assertFalse(testSk.isEmpty());
    assertTrue(testSk.isOrdered());
    assertNull(testSk.getMemorySegment());
    assertFalse(testSk.isOffHeap());
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

  private static void checkOtherCompactSketch(final ThetaSketch testSk, final ThetaSketch refSk, final boolean ordered) {
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
      if (refSk.isOffHeap()) {
        assertTrue(testSk.isOffHeap());
      } else {
        assertFalse(testSk.isOffHeap());
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
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    sk.update(1);
    final int bytes = sk.getCompactBytes();
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    sk.compact(true, wseg);
    final ThetaSketch csk2 = ThetaSketch.heapify(wseg);
    assertTrue(csk2 instanceof SingleItemSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmall() {
    final int k = 512;
    final int u = k;
    final boolean ordered = false;
    final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    final int bytes = usk.getCompactBytes();
    final byte[] byteArray = new byte[bytes -8]; //too small
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    usk.compact(ordered, seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmallOrdered() {
    final int k = 512;
    final int u = k;
    final boolean ordered = true;
    final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    final int bytes = usk.getCompactBytes();
    final byte[] byteArray = new byte[bytes -8]; //too small
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    usk.compact(ordered, seg);
  }

  @Test
  public void checkCompactCachePart() {
    //phony values except for curCount = 0.
    final long[] result = ThetaIntersectionImpl.compactCachePart(null, 4, 0, 0L, false);
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
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();

    final MemorySegment wseg1 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk1 = sk.compact(false, wseg1); //the first parameter is ignored when empty
    final State state1 = new State("DirectCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final MemorySegment wseg2 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk2 = sk.compact(false, wseg2);
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //different object because MemorySegment is valid
    assertFalse(csk1 == csk2);

    final MemorySegment wseg3 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk3 = csk1.compact(false, wseg3);
    state1.check(csk3);

    assertNotEquals(csk1, csk3); //different object because MemorySegment is valid
    assertFalse(csk1 == csk3);

    final CompactThetaSketch csk4 = csk1.compact(false, null);
    final State state4 = new State("EmptyCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state4.check(csk4);

    assertNotEquals(csk1, csk4); //different object because on heap
    assertFalse(csk1 == csk4);

    final CompactThetaSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  /**
   * Single-Item, segment-based Compact sketches are always ordered:
   */
  public void checkSingleItemMemorySegmentCompactSketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    sk.update(1);

    final MemorySegment wseg1 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk1 = sk.compact(false, wseg1); //the first parameter is ignored when single item
    final State state1 = new State("DirectCompactSketch", 1, 16, COMPACT, !EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final MemorySegment wseg2 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk2 = sk.compact(false, wseg2); //the first parameter is ignored when single item
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //different object because segment is valid
    assertFalse(csk1 == csk2);

    final MemorySegment wseg3 = MemorySegment.ofArray(new byte[16]);
    final CompactThetaSketch csk3 = csk1.compact(false, wseg3);
    state1.check(csk3);

    assertNotEquals(csk1, csk3); //different object because segment is valid
    assertFalse(csk1 == csk3);

    final CompactThetaSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkMultipleItemMemorySegmentCompactSketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    //This sequence is naturally out-of-order by the hash values.
    sk.update(1);
    sk.update(2);
    sk.update(3);

    final MemorySegment wseg1 = MemorySegment.ofArray(new byte[50]);
    final CompactThetaSketch csk1 = sk.compact(true, wseg1);
    final State state1 = new State("DirectCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final MemorySegment wseg2 = MemorySegment.ofArray(new byte[50]);
    final CompactThetaSketch csk2 = sk.compact(false, wseg2);
    final State state2 = new State("DirectCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, SEGMENT, !ORDERED, !ESTIMATION);
    state2.check(csk2);

    assertNotEquals(csk1, csk2); //different object because segment is valid
    assertFalse(csk1 == csk2);

    final MemorySegment wseg3 = MemorySegment.ofArray(new byte[50]);
    final CompactThetaSketch csk3 = csk1.compact(false, wseg3);
    state2.check(csk3);

    assertNotEquals(csk1, csk3); //different object because segment is valid
    assertFalse(csk1 == csk3);

    final CompactThetaSketch cskc = csk1.compact();
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
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();

    final CompactThetaSketch csk1 = sk.compact(false, null); //the first parameter is ignored when empty
    final State state1 = new State("EmptyCompactSketch", 0, 8, COMPACT, EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final CompactThetaSketch csk2 = sk.compact(false, null); //the first parameter is ignored when empty
    state1.check(csk1);

    assertEquals(csk1, csk2);
    assertTrue(csk1 == csk2);

    final CompactThetaSketch csk3 = csk1.compact(false, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //The same object
    assertTrue(csk1 == csk3);

    final CompactThetaSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //The same object
    assertTrue(csk1 == cskc);
  }

  @Test
  /**
   * Single-Item, heap-based Compact sketches are always ordered.
   */
  public void checkSingleItemHeapCompactSketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    sk.update(1);

    final CompactThetaSketch csk1 = sk.compact(false, null); //the first parameter is ignored when single item
    final State state1 = new State("SingleItemSketch", 1, 16, COMPACT, !EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final CompactThetaSketch csk2 = sk.compact(false, null); //the first parameter is ignored when single item
    state1.check(csk2);

    assertNotEquals(csk1, csk2); //calling the compact(boolean, null) method creates a new object
    assertFalse(csk1 == csk2);

    final CompactThetaSketch csk3 = csk1.compact(false, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //The same object
    assertTrue(csk1 == csk3);

    final CompactThetaSketch cskc = csk1.compact(); //this, however just returns the same object.
    state1.check(csk1);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkMultipleItemHeapCompactSketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    //This sequence is naturally out-of-order by the hash values.
    sk.update(1);
    sk.update(2);
    sk.update(3);

    final CompactThetaSketch csk1 = sk.compact(true, null); //creates a new object
    final State state1 = new State("HeapCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, !SEGMENT, ORDERED, !ESTIMATION);
    state1.check(csk1);

    final CompactThetaSketch csk2 = sk.compact(false, null); //creates a new object, unordered
    final State state2 = new State("HeapCompactSketch", 3, 40, COMPACT, !EMPTY, !DIRECT, !SEGMENT, !ORDERED, !ESTIMATION);
    state2.check(csk2);

    assertNotEquals(csk1, csk2); //order is different and different objects
    assertFalse(csk1 == csk2);

    final CompactThetaSketch csk3 = csk1.compact(true, null);
    state1.check(csk3);

    assertEquals(csk1, csk3); //the same object because wseg = null and csk1.ordered = dstOrdered
    assertTrue(csk1 == csk3);

    assertNotEquals(csk2, csk3); //different object because wseg = null and csk2.ordered = false && dstOrdered = true
    assertFalse(csk2 == csk3);

    final CompactThetaSketch cskc = csk1.compact();
    state1.check(cskc);

    assertEquals(csk1, cskc); //the same object
    assertTrue(csk1 == cskc);
  }

  @Test
  public void checkHeapifySingleItemSketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    sk.update(1);
    final int bytes = ThetaSketch.getMaxCompactSketchBytes(2); //1 more than needed
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    sk.compact(false, wseg);
    final ThetaSketch csk = ThetaSketch.heapify(wseg);
    assertTrue(csk instanceof SingleItemSketch);
  }

  @Test
  public void checkHeapifyEmptySketch() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    final MemorySegment wseg = MemorySegment.ofArray(new byte[16]); //empty, but extra bytes
    final CompactThetaSketch csk = sk.compact(false, wseg); //ignores order because it is empty
    assertTrue(csk instanceof DirectCompactSketch);
    final ThetaSketch csk2 = ThetaSketch.heapify(wseg);
    assertTrue(csk2 instanceof EmptyCompactSketch);
  }

  @Test
  public void checkGetCache() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setP((float).5).build();
    sk.update(7);
    final int bytes = sk.getCompactBytes();
    final CompactThetaSketch csk = sk.compact(true, MemorySegment.ofArray(new byte[bytes]));
    final long[] cache = csk.getCache();
    assertTrue(cache.length == 0);
  }

  @Test
  public void checkHeapCompactSketchCompact() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    sk.update(1);
    sk.update(2);
    final CompactThetaSketch csk = sk.compact();
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
    CompactThetaSketch csk1, csk2;
    int bytes;
    final int lgK = 6;

    //empty
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setLogNominalEntries(lgK).build();
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
    final int n = 1 << (lgK + 1);
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
    final int curCount = csk2.getRetainedEntries();
    assertEquals(csk2.getCompactBytes(), 24 + (curCount * 8));
  }

  @Test
  public void serializeDeserializeHeapV4() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactThetaSketch cs1 = sk.compact();
    final byte[] bytes = cs1.toByteArrayCompressed();
    final CompactThetaSketch cs2 = CompactThetaSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    final HashIterator it1 = cs1.iterator();
    final HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
  }

  @Test
  public void serializeDeserializeDirectV4_segment() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactThetaSketch cs1 = sk.compact(true, MemorySegment.ofArray(new byte[sk.getCompactBytes()]));
    final byte[] bytes = cs1.toByteArrayCompressed();
    final CompactThetaSketch cs2 = CompactThetaSketch.wrap(MemorySegment.ofArray(bytes));
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    final HashIterator it1 = cs1.iterator();
    final HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
  }

  @Test
  public void serializeDeserializeDirectV4_bytes() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactThetaSketch cs1 = sk.compact(true, MemorySegment.ofArray(new byte[sk.getCompactBytes()]));
    final byte[] bytes = cs1.toByteArrayCompressed();
    final CompactThetaSketch cs2 = CompactThetaSketch.wrap(bytes);
    assertEquals(cs1.getRetainedEntries(), cs2.getRetainedEntries());
    final HashIterator it1 = cs1.iterator();
    final HashIterator it2 = cs2.iterator();
    while (it1.next() && it2.next()) {
      assertEquals(it2.get(), it2.get());
    }
  }


  @Test
  public void serializeWrapBytesV3() {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactThetaSketch cs1 = sk.compact();
    final byte[] bytes = cs1.toByteArray();
    final CompactThetaSketch cs2 = new WrappedCompactSketch(bytes);
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
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
    final CompactThetaSketch cs1 = sk.compact();
    final byte[] bytes = cs1.toByteArrayCompressed();
    final CompactThetaSketch cs2 = new WrappedCompactCompressedSketch(bytes);
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

    State(final String classType, final int count, final int bytes, final boolean compact, final boolean empty, final boolean direct,
        final boolean hasSeg, final boolean ordered, final boolean estimation) {
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

    void check(final CompactThetaSketch csk) {
      assertEquals(csk.getClass().getSimpleName(), classType, "ClassType");
      assertEquals(csk.getRetainedEntries(true), count, "curCount");
      assertEquals(csk.getCurrentBytes(), bytes, "Bytes" );
      assertEquals(csk.isCompact(), compact, "Compact");
      assertEquals(csk.isEmpty(), empty, "Empty");
      assertEquals(csk.isOffHeap(), direct, "Direct");
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
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
