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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc","deprecation"})
public class SingleItemSketchTest {
  final static short DEFAULT_SEED_HASH = (short) (computeSeedHash(DEFAULT_UPDATE_SEED) & 0XFFFFL);

  @Test
  public void check1() {
    final Union union = Sketches.setOperationBuilder().buildUnion();
    union.update(SingleItemSketch.create(1));
    union.update(SingleItemSketch.create(1.0));
    union.update(SingleItemSketch.create(0.0));
    union.update(SingleItemSketch.create("1"));
    union.update(SingleItemSketch.create(new byte[] {1,2,3,4}));
    union.update(SingleItemSketch.create(new char[] {'a'}));
    union.update(SingleItemSketch.create(new int[] {2}));
    union.update(SingleItemSketch.create(new long[] {3}));

    union.update(SingleItemSketch.create(-0.0)); //duplicate

    final double est = union.getResult().getEstimate();
    println(""+est);
    assertEquals(est, 8.0, 0.0);

    assertNull(SingleItemSketch.create(""));
    final String str = null;
    assertNull(SingleItemSketch.create(str));//returns null

    assertNull(SingleItemSketch.create(new byte[0]));//returns null
    final byte[] byteArr = null;
    assertNull(SingleItemSketch.create(byteArr));//returns null

    assertNull(SingleItemSketch.create(new char[0]));//returns null
    final char[] charArr = null;
    assertNull(SingleItemSketch.create(charArr));//returns null

    assertNull(SingleItemSketch.create(new int[0]));//returns null
    final int[] intArr = null;
    assertNull(SingleItemSketch.create(intArr));//returns null

    assertNull(SingleItemSketch.create(new long[0]));//returns null
    final long[] longArr = null;
    assertNull(SingleItemSketch.create(longArr));//returns null
  }

  @Test
  public void check2() {
    final long seed = DEFAULT_UPDATE_SEED;
    final Union union = Sketches.setOperationBuilder().buildUnion();
    union.update(SingleItemSketch.create(1, seed));
    union.update(SingleItemSketch.create(1.0, seed));
    union.update(SingleItemSketch.create(0.0, seed));
    union.update(SingleItemSketch.create("1", seed));
    union.update(SingleItemSketch.create(new byte[] {1,2,3,4}, seed));
    union.update(SingleItemSketch.create(new char[] {'a'}, seed));
    union.update(SingleItemSketch.create(new int[] {2}, seed));
    union.update(SingleItemSketch.create(new long[] {3}, seed));

    union.update(SingleItemSketch.create(-0.0, seed)); //duplicate

    final double est = union.getResult().getEstimate();
    println(""+est);
    assertEquals(est, 8.0, 0.0);

    assertNull(SingleItemSketch.create("", seed));
    final String str = null;
    assertNull(SingleItemSketch.create(str, seed));//returns null

    assertNull(SingleItemSketch.create(new byte[0], seed));//returns null
    final byte[] byteArr = null;
    assertNull(SingleItemSketch.create(byteArr, seed));//returns null

    assertNull(SingleItemSketch.create(new char[0], seed));//returns null
    final char[] charArr = null;
    assertNull(SingleItemSketch.create(charArr, seed));//returns null

    assertNull(SingleItemSketch.create(new int[0], seed));//returns null
    final int[] intArr = null;
    assertNull(SingleItemSketch.create(intArr, seed));//returns null

    assertNull(SingleItemSketch.create(new long[0], seed));//returns null
    final long[] longArr = null;
    assertNull(SingleItemSketch.create(longArr, seed));//returns null
  }

  @Test
  public void checkSketchInterface() {
    final SingleItemSketch sis = SingleItemSketch.create(1);
    assertEquals(sis.getCompactBytes(), 16);
    assertEquals(sis.getEstimate(), 1.0);
    assertEquals(sis.getLowerBound(1), 1.0);
    assertEquals(sis.getRetainedEntries(true), 1);
    assertEquals(sis.getUpperBound(1), 1.0);
    assertFalse(sis.isDirect());
    assertFalse(sis.hasMemory());
    assertFalse(sis.isEmpty());
    assertTrue(sis.isOrdered());
  }

  @Test
  public void checkLessThanThetaLong() {
    for (int i = 0; i < 10; i++) {
      final long[] data = { i };
      final long h = hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1;
      final SingleItemSketch sis = SingleItemSketch.create(i);
      final long halfMax = Long.MAX_VALUE >> 1;
      final int count = sis.getCountLessThanThetaLong(halfMax);
      assertEquals(count, h < halfMax ? 1 : 0);
    }
  }

  @Test
  public void checkSerDe() {
    final SingleItemSketch sis = SingleItemSketch.create(1);
    final byte[] byteArr = sis.toByteArray();
    final Memory mem = Memory.wrap(byteArr);
    final SingleItemSketch sis2 = SingleItemSketch.heapify(mem);
    assertEquals(sis2.getEstimate(), 1.0);

    final SingleItemSketch sis3 = SingleItemSketch.heapify(mem, DEFAULT_UPDATE_SEED);
    assertEquals(sis2.getEstimate(), 1.0);

    final Union union = Sketches.setOperationBuilder().buildUnion();
    union.update(sis);
    union.update(sis2);
    union.update(sis3);
    assertEquals(union.getResult().getEstimate(), 1.0);
  }

  @Test
  public void checkRestricted() {
    final SingleItemSketch sis = SingleItemSketch.create(1);
    assertNull(sis.getMemory());
    assertEquals(sis.getCompactPreambleLongs(), 1);
  }

  @Test
  public void unionWrapped() {
    final Sketch sketch = SingleItemSketch.create(1);
    final Union union = Sketches.setOperationBuilder().buildUnion();
    final Memory mem = Memory.wrap(sketch.toByteArray());
    union.update(mem);
    assertEquals(union.getResult().getEstimate(), 1, 0);
  }

  @Test
  public void buildAndCompact() {
    UpdateSketch sk1;
    CompactSketch csk;
    int bytes;
    //On-heap
    sk1 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk1.update(1);
    csk = sk1.compact(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    csk = sk1.compact(false, null);
    assertTrue(csk instanceof SingleItemSketch);

    //Off-heap
    bytes = Sketches.getMaxUpdateSketchBytes(32);
    WritableMemory wmem = WritableMemory.wrap(new byte[bytes]);
    sk1= Sketches.updateSketchBuilder().setNominalEntries(32).build(wmem);
    sk1.update(1);
    csk = sk1.compact(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    csk = sk1.compact(false, null);
    assertTrue(csk instanceof SingleItemSketch);

    bytes = Sketches.getMaxCompactSketchBytes(1);
    wmem = WritableMemory.wrap(new byte[bytes]);
    csk = sk1.compact(true, wmem);
    assertTrue(csk.isOrdered());
    csk = sk1.compact(false, wmem);
    assertTrue(csk.isOrdered());
  }

  @Test
  public void intersection() {
    UpdateSketch sk1, sk2;
    CompactSketch csk;
    int bytes;
    //Intersection on-heap
    sk1 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk2 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk1.update(1);
    sk1.update(2);
    sk2.update(1);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(sk1);
    inter.intersect(sk2);
    csk = inter.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);

    //Intersection off-heap
    bytes = Sketches.getMaxIntersectionBytes(32);
    final WritableMemory wmem = WritableMemory.wrap(new byte[bytes]);
    inter = Sketches.setOperationBuilder().buildIntersection(wmem);
    inter.intersect(sk1);
    inter.intersect(sk2);
    csk = inter.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    csk = inter.getResult(false, null);
    assertTrue(csk instanceof SingleItemSketch);
  }

  @Test
  public void union() {
    UpdateSketch sk1, sk2;
    CompactSketch csk;
    int bytes;
    //Union on-heap
    sk1 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk2 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk1.update(1);
    sk2.update(1);
    Union union = Sketches.setOperationBuilder().buildUnion();
    union.update(sk1);
    union.update(sk2);
    csk = union.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);

    //Union off-heap
    bytes = Sketches.getMaxUnionBytes(32);
    final WritableMemory wmem = WritableMemory.wrap(new byte[bytes]);
    union = Sketches.setOperationBuilder().buildUnion(wmem);
    union.update(sk1);
    union.update(sk2);
    csk = union.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    csk = union.getResult(false, null);
    assertTrue(csk instanceof SingleItemSketch);
  }

  @Test
  public void aNotB() {
    UpdateSketch sk1, sk2;
    CompactSketch csk;
    //AnotB on-heap
    sk1 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk2 = Sketches.updateSketchBuilder().setNominalEntries(32).build();
    sk1.update(1);
    sk2.update(2);
    final AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.update(sk1, sk2);
    csk = aNotB.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    //not AnotB off-heap form
  }

  @Test
  public void checkHeapifyInstance() {
    final UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    final UpdateSketch sk2 = new UpdateSketchBuilder().build();
    sk2.update(1);
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(sk1);
    inter.intersect(sk2);
    final WritableMemory wmem = WritableMemory.wrap(new byte[16]);
    final CompactSketch csk = inter.getResult(false, wmem);
    assertTrue(csk.isOrdered());
    final Sketch csk2 = Sketches.heapifySketch(wmem);
    assertTrue(csk2 instanceof SingleItemSketch);
    println(csk2.toString(true, true, 1, true));
  }

  @Test
  public void checkSingleItemBadFlags() {
    final UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    final WritableMemory wmem = WritableMemory.allocate(16);
    sk1.compact(true, wmem);
    wmem.putByte(5, (byte) 0); //corrupt flags
    try {
      SingleItemSketch.heapify(wmem);
      fail();
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkDirectUnionSingleItem2() {
    Sketch sk = Sketch.wrap(siSkWoutSiFlag24Bytes());
    assertEquals(sk.getEstimate(), 1.0, 0.0);
    //println(sk.toString());
    sk = Sketch.wrap(siSkWithSiFlag24Bytes());
    assertEquals(sk.getEstimate(), 1.0, 0.0);
    //println(sk.toString());
  }

  @Test
  public void checkSingleItemCompact() {
    final UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    final CompactSketch csk = sk1.compact();
    assertTrue(csk instanceof SingleItemSketch);
    final CompactSketch csk2 = csk.compact();
    assertEquals(csk, csk2);
    final CompactSketch csk3 = csk.compact(true, WritableMemory.allocate(16));
    assertTrue(csk3 instanceof DirectCompactSketch);
    assertEquals(csk2.getCurrentPreambleLongs(), 1);
    assertEquals(csk3.getCurrentPreambleLongs(), 1);
  }


  static final long SiSkPre0WithSiFlag = 0x93cc3a0000030301L;
  static final long SiSkPre0WoutSiFlag = 0x93cc1a0000030301L;
  static final long Hash = 0x05a186bdcb7df915L;

  static Memory siSkWithSiFlag24Bytes() {
    final int cap = 24; //8 extra bytes
    final WritableMemory wmem = WritableMemory.allocate(cap);
    wmem.putLong(0, SiSkPre0WithSiFlag);
    wmem.putLong(8, Hash);
    return wmem;
  }

  static Memory siSkWoutSiFlag24Bytes() {
    final int cap = 24; //8 extra bytes
    final WritableMemory wmem = WritableMemory.allocate(cap);
    wmem.putLong(0, SiSkPre0WoutSiFlag);
    wmem.putLong(8, Hash);
    return wmem;
  }

  @Test
  public void checkDruidJira_ADDDRUID_856() {
    final byte[] bytes = { 0x01, 0x03, 0x03, 0x00, 0x00, 0x3A, (byte) 0xCC, (byte) 0x93,
        (byte) 0xB0, (byte) 0xD9, 0x39, 0x5C, 0x25, 0x3E, (byte) 0xD0, 0x29,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    final Sketch s = Sketches.wrapSketch(Memory.wrap(bytes));
    final Union u = SetOperation.builder().buildUnion();
    u.union(s); //should not throw
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
