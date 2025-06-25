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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SingleItemSketchTest {
  final static short DEFAULT_SEED_HASH = (short) (ThetaUtil.computeSeedHash(ThetaUtil.DEFAULT_UPDATE_SEED) & 0XFFFFL);

  @Test
  public void check1() {
    Union union = Sketches.setOperationBuilder().buildUnion();
    union.union(SingleItemSketch.create(1));
    union.union(SingleItemSketch.create(1.0));
    union.union(SingleItemSketch.create(0.0));
    union.union(SingleItemSketch.create("1"));
    union.union(SingleItemSketch.create(new byte[] {1,2,3,4}));
    union.union(SingleItemSketch.create(new char[] {'a'}));
    union.union(SingleItemSketch.create(new int[] {2}));
    union.union(SingleItemSketch.create(new long[] {3}));

    union.union(SingleItemSketch.create(-0.0)); //duplicate

    double est = union.getResult().getEstimate();
    println(""+est);
    assertEquals(est, 8.0, 0.0);

    assertNull(SingleItemSketch.create(""));
    String str = null;
    assertNull(SingleItemSketch.create(str));//returns null

    assertNull(SingleItemSketch.create(new byte[0]));//returns null
    byte[] byteArr = null;
    assertNull(SingleItemSketch.create(byteArr));//returns null

    assertNull(SingleItemSketch.create(new char[0]));//returns null
    char[] charArr = null;
    assertNull(SingleItemSketch.create(charArr));//returns null

    assertNull(SingleItemSketch.create(new int[0]));//returns null
    int[] intArr = null;
    assertNull(SingleItemSketch.create(intArr));//returns null

    assertNull(SingleItemSketch.create(new long[0]));//returns null
    long[] longArr = null;
    assertNull(SingleItemSketch.create(longArr));//returns null
  }

  @Test
  public void check2() {
    long seed = ThetaUtil.DEFAULT_UPDATE_SEED;
    Union union = Sketches.setOperationBuilder().buildUnion();
    union.union(SingleItemSketch.create(1, seed));
    union.union(SingleItemSketch.create(1.0, seed));
    union.union(SingleItemSketch.create(0.0, seed));
    union.union(SingleItemSketch.create("1", seed));
    union.union(SingleItemSketch.create(new byte[] {1,2,3,4}, seed));
    union.union(SingleItemSketch.create(new char[] {'a'}, seed));
    union.union(SingleItemSketch.create(new int[] {2}, seed));
    union.union(SingleItemSketch.create(new long[] {3}, seed));

    union.union(SingleItemSketch.create(-0.0, seed)); //duplicate

    double est = union.getResult().getEstimate();
    println(""+est);
    assertEquals(est, 8.0, 0.0);

    assertNull(SingleItemSketch.create("", seed));
    String str = null;
    assertNull(SingleItemSketch.create(str, seed));//returns null

    assertNull(SingleItemSketch.create(new byte[0], seed));//returns null
    byte[] byteArr = null;
    assertNull(SingleItemSketch.create(byteArr, seed));//returns null

    assertNull(SingleItemSketch.create(new char[0], seed));//returns null
    char[] charArr = null;
    assertNull(SingleItemSketch.create(charArr, seed));//returns null

    assertNull(SingleItemSketch.create(new int[0], seed));//returns null
    int[] intArr = null;
    assertNull(SingleItemSketch.create(intArr, seed));//returns null

    assertNull(SingleItemSketch.create(new long[0], seed));//returns null
    long[] longArr = null;
    assertNull(SingleItemSketch.create(longArr, seed));//returns null
  }

  @Test
  public void checkSketchInterface() {
    SingleItemSketch sis = SingleItemSketch.create(1);
    assertEquals(sis.getCompactBytes(), 16);
    assertEquals(sis.getEstimate(), 1.0);
    assertEquals(sis.getLowerBound(1), 1.0);
    assertEquals(sis.getRetainedEntries(true), 1);
    assertEquals(sis.getUpperBound(1), 1.0);
    assertFalse(sis.isDirect());
    assertFalse(sis.hasMemorySegment());
    assertFalse(sis.isEmpty());
    assertTrue(sis.isOrdered());
  }

  @Test
  public void checkLessThanThetaLong() {
    for (int i = 0; i < 10; i++) {
      long[] data = { i };
      long h = hash(data, ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1;
      SingleItemSketch sis = SingleItemSketch.create(i);
      long halfMax = Long.MAX_VALUE >> 1;
      int count = sis.getCountLessThanThetaLong(halfMax);
      assertEquals(count, (h < halfMax) ? 1 : 0);
    }
  }

  @Test
  public void checkSerDe() {
    SingleItemSketch sis = SingleItemSketch.create(1);
    byte[] byteArr = sis.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArr);
    final short defaultSeedHash = ThetaUtil.computeSeedHash(ThetaUtil.DEFAULT_UPDATE_SEED);
    SingleItemSketch sis2 = SingleItemSketch.heapify(seg,  defaultSeedHash);
    assertEquals(sis2.getEstimate(), 1.0);

    SingleItemSketch sis3 = SingleItemSketch.heapify(seg , defaultSeedHash);
    assertEquals(sis3.getEstimate(), 1.0);

    Union union = Sketches.setOperationBuilder().buildUnion();
    union.union(sis);
    union.union(sis2);
    union.union(sis3);
    CompactSketch csk = union.getResult();
    assertTrue(csk instanceof SingleItemSketch);
    assertEquals(union.getResult().getEstimate(), 1.0);
  }

  @Test
  public void checkRestricted() {
    SingleItemSketch sis = SingleItemSketch.create(1);
    assertNull(sis.getMemorySegment());
    assertEquals(sis.getCompactPreambleLongs(), 1);
  }

  @Test
  public void unionWrapped() {
    Sketch sketch = SingleItemSketch.create(1);
    Union union = Sketches.setOperationBuilder().buildUnion();
    MemorySegment seg  = MemorySegment.ofArray(sketch.toByteArray());
    union.union(seg );
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
    MemorySegment wseg  = MemorySegment.ofArray(new byte[bytes]);
    sk1= Sketches.updateSketchBuilder().setNominalEntries(32).build(wseg );
    sk1.update(1);
    csk = sk1.compact(true, null);
    assertTrue(csk instanceof SingleItemSketch);
    csk = sk1.compact(false, null);
    assertTrue(csk instanceof SingleItemSketch);

    bytes = Sketches.getMaxCompactSketchBytes(1);
    wseg  = MemorySegment.ofArray(new byte[bytes]);
    csk = sk1.compact(true, wseg );
    assertTrue(csk.isOrdered());
    csk = sk1.compact(false, wseg );
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
    MemorySegment wseg  = MemorySegment.ofArray(new byte[bytes]);
    inter = Sketches.setOperationBuilder().buildIntersection(wseg );
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
    union.union(sk1);
    union.union(sk2);
    csk = union.getResult(true, null);
    assertTrue(csk instanceof SingleItemSketch);

    //Union off-heap
    bytes = Sketches.getMaxUnionBytes(32);
    MemorySegment wseg  = MemorySegment.ofArray(new byte[bytes]);
    union = Sketches.setOperationBuilder().buildUnion(wseg );
    union.union(sk1);
    union.union(sk2);
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
    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.setA(sk1);
    aNotB.notB(sk2);
    csk = aNotB.getResult(true, null, true);
    assertTrue(csk instanceof SingleItemSketch);
    //not AnotB off-heap form
  }

  @Test
  public void checkHeapifyInstance() {
    UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    UpdateSketch sk2 = new UpdateSketchBuilder().build();
    sk2.update(1);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(sk1);
    inter.intersect(sk2);
    MemorySegment wseg  = MemorySegment.ofArray(new byte[16]);
    CompactSketch csk = inter.getResult(false, wseg );
    assertTrue(csk.isOrdered());
    Sketch csk2 = Sketches.heapifySketch(wseg );
    assertTrue(csk2 instanceof SingleItemSketch);
    println(csk2.toString(true, true, 1, true));
  }

  @Test
  public void checkSingleItemBadFlags() {
    final short defaultSeedHash = ThetaUtil.computeSeedHash(ThetaUtil.DEFAULT_UPDATE_SEED);
    UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    MemorySegment wseg  = MemorySegment.ofArray(new byte[16]);
    sk1.compact(true, wseg );
    wseg .set(JAVA_BYTE, 5, (byte) 0); //corrupt flags to zero
    try {
      SingleItemSketch.heapify(wseg , defaultSeedHash); //fails due to corrupted flags bytes
      fail();
    } catch (SketchesArgumentException e) { }
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
    UpdateSketch sk1 = new UpdateSketchBuilder().build();
    sk1.update(1);
    CompactSketch csk = sk1.compact();
    assertTrue(csk instanceof SingleItemSketch);
    CompactSketch csk2 = csk.compact();
    assertEquals(csk, csk2);
    CompactSketch csk3 = csk.compact(true, MemorySegment.ofArray(new byte[16]));
    assertTrue(csk3 instanceof DirectCompactSketch);
    assertEquals(csk2.getCurrentPreambleLongs(), 1);
    assertEquals(csk3.getCurrentPreambleLongs(), 1);
  }


  static final long SiSkPre0WithSiFlag = 0x93cc3a0000030301L;
  static final long SiSkPre0WoutSiFlag = 0x93cc1a0000030301L;
  static final long Hash = 0x05a186bdcb7df915L;

  static MemorySegment siSkWithSiFlag24Bytes() {
    int cap = 24; //8 extra bytes
    MemorySegment wseg  = MemorySegment.ofArray(new byte[cap]);
    wseg .set(JAVA_LONG_UNALIGNED, 0, SiSkPre0WithSiFlag);
    wseg .set(JAVA_LONG_UNALIGNED, 8, Hash);
    return wseg ;
  }

  static MemorySegment siSkWoutSiFlag24Bytes() {
    int cap = 24; //8 extra bytes
    MemorySegment wseg  = MemorySegment.ofArray(new byte[cap]);
    wseg .set(JAVA_LONG_UNALIGNED, 0, SiSkPre0WoutSiFlag);
    wseg .set(JAVA_LONG_UNALIGNED, 8, Hash);
    return wseg;
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
