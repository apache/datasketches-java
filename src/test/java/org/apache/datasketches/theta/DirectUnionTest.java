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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.theta.HeapUnionTest.testAllCompactForms;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.SetOperation.getMaxUnionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectUnionTest {

  @Test
  public void checkExactUnionNoOverlap() {
    final int lgK = 9; //512
    final int k = 1 << lgK;
    final int u = k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);
  }

  @Test
  public void checkEstUnionNoOverlap() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //2*k
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2*k no overlap
    }

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.05);
  }

  @Test
  public void checkExactUnionWithOverlap() {
    final int lgK = 9; //512
    final int k = 1 << lgK;
    final int u = k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //256
    }
    for (int i=0; i<u  ; i++) {
      usk2.update(i); //512, 256 overlapped
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate()/2, 0.0); //exact, overlapped

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);
  }

  @Test
  public void checkHeapifyExact() {
    final int lgK = 9; //512
    final int k = 1 << lgK;
    final int u = k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);

    final Union union2 = (Union)SetOperation.heapify(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.0);
  }

  //these parallel the checkHeapifyExact, etc.
  @Test
  public void checkWrapExact() {
    final int lgK = 9; //512
    final int k = 1 << lgK;
    final int u = k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlap() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //2k
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2k no overlap, exact
    }

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //update with heap UpdateSketch
    union.union(usk2); //update with heap UpdateSketch, early stop not possible

    testAllCompactForms(union, u, 0.05);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedIn() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2k no overlap, exact, will force early stop
    }

    final CompactSketch cosk2 = usk2.compact(true, null);

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1);  //update with heap UpdateSketch
    union.union(cosk2); //update with heap Compact, Ordered input, early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.union(emptySketch); //updates with empty
    emptySketch = null;
    union.union(emptySketch); //updates with null

    testAllCompactForms(union, u, 0.05);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedDirectIn() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build(); //2k estimating
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<u/2; i++) {
      usk1.update(i); //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    final MemorySegment cskSeg2 = MemorySegment.ofArray(new byte[usk2.getCompactBytes()]);
    final CompactSketch cosk2 = usk2.compact(true, cskSeg2); //ordered, loads the cskSeg2 as ordered

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1);      //updates with heap UpdateSketch
    union.union(cosk2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.union(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.union(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedSegIn() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<u/2; i++) {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    final MemorySegment cskSeg2 = MemorySegment.ofArray(new byte[usk2.getCompactBytes()]);
    usk2.compact(true, cskSeg2); //ordered, loads the cskSeg2 as ordered

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1);        //updates with heap UpdateSketch
    union.union(cskSeg2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.union(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.union(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapUnorderedSegIn() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<u/2; i++) {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    final MemorySegment cskSeg2 = MemorySegment.ofArray(new byte[usk2.getCompactBytes()]);
    usk2.compact(false, cskSeg2); //unordered, loads the cskSeg2 as unordered

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1);        //updates with heap UpdateSketch
    union.union(cskSeg2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.union(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.union(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    final Union union2 = Union.wrap(MemorySegment.ofArray(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkMultiUnion() {
    final int lgK = 13; //8192
    final int k = 1 << lgK;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk3 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk4 = UpdateSketch.builder().setNominalEntries(k).build();

    int v=0;
    int u = 1000000;
    for (int i=0; i<u; i++) {
      usk1.update(i+v);
    }
    v += u;
    u = 26797;
    for (int i=0; i<u; i++) {
      usk2.update(i+v);
    }
    v += u;
    for (int i=0; i<u; i++) {
      usk3.update(i+v);
    }
    v += u;
    for (int i=0; i<u; i++) {
      usk4.update(i+v);
    }
    v += u;

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(usk1); //updates with heap UpdateSketch
    union.union(usk2); //updates with heap UpdateSketch
    union.union(usk3); //updates with heap UpdateSketch
    union.union(usk4); //updates with heap UpdateSketch

    final CompactSketch csk = union.getResult(true, null);
    final double est = csk.getEstimate();
    assertEquals(est, v, .01*v);
  }

  @Test
  public void checkDirectSegmentIn() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u1 = 2*k;
    final int u2 = 1024; //smaller exact sketch forces early stop
    final int totU = u1+u2;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u1; i++) {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++) {
      usk2.update(i); //2*k + 1024 no overlap
    }

    final MemorySegment skSeg1 = MemorySegment.ofArray(usk1.compact(false, null).toByteArray()).asReadOnly();
    final MemorySegment skSeg2 = MemorySegment.ofArray(usk2.compact(true, null).toByteArray()).asReadOnly();

    final CompactSketch csk1 = (CompactSketch)ThetaSketch.wrap(skSeg1);
    final CompactSketch csk2 = (CompactSketch)ThetaSketch.wrap(skSeg2);

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(csk1);
    union.union(csk2);

    final CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }

  @Test
  public void checkUpdateSegmentSpecialCases2() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;
    final int u = 2*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++)
     {
      usk1.update(i); //force prelongs to 3
    }
    final CompactSketch usk1c = usk1.compact(true, null);
    final MemorySegment v3seg1 = MemorySegment.ofArray(usk1c.toByteArray());

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);
    union.union(v3seg1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegBadSerVer() {
    final int lgK = 12; //4096
    final int k = 1 << lgK;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    usk1.update(1);
    usk1.update(2);
    final CompactSketch usk1c = usk1.compact(true, null);
    final MemorySegment v3seg1 = MemorySegment.ofArray(usk1c.toByteArray());
    //corrupt SerVer
    v3seg1.set(JAVA_BYTE, SER_VER_BYTE, (byte)0);

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.union(v3seg1);
  }

  //Special DirectUnion cases
  @Test //Himanshu's issue
  public void checkDirectWrap() {
    final int nomEntries = 16;
    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(nomEntries)]);
    SetOperation.builder().setNominalEntries(nomEntries).buildUnion(uSeg);

    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(nomEntries).build();
    sk1.update("a");
    sk1.update("b");

    final UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(nomEntries).build();
    sk2.update("c");
    sk2.update("d");

    Union union = Union.wrap(uSeg);
    union.union(sk1);

    union = Union.wrap(uSeg);
    union.union(sk2);

    final CompactSketch sketch = union.getResult(true, null);
    assertEquals(4.0, sketch.getEstimate(), 0.0);
  }

  @Test
  public void checkEmptyUnionCompactResult() {
    final int k = 64;

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    final MemorySegment seg = MemorySegment.ofArray(new byte[ThetaSketch.getMaxCompactSketchBytes(0)]);
    final CompactSketch csk = union.getResult(false, seg); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }

  @Test
  public void checkEmptyUnionCompactOrderedResult() {
    final int k = 64;

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    final MemorySegment seg = MemorySegment.ofArray(new byte[ThetaSketch.getMaxCompactSketchBytes(0)]);
    final CompactSketch csk = union.getResult(true, seg); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }

  @Test
  public void checkUnionSegToString() {
    final int k = 64;

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]); //union segment
    SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);
  }

  @Test
  public void checkGetResult() {
    final int k = 1024;
    final UpdateSketch sk = UpdateSketch.builder().build();

    final int segBytes = getMaxUnionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iSeg = MemorySegment.ofArray(segArr);

    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(iSeg);
    union.union(sk);
    final CompactSketch csk = union.getResult();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test
  public void checkPrimitiveUpdates() {
    final int k = 32;
    final MemorySegment uSeg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uSeg);

    union.update(1L);
    union.update(1.5); //#1 double
    union.update(0.0);
    union.update(-0.0);
    String s = null;
    union.update(s); //null string
    s = "";
    union.update(s); //empty string
    s = "String";
    union.update(s); //#2 actual string
    byte[] byteArr = null;
    union.update(byteArr); //null byte[]
    byteArr = new byte[0];
    union.update(byteArr); //empty byte[]
    byteArr = "Byte Array".getBytes(UTF_8);
    union.update(byteArr); //#3 actual byte[]
    union.update(ByteBuffer.wrap(byteArr)); // same as previous
    union.update(ByteBuffer.wrap(byteArr, 0, 4)); // #4 byte slice
    int[] intArr = null;
    union.update(intArr); //null int[]
    intArr = new int[0];
    union.update(intArr); //empty int[]
    final int[] intArr2 = { 1, 2, 3, 4, 5 };
    union.update(intArr2); //#5 actual int[]
    long[] longArr = null;
    union.update(longArr); //null long[]
    longArr = new long[0];
    union.update(longArr); //empty long[]
    final long[] longArr2 = { 6, 7, 8, 9 };
    union.update(longArr2); //#6 actual long[]
    final CompactSketch comp = union.getResult();
    final double est = comp.getEstimate();
    final boolean empty = comp.isEmpty();
    assertEquals(est, 8.0, 0.0);
    assertFalse(empty);
  }

  @Test
  public void checkGetFamily() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 +32]);
    final SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, seg);
    assertEquals(setOp.getFamily(), Family.UNION);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreambleLongsCorruption() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 +32]);

    final SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, seg);
    println(setOp.toString());
    final int familyID = PreambleUtil.extractFamilyID(seg);
    final int preLongs = ThetaSketch.getPreambleLongs(seg);
    assertEquals(familyID, Family.UNION.getID());
    assertEquals(preLongs, Family.UNION.getMaxPreLongs());
    PreambleUtil.insertPreLongs(seg, 3); //Corrupt with 3; correct value is 4
    DirectQuickSelectSketch.writableWrap(seg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizeTooSmall() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 +32]); //initialized
    final SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, seg);
    println(setOp.toString());
    final MemorySegment seg2 = MemorySegment.ofArray(new byte[32]); //for just preamble
    MemorySegment.copy(seg, 0, seg2, 0, 32); //too small
    DirectQuickSelectSketch.writableWrap(seg2, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkForDruidBug() {
    final int k = 16384;
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 100000; i++) {
      usk.update(Integer.toString(i));
    }
    usk.rebuild(); //optional but created the symptom

    final ThetaSketch s = usk.compact();

    //create empty target union in off-heap segment
    final MemorySegment seg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union1 = SetOperation.builder().setNominalEntries(k).buildUnion(seg);

    union1.union(s);

    final CompactSketch csk = union1.getResult();

    assertTrue(csk.getTheta() < 0.2);
    assertEquals(csk.getRetainedEntries(true), 16384);
    final double est = csk.getEstimate();
    assertTrue(est > 98663.0);
    assertTrue(est < 101530.0);
  }

  @Test
  public void checkForDruidBug2() { //update union with just sketch segment reference
    final int k = 16384;
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 100000; i++) {
      usk.update(Integer.toString(i));
    }
    usk.rebuild(); //optional but created the symptom
    final MemorySegment segIn = MemorySegment.ofArray(new byte[usk.getCompactBytes()]);
    usk.compact(true, segIn); //side effect of loading the segIn

    //create empty target union in off-heap segment
    final MemorySegment seg = MemorySegment.ofArray(new byte[getMaxUnionBytes(k)]);
    final Union union1 = SetOperation.builder().setNominalEntries(k).buildUnion(seg);

    union1.union(segIn);

    final CompactSketch csk = union1.getResult();

    assertTrue(csk.getTheta() < 0.2);
    assertEquals(csk.getRetainedEntries(true), 16384);
    final double est = csk.getEstimate();
    assertTrue(est > 98663.0);
    assertTrue(est < 101530.0);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //Disable here
  }

}
