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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class HeapUnionTest {

  @Test
  public void checkExactUnionNoOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);
  }

  @Test
  public void checkEstUnionNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //2*k
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2*k no overlap
    }

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.05);
  }

  @Test
  public void checkExactUnionWithOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //256
    }
    for (int i=0; i<u  ; i++) {
      usk2.update(i); //512, 256 overlapped
    }

    assertEquals(u, usk1.getEstimate() + (usk2.getEstimate()/2), 0.0); //exact, overlapped

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);
  }

  @Test
  public void checkHeapifyExact() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.0);
  }

  @Test
  public void checkHeapifyEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //2k
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2k no overlap, exact
    }

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch, early stop not possible

    testAllCompactForms(union, u, 0.05);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);
  }

  @Test
  public void checkHeapifyEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++) {
      usk1.update(i); //2k
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i); //2k no overlap, exact, will force early stop
    }

    CompactSketch cosk2 = usk2.compact(true, null);

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1);  //update with heap UpdateSketch
    union.update(cosk2); //update with heap Compact, Ordered input, early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty
    emptySketch = null;
    union.update(emptySketch); //updates with null

    testAllCompactForms(union, u, 0.05);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedDirectIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++) {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCompactBytes()]);
    CompactSketch cosk2 = usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cosk2);       //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkHeapifyEstNoOverlapOrderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++) {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCompactBytes()]);
    usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkHeapifyEstNoOverlapUnorderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++) {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++) {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCompactBytes()]);
    usk2.compact(false, cskMem2); //unordered, loads the cskMem2 as unordered

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = (Union)SetOperation.heapify(Memory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkMultiUnion() {
    int lgK = 13; //8192
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk3 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk4 = UpdateSketch.builder().setNominalEntries(k).build();

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

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(usk1); //updates with heap UpdateSketch
    union.update(usk2); //updates with heap UpdateSketch
    union.update(usk3); //updates with heap UpdateSketch
    union.update(usk4); //updates with heap UpdateSketch

    CompactSketch csk = union.getResult(true, null);
    double est = csk.getEstimate();
    assertEquals(est, v, .01*v);
  }

  @Test
  public void checkDirectMemoryIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop
    int totU = u1+u2;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u1; i++) {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++) {
      usk2.update(i); //2*k + 1024 no overlap
    }

    WritableMemory skMem1 = WritableMemory.wrap(usk1.compact(false, null).toByteArray());
    WritableMemory skMem2 = WritableMemory.wrap(usk2.compact(true, null).toByteArray());

    CompactSketch csk1 = (CompactSketch)Sketch.wrap(skMem1);
    CompactSketch csk2 = (CompactSketch)Sketch.wrap(skMem2);

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(csk1);
    union.update(csk2);

    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }

  @Test
  public void checkSerVer1Handling() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop
    int totU = u1+u2;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u1; i++) {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++) {
      usk2.update(i); //2*k + 1024 no overlap
    }

    Memory v1mem1 = convertSerVer3toSerVer1(usk1.compact(true, null));
    Memory v1mem2 = convertSerVer3toSerVer1(usk2.compact(true, null));

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(v1mem1);
    union.update(v1mem2);

    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }

  @Test
  public void checkSerVer2Handling() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u1 = 2*k;
    int u2 = 1024; //smaller exact sketch forces early stop
    int totU = u1+u2;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u1; i++) {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++) {
      usk2.update(i); //2*k + 1024 no overlap
    }

    Memory v2mem1 = convertSerVer3toSerVer2(usk1.compact(true, null), Util.DEFAULT_UPDATE_SEED);
    Memory v2mem2 = convertSerVer3toSerVer2(usk2.compact(true, null), Util.DEFAULT_UPDATE_SEED);

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

    union.update(v2mem1);
    union.update(v2mem2);

    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), totU, .05*k);
  }

  @Test
  public void checkUpdateMemorySpecialCases() {
    int lgK = 12; //4096
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    CompactSketch usk1c = usk1.compact(true, null);
    WritableMemory v3mem1 = WritableMemory.wrap(usk1c.toByteArray());

    Memory v1mem1 = convertSerVer3toSerVer1(usk1.compact(true, null));

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    union.update(v1mem1);
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    Memory v2mem1 = convertSerVer3toSerVer2(usk1.compact(true, null), Util.DEFAULT_UPDATE_SEED);

    union = SetOperation.builder().setNominalEntries(k).buildUnion();
    union.update(v2mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    union = SetOperation.builder().setNominalEntries(k).buildUnion();
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    union = SetOperation.builder().setNominalEntries(k).buildUnion();
    v3mem1 = null;
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkUpdateMemorySpecialCases2() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 2*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++)
     {
      usk1.update(i); //force prelongs to 3
    }
    CompactSketch usk1c = usk1.compact(true, null);
    WritableMemory v3mem1 = WritableMemory.wrap(usk1c.toByteArray());
    //println(PreambleUtil.toString(v3mem1));
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    union.update(v3mem1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemBadSerVer() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    usk1.update(1);
    usk1.update(2);
    CompactSketch usk1c = usk1.compact(true, null);
    WritableMemory v3mem1 = WritableMemory.wrap(usk1c.toByteArray());
    //corrupt SerVer
    v3mem1.putByte(SER_VER_BYTE, (byte)0);

    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    union.update(v3mem1);
  }

  @Test
  public void checkEmptySerVer2and3() {
    UpdateSketch usk1 = UpdateSketch.builder().build();
    CompactSketch usk1c = usk1.compact(true, null);
    byte[] skArr = usk1c.toByteArray();
    byte[] skArr2 = Arrays.copyOf(skArr, skArr.length * 2);
    WritableMemory v3mem1 = WritableMemory.wrap(skArr2);

    Union union = SetOperation.builder().buildUnion();
    union.update(v3mem1);

    Memory v2mem1 = convertSerVer3toSerVer2(usk1c, Util.DEFAULT_UPDATE_SEED);
    WritableMemory v2mem2 = WritableMemory.wrap(new byte[16]);
    v2mem1.copyTo(0, v2mem2, 0, 8);

    union = SetOperation.builder().buildUnion();
    union.update(v2mem2);
  }

  @Test
  public void checkGetResult() {
    int k = 1024;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(sk);
    CompactSketch csk = union.getResult();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test
  public void checkPrimitiveUpdates() {
    int k = 32;
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();

    union.update(1L);   //#1 long
    union.update(1.5);  //#2 double
    union.update(0.0);
    union.update(-0.0); //#3 double
    String s = null;
    union.update(s);    //null string
    s = "";
    union.update(s);    //empty string
    s = "String";
    union.update(s);    //#4 actual string

    byte[] byteArr = null;
    union.update(byteArr); //null byte[]
    byteArr = new byte[0];
    union.update(byteArr); //empty byte[]
    byteArr = "Byte Array".getBytes(UTF_8);
    union.update(byteArr); //#5 actual byte[]

    char[] charArr = null;
    union.update(charArr); //null char[]
    charArr = new char[0];
    union.update(charArr); //empty char[]
    charArr = "String".toCharArray();
    union.update(charArr); //#6 actual char[]

    int[] intArr = null;
    union.update(intArr); //null int[]
    intArr = new int[0];
    union.update(intArr); //empty int[]
    int[] intArr2 = { 1, 2, 3, 4, 5 };
    union.update(intArr2); //#7 actual int[]

    long[] longArr = null;
    union.update(longArr); //null long[]
    longArr = new long[0];
    union.update(longArr); //empty long[]
    long[] longArr2 = { 6, 7, 8, 9 };
    union.update(longArr2); //#8 actual long[]

    CompactSketch comp = union.getResult();
    double est = comp.getEstimate();
    boolean empty = comp.isEmpty();
    assertEquals(est, 8.0, 0.0);
    assertFalse(empty);
  }

  //used by DirectUnionTest as well
  public static void testAllCompactForms(Union union, double expected, double toll) {
    double compEst1, compEst2;
    compEst1 = union.getResult(false, null).getEstimate(); //not ordered, no mem
    assertEquals(compEst1, expected, toll*expected);

    CompactSketch comp2 = union.getResult(true, null); //ordered, no mem
    compEst2 = comp2.getEstimate();
    assertEquals(compEst2, compEst1, 0.0);

    WritableMemory mem = WritableMemory.wrap(new byte[comp2.getCurrentBytes()]);

    compEst2 = union.getResult(false, mem).getEstimate(); //not ordered, mem
    assertEquals(compEst2, compEst1, 0.0);

    compEst2 = union.getResult(true, mem).getEstimate(); //ordered, mem
    assertEquals(compEst2, compEst1, 0.0);
  }

  @Test
  public void checkGetFamily() {
    SetOperation setOp = new SetOperationBuilder().build(Family.UNION);
    assertEquals(setOp.getFamily(), Family.UNION);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //Disable here
  }
}
