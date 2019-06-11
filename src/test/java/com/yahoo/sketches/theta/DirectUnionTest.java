/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.ForwardCompatibilityTest.convertSerV3toSerV1;
import static com.yahoo.sketches.theta.ForwardCompatibilityTest.convertSerV3toSerV2;
import static com.yahoo.sketches.theta.HeapUnionTest.testAllCompactForms;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.SetOperation.getMaxUnionBytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class DirectUnionTest {

  @Test
  public void checkExactUnionNoOverlap() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //2*k
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //2*k no overlap
    }

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //256
    }
    for (int i=0; i<u  ; i++)
     {
      usk2.update(i); //512, 256 overlapped
    }

    assertEquals(u, usk1.getEstimate() + (usk2.getEstimate()/2), 0.0); //exact, overlapped

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);

    Union union2 = (Union)SetOperation.heapify(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.0);
  }

  //these parallel the checkHeapifyExact, etc.
  @Test
  public void checkWrapExact() {
    int lgK = 9; //512
    int k = 1 << lgK;
    int u = k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //256
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //256 no overlap
    }

    assertEquals(u, usk1.getEstimate() + usk2.getEstimate(), 0.0); //exact, no overlap

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch

    testAllCompactForms(union, u, 0.0);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlap() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //2k
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //2k no overlap, exact
    }

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1); //update with heap UpdateSketch
    union.update(usk2); //update with heap UpdateSketch, early stop not possible

    testAllCompactForms(union, u, 0.05);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //2k estimating
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i); //2k no overlap, exact, will force early stop
    }

    CompactSketch cosk2 = usk2.compact(true, null);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1);  //update with heap UpdateSketch
    union.update(cosk2); //update with heap Compact, Ordered input, early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty
    emptySketch = null;
    union.update(emptySketch); //updates with null

    testAllCompactForms(union, u, 0.05);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedDirectIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build(); //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i); //2k estimating
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCurrentBytes(true)]);
    CompactSketch cosk2 = usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1);      //updates with heap UpdateSketch
    union.update(cosk2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapOrderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCurrentBytes(true)]);
    usk2.compact(true, cskMem2); //ordered, loads the cskMem2 as ordered

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

    testAllCompactForms(union2, u, 0.05);

    union2.reset();
    assertEquals(union2.getResult(true, null).getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkWrapEstNoOverlapUnorderedMemIn() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();   //2k estimating
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(2 * k).build(); //2k exact for early stop test

    for (int i=0; i<(u/2); i++)
     {
      usk1.update(i);  //2k estimating
    }
    for (int i=u/2; i<u; i++)
     {
      usk2.update(i);  //2k no overlap, exact, will force early stop
    }

    WritableMemory cskMem2 = WritableMemory.wrap(new byte[usk2.getCurrentBytes(true)]);
    usk2.compact(false, cskMem2); //unordered, loads the cskMem2 as unordered

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(usk1);        //updates with heap UpdateSketch
    union.update(cskMem2);     //updates with direct CompactSketch, ordered, use early stop

    UpdateSketch emptySketch = UpdateSketch.builder().setNominalEntries(k).build();
    union.update(emptySketch); //updates with empty sketch
    emptySketch = null;
    union.update(emptySketch); //updates with null sketch

    testAllCompactForms(union, u, 0.05);

    Union union2 = Sketches.wrapUnion(WritableMemory.wrap(union.toByteArray()));

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

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<u1; i++)
     {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++)
     {
      usk2.update(i); //2*k + 1024 no overlap
    }

    Memory skMem1 = Memory.wrap(usk1.compact(false, null).toByteArray());
    Memory skMem2 = Memory.wrap(usk2.compact(true, null).toByteArray());

    CompactSketch csk1 = (CompactSketch)Sketch.wrap(skMem1);
    CompactSketch csk2 = (CompactSketch)Sketch.wrap(skMem2);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<u1; i++)
     {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++)
     {
      usk2.update(i); //2*k + 1024 no overlap
    }

    WritableMemory skMem1 = WritableMemory.wrap(usk1.compact(true, null).toByteArray());
    WritableMemory skMem2 = WritableMemory.wrap(usk2.compact(true, null).toByteArray());

    Memory v1mem1 = convertSerV3toSerV1(skMem1);
    Memory v1mem2 = convertSerV3toSerV1(skMem2);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    for (int i=0; i<u1; i++)
     {
      usk1.update(i); //2*k
    }
    for (int i=u1; i<totU; i++)
     {
      usk2.update(i); //2*k + 1024 no overlap
    }

    WritableMemory skMem1 = WritableMemory.wrap(usk1.compact(true, null).toByteArray());
    WritableMemory skMem2 = WritableMemory.wrap(usk2.compact(true, null).toByteArray());

    Memory v2mem1 = convertSerV3toSerV2(skMem1);
    Memory v2mem2 = convertSerV3toSerV2(skMem2);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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

    Memory v1mem1 = convertSerV3toSerV1(v3mem1);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v1mem1);
    CompactSketch cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    Memory v2mem1 = convertSerV3toSerV2(v3mem1);

    uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v2mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v3mem1);
    cOut = union.getResult(true, null);
    assertEquals(cOut.getEstimate(), 0.0, 0.0);

    uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
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

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v3mem1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemBadSerVer() {
    int lgK = 12; //4096
    int k = 1 << lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    CompactSketch usk1c = usk1.compact(true, null);
    WritableMemory v3mem1 = WritableMemory.wrap(usk1c.toByteArray());
    //corrupt SerVer
    v3mem1.putByte(SER_VER_BYTE, (byte)0);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    union.update(v3mem1);
  }

  @Test
  //where the granted mem is larger than required
  public void checkEmptySerVer2and3() {
    int lgK = 12; //4096
    int k = 1 << lgK;
    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    CompactSketch usk1c = usk1.compact(true, null);
    byte[] skArr = usk1c.toByteArray();
    byte[] skArr2 = Arrays.copyOf(skArr, skArr.length * 2);
    WritableMemory v3mem1 = WritableMemory.wrap(skArr2);

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v3mem1);

    Memory v2mem1 = convertSerV3toSerV2(v3mem1);
    WritableMemory v2mem2 = WritableMemory.wrap(new byte[16]);
    v2mem1.copyTo(0, v2mem2, 0, 8);

    uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
    union.update(v2mem2);
  }

  //Special DirectUnion cases
  @Test //Himanshu's issue
  public void checkDirectWrap() {
    int nomEntries = 16;
    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(nomEntries)]);
    SetOperation.builder().setNominalEntries(nomEntries).buildUnion(uMem);

    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(nomEntries).build();
    sk1.update("a");
    sk1.update("b");

    UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(nomEntries).build();
    sk2.update("c");
    sk2.update("d");

    Union union = Sketches.wrapUnion(uMem);
    union.update(sk1);

    union = Sketches.wrapUnion(uMem);
    union.update(sk2);

    CompactSketch sketch = union.getResult(true, null);
    assertEquals(4.0, sketch.getEstimate(), 0.0);
  }

  @Test
  public void checkEmptyUnionCompactResult() {
    int k = 64;

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    WritableMemory mem = WritableMemory.wrap(new byte[Sketch.getMaxCompactSketchBytes(0)]);
    CompactSketch csk = union.getResult(false, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }

  @Test
  public void checkEmptyUnionCompactOrderedResult() {
    int k = 64;

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

    WritableMemory mem = WritableMemory.wrap(new byte[Sketch.getMaxCompactSketchBytes(0)]);
    CompactSketch csk = union.getResult(true, mem); //DirectCompactSketch
    assertTrue(csk.isEmpty());
  }

  @Test
  public void checkUnionMemToString() {
    int k = 64;

    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]); //union memory
    SetOperation.builder().setNominalEntries(k).buildUnion(uMem);
  }

  @Test
  public void checkGetResult() {
    int k = 1024;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    int memBytes = getMaxUnionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(iMem);
    union.update(sk);
    CompactSketch csk = union.getResult();
    assertEquals(csk.getCurrentBytes(true), 8);
  }

  @Test
  public void checkPrimitiveUpdates() {
    int k = 32;
    WritableMemory uMem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion(uMem);

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
    int[] intArr = null;
    union.update(intArr); //null int[]
    intArr = new int[0];
    union.update(intArr); //empty int[]
    int[] intArr2 = { 1, 2, 3, 4, 5 };
    union.update(intArr2); //#4 actual int[]
    long[] longArr = null;
    union.update(longArr); //null long[]
    longArr = new long[0];
    union.update(longArr); //empty long[]
    long[] longArr2 = { 6, 7, 8, 9 };
    union.update(longArr2); //#5 actual long[]
    CompactSketch comp = union.getResult();
    double est = comp.getEstimate();
    boolean empty = comp.isEmpty();
    assertEquals(est, 7.0, 0.0);
    assertFalse(empty);
  }

  @Test
  public void checkGetFamily() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +32]);
    SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, mem);
    assertEquals(setOp.getFamily(), Family.UNION);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreambleLongsCorruption() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +32]);

    SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, mem);
    println(setOp.toString());
    int familyID = PreambleUtil.extractFamilyID(mem);
    int preLongs = PreambleUtil.extractPreLongs(mem);
    assertEquals(familyID, Family.UNION.getID());
    assertEquals(preLongs, Family.UNION.getMaxPreLongs());
    PreambleUtil.insertPreLongs(mem, 3); //Corrupt with 3; correct value is 4
    DirectQuickSelectSketch.writableWrap(mem, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizeTooSmall() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +32]); //initialized
    SetOperation setOp = new SetOperationBuilder().setNominalEntries(k).build(Family.UNION, mem);
    println(setOp.toString());
    WritableMemory mem2 = WritableMemory.wrap(new byte[32]); //for just preamble
    mem.copyTo(0, mem2, 0, 32); //too small
    DirectQuickSelectSketch.writableWrap(mem2, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkForDruidBug() {
    final int k = 16384;
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 100000; i++) {
      usk.update(Integer.toString(i));
    }
    usk.rebuild(); //optional but created the symptom

    final Sketch s = usk.compact();

    //create empty target union in off-heap mem
    final WritableMemory mem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union1 = SetOperation.builder().setNominalEntries(k).buildUnion(mem);

    union1.update(s);

    CompactSketch csk = union1.getResult();

    assertTrue(csk.getTheta() < 0.2);
    assertEquals(csk.getRetainedEntries(), 16384);
    final double est = csk.getEstimate();
    assertTrue(est > 98663.0);
    assertTrue(est < 101530.0);
  }

  @Test
  public void checkForDruidBug2() { //update union with just sketch memory reference
    final int k = 16384;
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 100000; i++) {
      usk.update(Integer.toString(i));
    }
    usk.rebuild(); //optional but created the symptom
    final WritableMemory memIn = WritableMemory.allocate(usk.getCurrentBytes(true));
    usk.compact(true, memIn); //side effect of loading the memIn

    //create empty target union in off-heap mem
    final WritableMemory mem = WritableMemory.wrap(new byte[getMaxUnionBytes(k)]);
    Union union1 = SetOperation.builder().setNominalEntries(k).buildUnion(mem);

    union1.update(memIn);

    CompactSketch csk = union1.getResult();

    assertTrue(csk.getTheta() < 0.2);
    assertEquals(csk.getRetainedEntries(), 16384);
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
  static void println(String s) {
    //System.out.println(s); //Disable here
  }

}
