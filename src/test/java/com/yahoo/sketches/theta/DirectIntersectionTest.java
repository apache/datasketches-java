/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.SetOperation.CONST_PREAMBLE_LONGS;
import static com.yahoo.sketches.theta.SetOperation.getMaxIntersectionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
public class DirectIntersectionTest {
  private static final int PREBYTES = CONST_PREAMBLE_LONGS << 3; //24

  @Test
  public void checkExactIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(k/2); i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter = SetOperation.builder().buildIntersection(iMem);

    inter.update(usk1);
    inter.update(usk2);

    long[] cache = inter.getCache();
    assertEquals(cache.length, 32);

    CompactSketch rsk1;
    boolean ordered = true;

    assertTrue(inter.hasResult());
    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    boolean compact = true;
    int bytes = rsk1.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArray);

    rsk1 = inter.getResult(!ordered, mem);
    assertEquals(rsk1.getEstimate(), 0.0);
    //executed twice to fully exercise the internal state machine
    rsk1 = inter.getResult(ordered, mem);
    assertEquals(rsk1.getEstimate(), 0.0);
  }

  @Test
  public void checkExactIntersectionFullOverlap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
    }
    for (int i=0; i<k; i++) {
      usk2.update(i);
    }

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(usk1);
    inter.update(usk2);

    CompactSketch rsk1;
    boolean ordered = true;

    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);

    boolean compact = true;
    int bytes = rsk1.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArray);

    rsk1 = inter.getResult(!ordered, mem); //executed twice to fully exercise the internal state machine
    assertEquals(rsk1.getEstimate(), (double)k);

    rsk1 = inter.getResult(ordered, mem);
    assertEquals(rsk1.getEstimate(), (double)k);
  }

  @Test
  public void checkIntersectionEarlyStop() {
    int lgK = 10;
    int k = 1<<lgK;
    int u = 4*k;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }
    for (int i=u/2; i<(u + (u/2)); i++) {
      usk2.update(i);
    }

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Intersection inter =
        SetOperation.builder().buildIntersection(iMem);
    inter.update(csk1);
    inter.update(csk2);

    CompactSketch rsk1 = inter.getResult(true, null);
    println(""+rsk1.getEstimate());
  }

  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = SketchesStateException.class)
  public void checkNoCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter = SetOperation.builder().buildIntersection(iMem);
    assertFalse(inter.hasResult());
    inter.getResult(false, null);
  }

  @Test
  public void check1stCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch rsk1;
    double est;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = null
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(null);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = empty
    sk = UpdateSketch.builder().setNominalEntries(k).build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = valid and not empty
    sk = UpdateSketch.builder().setNominalEntries(k).build();
    sk.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
  }

  @Test
  public void check2ndCallAfterNull() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch comp1;
    double est;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = null
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(null);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = null
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(null);
    //2nd call = empty
    sk = UpdateSketch.builder().build(); //empty
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = null
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(null);
    //2nd call = valid & not empty
    sk = UpdateSketch.builder().build();
    sk.update(1);
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }

  @Test
  public void check2ndCallAfterEmpty() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = valid and not empty
    sk2 = UpdateSketch.builder().build();
    sk2.update(1);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }

  @Test
  public void check2ndCallAfterValid() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(1);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
    //2nd call = valid not intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(2);
    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);
  }

  @Test
  public void checkEstimatingIntersect() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk2.update(i);  //est mode
    }
    println("sk2: "+sk2.getEstimate());

    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkOverflow() {
    int lgK = 9; //512
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    int reqBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[reqBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(2 * k).build(); // bigger sketch
    for (int i=0; i<(4*k); i++)
     {
      sk1.update(i);  //force est mode
    }
    println("sk1est: "+sk1.getEstimate());
    println("sk1cnt: "+sk1.getRetainedEntries(true));

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);
  }

  @Test
  public void checkHeapify() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1, comp2;
    double est, est2;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(sk1);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk2.update(i);  //est mode
    }
    println("sk2: "+sk2.getEstimate());

    inter.update(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);

    byte[] byteArray = inter.toByteArray();
    Memory mem = Memory.wrap(byteArray);
    Intersection inter2 = (Intersection) SetOperation.heapify(mem);
    comp2 = inter2.getResult(false, null);
    est2 = comp2.getEstimate();
    println("Est2: "+est2);
  }

  @Test
  public void checkWrapNullEmpty() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    inter2 = Sketches.wrapIntersection(iMem);
    //both in virgin state, empty = false
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());

    inter1.update(null);  //A virgin intersection intersected with null => empty = true;
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter1.hasResult());
    assertTrue(inter2.hasResult());
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertTrue(comp.isEmpty());
  }

  @Test
  public void checkWrapNullEmpty2() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    inter2 = Sketches.wrapIntersection(iMem);
    //both in virgin state, empty = false
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());

    sk1 = UpdateSketch.builder().setP((float) .005).setFamily(Family.QUICKSELECT).setNominalEntries(k).build();
    sk1.update(1); //very unlikely to go into cache due to p.
    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false.

    inter1.update(sk1);
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter1.hasResult());
    assertTrue(inter2.hasResult());
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizeLowerLimit() {
    int k = 8;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    SetOperation.builder().buildIntersection(iMem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizedTooSmall() {
    int lgK = 5;
    int k = 1<<lgK;
    int u = 4*k;

    int memBytes = getMaxIntersectionBytes(k/2);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }

    CompactSketch csk1 = usk1.compact(true, null);

    Intersection inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(csk1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    int k = 32;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    Intersection inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    byte[] byteArray = inter1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2);//RF not used = 0
    Sketches.wrapIntersection(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 32;
    Intersection inter1;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    byte[] byteArray = inter1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putByte(SER_VER_BYTE, (byte) 2);
    Sketches.wrapIntersection(mem); //throws in SetOperations
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    int k = 32;
    Union union;

    union = SetOperation.builder().setNominalEntries(k).buildUnion();
    byte[] byteArray = union.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    Sketches.wrapIntersection(mem);
  }

  @Test
  public void checkWrap() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter, inter2, inter3;
    UpdateSketch sk1, sk2;
    CompactSketch resultComp1, resultComp2;
    double est, est2;

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr1 = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr1);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk1.update(i);  //est mode
    }
    CompactSketch compSkIn1 = sk1.compact(true, null);
    println("compSkIn1: "+compSkIn1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.update(compSkIn1);

    byte[] memArr2 = inter.toByteArray();
    WritableMemory srcMem = WritableMemory.wrap(memArr2);
    inter2 = Sketches.wrapIntersection(srcMem);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk2.update(i);  //est mode
    }
    CompactSketch compSkIn2 = sk2.compact(true, null);
    println("sk2: "+compSkIn2.getEstimate());

    inter2.update(compSkIn2);
    resultComp1 = inter2.getResult(false, null);
    est = resultComp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);

    byte[] memArr3 = inter2.toByteArray();
    WritableMemory srcMem2 = WritableMemory.wrap(memArr3);
    inter3 = Sketches.wrapIntersection(srcMem2);
    resultComp2 = inter3.getResult(false, null);
    est2 = resultComp2.getEstimate();
    println("Est2: "+est2);

    inter.reset();
    inter2.reset();
    inter3.reset();
  }

  @Test
  public void checkDefaultMinSize() {
    int k = 32;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptionMinSize() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
  }

  @Test
  public void checkGetResult() {
    int k = 1024;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

    Intersection inter = Sketches.setOperationBuilder().buildIntersection(iMem);
    inter.update(sk);
    CompactSketch csk = inter.getResult();
    assertEquals(csk.getCurrentBytes(true), 8);
  }

  @Test
  public void checkFamily() {
    //cheap trick
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + PREBYTES]);
    IntersectionImplR impl = IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
    assertEquals(impl.getFamily(), Family.INTERSECTION);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptions1() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
    //corrupt SerVer
    mem.putByte(PreambleUtil.SER_VER_BYTE, (byte) 2);
    IntersectionImplR.wrapInstance(mem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptions2() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
    //mem now has non-empty intersection
    //corrupt empty and CurCount
    mem.setBits(PreambleUtil.FLAGS_BYTE, (byte) PreambleUtil.EMPTY_FLAG_MASK);
    mem.putInt(PreambleUtil.RETAINED_ENTRIES_INT, 2);
    IntersectionImplR.wrapInstance(mem, DEFAULT_UPDATE_SEED);
  }

  //Check Alex's bug intersecting 2 direct full sketches with only overlap of 2
  //
  @Test
  public void checkOverlappedDirect() {
    int k = 1 << 4;
    int memBytes = (2*k*16) +PREBYTES; //plenty of room
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    UpdateSketch sk2 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sk1.update(i);
      sk2.update((k-2) +i); //overlap by 2
    }
    WritableMemory memIn1 = WritableMemory.wrap(new byte[memBytes]);
    WritableMemory memIn2 = WritableMemory.wrap(new byte[memBytes]);
    WritableMemory memOut = WritableMemory.wrap(new byte[memBytes]);
    CompactSketch csk1 = sk1.compact(true, memIn1);
    CompactSketch csk2 = sk2.compact(true, memIn2);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection(memOut);
    inter.update(csk1);
    inter.update(csk2);
    CompactSketch cskOut = inter.getResult(true, memOut);
    assertEquals(cskOut.getEstimate(), 2.0, 0.0);
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
