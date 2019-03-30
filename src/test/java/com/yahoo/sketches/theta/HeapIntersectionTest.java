/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
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
public class HeapIntersectionTest {

  @Test
  public void checkExactIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(k/2); i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    Intersection inter = SetOperation.builder().buildIntersection();

    inter.update(usk1);
    inter.update(usk2);

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

    assertFalse(inter.isSameResource(mem));
  }

  @Test
  public void checkExactIntersectionFullOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
    }
    for (int i=0; i<k; i++) {
      usk2.update(i);
    }

    Intersection inter = SetOperation.builder().buildIntersection();
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
    for (int i=u/2; i<(u + (u/2)); i++)
     {
      usk2.update(i); //inter 512
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Intersection inter = SetOperation.builder().buildIntersection();
    inter.update(csk1);
    inter.update(csk2);

    CompactSketch rsk1 = inter.getResult(true, null);
    double result = rsk1.getEstimate();
    double sd2err = (2048 * 2.0)/Math.sqrt(k);
    //println("2048 = " + rsk1.getEstimate() + " +/- " + sd2err);
    assertEquals(result, 2048.0, sd2err);
  }

  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = SketchesStateException.class)
  public void checkNoCall() {
    Intersection inter = SetOperation.builder().buildIntersection();
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

    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = empty
    sk = UpdateSketch.builder().setNominalEntries(k).build(); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = valid and not empty
    sk = UpdateSketch.builder().setNominalEntries(k).build();
    sk.update(1);
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
  }

  @Test
  public void check2ndCallAfterNull() {
    Intersection inter;
    UpdateSketch sk;
    CompactSketch comp1;
    double est;

    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = null
    inter = SetOperation.builder().buildIntersection();
    inter.update(null);
    //2nd call = empty
    sk = UpdateSketch.builder().build(); //empty
    inter.update(sk);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = null
    inter = SetOperation.builder().buildIntersection();
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
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.update(sk1);
    //2nd call = null
    inter.update(null);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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
    inter = SetOperation.builder().buildIntersection();
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

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection();
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

  @Test
  public void checkHeapifyAndWrap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2 * k); i++) {
      sk1.update(i);  //est mode
    }
    CompactSketch cSk1 = sk1.compact(true, null);
    double cSk1Est = cSk1.getEstimate();
    println("cSk1Est: " + cSk1Est);

    Intersection inter = SetOperation.builder().buildIntersection();
    //1st call with a valid sketch
    inter.update(cSk1);

    UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2 * k); i++) {
      sk2.update(i);  //est mode
    }
    CompactSketch cSk2 = sk2.compact(true, null);
    double cSk2Est = cSk2.getEstimate();
    println("cSk2Est: " + cSk2Est);
    assertEquals(cSk2Est, cSk1Est, 0.0);

    //2nd call with identical valid sketch
    inter.update(cSk2);
    CompactSketch interResultCSk1 = inter.getResult(false, null);
    double inter1est = interResultCSk1.getEstimate();
    assertEquals(inter1est, cSk1Est, 0.0);
    println("Inter1Est: " + inter1est);

    //Put the intersection into memory
    byte[] byteArray = inter.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //Heapify
    Intersection inter2 = (Intersection) SetOperation.heapify(mem);
    CompactSketch heapifiedSk = inter2.getResult(false, null);
    double heapifiedEst = heapifiedSk.getEstimate();
    assertEquals(heapifiedEst, cSk1Est, 0.0);
    println("HeapifiedEst: "+heapifiedEst);

    //Wrap
    Intersection inter3 = Sketches.wrapIntersection(mem);
    CompactSketch wrappedSk = inter3.getResult(false, null);
    double wrappedEst = wrappedSk.getEstimate();
    assertEquals(wrappedEst, cSk1Est, 0.0);
    println("WrappedEst: "+ wrappedEst);

    inter.reset();
    inter2.reset();
    inter3.reset();
  }

  @Test
  public void checkHeapifyNullEmpty() {
    Intersection inter1, inter2;

    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory srcMem = Memory.wrap(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    //inter2 is in virgin state, empty = false

    inter1.update(null);  //A virgin intersection intersected with null => empty = true;
    byteArray = inter1.toByteArray(); //update the byteArray

    srcMem = Memory.wrap(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertTrue(comp.isEmpty());
  }

  @Test
  public void checkHeapifyNullEmpty2() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    Memory srcMem = Memory.wrap(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    //inter2 is in virgin state

    sk1 = UpdateSketch.builder().setP((float) .005).setFamily(Family.QUICKSELECT).setNominalEntries(k).build();
    sk1.update(1);
    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false!

    inter1.update(sk1);
    byteArray = inter1.toByteArray();
    srcMem = Memory.wrap(byteArray);
    inter2 = (Intersection) SetOperation.heapify(srcMem);
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 2); //RF not used = 0
    SetOperation.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    byte[] byteArray = inter1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putByte(SER_VER_BYTE, (byte) 2);
    SetOperation.heapify(mem);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    int k = 32;
    Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    byte[] byteArray = union.toByteArray();
    Memory mem = Memory.wrap(byteArray);
    Intersection inter1 = (Intersection) SetOperation.heapify(mem); //bad cast
    println(inter1.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadEmptyState() {
    Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    inter1.update(sk); //initializes to a true empty intersection.
    byte[] byteArray = inter1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putInt(RETAINED_ENTRIES_INT, 1);
    SetOperation.heapify(mem);
  }

  @Test
  public void checkEmpty() {
    UpdateSketch usk = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.update(usk);
    assertTrue(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(true), 0);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    long[] longArr = inter.getCache();
    assertEquals(longArr.length, 0);
  }

  @Test
  public void checkOne() {
    UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.update(usk);
    assertFalse(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(true), 1);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    long[] longArr = inter.getCache();
    assertEquals(longArr.length, 32);
  }

  @Test
  public void checkGetResult() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.update(sk);
    CompactSketch csk = inter.getResult();
    assertEquals(csk.getCurrentBytes(true), 8);
  }

  @Test
  public void checkFamily() {
    IntersectionImpl impl = IntersectionImpl.initNewHeapInstance(DEFAULT_UPDATE_SEED);
    assertEquals(impl.getFamily(), Family.INTERSECTION);
  }

  @Test
  public void checkPairIntersectSimple() {
    UpdateSketch skA = Sketches.updateSketchBuilder().build();
    UpdateSketch skB = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    CompactSketch csk = inter.intersect(skA, skB);
    assertEquals(csk.getCurrentBytes(true), 8);
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
