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
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class HeapIntersectionTest {

  @SuppressWarnings("deprecation")
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

    inter.intersect(usk1);
    inter.update(usk2); //check deprecated

    CompactSketch rsk1;
    boolean ordered = true;

    assertTrue(inter.hasResult());
    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    int bytes = rsk1.getCompactBytes();
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
    inter.intersect(usk1);
    inter.intersect(usk2);

    CompactSketch rsk1;
    boolean ordered = true;

    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), (double)k);

    int bytes = rsk1.getCompactBytes();
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
      usk2.update(i);
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(csk1);
    inter.intersect(csk2);

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
  public void checkIntersectionNull() {
    Intersection inter = SetOperation.builder().buildIntersection();
    UpdateSketch sk = null;
    try { inter.intersect(sk); fail(); }
    catch (SketchesArgumentException e) { }

    try { inter.intersect(sk, sk); fail(); }
    catch (SketchesArgumentException e) { }
  }

  @Test
  public void check1stCall() {
    int lgK = 9;
    int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk;
    CompactSketch rsk1;
    double est;

    //1st call = empty
    sk = UpdateSketch.builder().setNominalEntries(k).build(); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = valid and not empty
    sk = UpdateSketch.builder().setNominalEntries(k).build();
    sk.update(1);
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
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
    inter.intersect(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.intersect(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk1);
    //2nd call = valid and not empty
    sk2 = UpdateSketch.builder().build();
    sk2.update(1);
    inter.intersect(sk2);
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
    inter.intersect(sk1);
    //2nd call = empty
    sk2 = UpdateSketch.builder().build(); //empty
    inter.intersect(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk1);
    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(1);
    inter.intersect(sk2);
    comp1 = inter.getResult(false, null);
    est = comp1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk1);
    //2nd call = valid not intersecting
    sk2 = UpdateSketch.builder().build(); //empty
    sk2.update(2);
    inter.intersect(sk2);
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
    inter.intersect(sk1);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(2*k); i++)
     {
      sk2.update(i);  //est mode
    }
    println("sk2: "+sk2.getEstimate());

    inter.intersect(sk2);
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
    inter.intersect(cSk1);

    UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < (2 * k); i++) {
      sk2.update(i);  //est mode
    }
    CompactSketch cSk2 = sk2.compact(true, null);
    double cSk2Est = cSk2.getEstimate();
    println("cSk2Est: " + cSk2Est);
    assertEquals(cSk2Est, cSk1Est, 0.0);

    //2nd call with identical valid sketch
    inter.intersect(cSk2);
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


  /**
   * This proves that the hash of 7 is < 0.5. This fact will be used in other tests involving P.
   */
  @Test
  public void checkPreject() {
    UpdateSketch sk = UpdateSketch.builder().setP((float) .5).build();
    sk.update(7);
    assertEquals(sk.getRetainedEntries(), 0);
  }

  @Test
  public void checkHeapifyVirginEmpty() {
    int lgK = 5;
    int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    inter1 = SetOperation.builder().buildIntersection(); //virgin heap
    Memory srcMem = Memory.wrap(inter1.toByteArray());
    inter2 = (Intersection) SetOperation.heapify(srcMem); //virgin heap, independent of inter1
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());

    //This constructs a sketch with 0 entries and theta < 1.0
    sk1 = UpdateSketch.builder().setP((float) .5).setNominalEntries(k).build();
    sk1.update(7); //will be rejected by P, see proof above.

    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false!, but has a result.
    inter1.intersect(sk1);
    assertFalse(inter1.isEmpty());
    assertTrue(inter1.hasResult());
    //note that inter2 is independent
    assertFalse(inter2.hasResult());

    //test the path via toByteArray, heapify, now in a different state
    srcMem = Memory.wrap(inter1.toByteArray());
    inter2 = (Intersection) SetOperation.heapify(srcMem); //inter2 identical to inter1
    assertFalse(inter2.isEmpty());
    assertTrue(inter2.hasResult());

    //test the compaction path
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
    @SuppressWarnings("unused")
    Intersection inter1 = (Intersection) SetOperation.heapify(mem); //bad cast
    //println(inter1.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadEmptyState() {
    Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    inter1.intersect(sk); //initializes to a true empty intersection.
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
    inter.intersect(usk);
    assertTrue(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(), 0);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    long[] longArr = inter.getCache(); //only applies to stateful
    assertEquals(longArr.length, 0);
  }

  @Test
  public void checkOne() {
    UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(usk);
    assertFalse(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(), 1);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    long[] longArr = inter.getCache(); //only applies to stateful
    assertEquals(longArr.length, 32);
  }

  @Test
  public void checkGetResult() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();

    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(sk);
    CompactSketch csk = inter.getResult();
    assertEquals(csk.getCompactBytes(), 8);
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
    assertEquals(csk.getCompactBytes(), 8);
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
