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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.theta2.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.thetacommon2.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class HeapIntersectionTest {

  @Test
  public void checkExactIntersectionNoOverlap() {
    final int lgK = 9;
    final int k = 1<<lgK;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k/2; i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    final Intersection inter = SetOperation.builder().buildIntersection();

    inter.intersect(usk1);
    inter.intersect(usk2);

    CompactSketch rsk1;
    final boolean ordered = true;

    assertTrue(inter.hasResult());
    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), 0.0);

    final int bytes = rsk1.getCompactBytes();
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);

    rsk1 = inter.getResult(!ordered, seg);
    assertEquals(rsk1.getEstimate(), 0.0);

    //executed twice to fully exercise the internal state machine
    rsk1 = inter.getResult(ordered, seg);
    assertEquals(rsk1.getEstimate(), 0.0);

    assertFalse(inter.isSameResource(seg));
  }

  @Test
  public void checkExactIntersectionFullOverlap() {
    final int lgK = 9;
    final int k = 1<<lgK;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
    }
    for (int i=0; i<k; i++) {
      usk2.update(i);
    }

    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(usk1);
    inter.intersect(usk2);

    CompactSketch rsk1;
    final boolean ordered = true;

    rsk1 = inter.getResult(!ordered, null);
    assertEquals(rsk1.getEstimate(), k);

    rsk1 = inter.getResult(ordered, null);
    assertEquals(rsk1.getEstimate(), k);

    final int bytes = rsk1.getCompactBytes();
    final byte[] byteArray = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);

    rsk1 = inter.getResult(!ordered, seg); //executed twice to fully exercise the internal state machine
    assertEquals(rsk1.getEstimate(), k);

    rsk1 = inter.getResult(ordered, seg);
    assertEquals(rsk1.getEstimate(), k);
  }

  @Test
  public void checkIntersectionEarlyStop() {
    final int lgK = 10;
    final int k = 1<<lgK;
    final int u = 4*k;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }
    for (int i=u/2; i<u + u/2; i++)
     {
      usk2.update(i);
    }

    final CompactSketch csk1 = usk1.compact(true, null);
    final CompactSketch csk2 = usk2.compact(true, null);

    final Intersection inter = SetOperation.builder().buildIntersection();
    inter.intersect(csk1);
    inter.intersect(csk2);

    final CompactSketch rsk1 = inter.getResult(true, null);
    final double result = rsk1.getEstimate();
    final double sd2err = 2048 * 2.0/Math.sqrt(k);
    //println("2048 = " + rsk1.getEstimate() + " +/- " + sd2err);
    assertEquals(result, 2048.0, sd2err);
  }

  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = SketchesStateException.class)
  public void checkNoCall() {
    final Intersection inter = SetOperation.builder().buildIntersection();
    assertFalse(inter.hasResult());
    inter.getResult(false, null);
  }

  @Test
  public void checkIntersectionNull() {
    final Intersection inter = SetOperation.builder().buildIntersection();
    final UpdateSketch sk = null;
    try { inter.intersect(sk); fail(); }
    catch (final SketchesArgumentException e) { }

    try { inter.intersect(sk, sk); fail(); }
    catch (final SketchesArgumentException e) { }
  }

  @Test
  public void check1stCall() {
    final int lgK = 9;
    final int k = 1<<lgK;
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
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection();
    inter.intersect(sk1);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
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
    final int lgK = 9;
    final int k = 1<<lgK;

    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 2 * k; i++) {
      sk1.update(i);  //est mode
    }
    final CompactSketch cSk1 = sk1.compact(true, null);
    final double cSk1Est = cSk1.getEstimate();
    println("cSk1Est: " + cSk1Est);

    final Intersection inter = SetOperation.builder().buildIntersection();
    //1st call with a valid sketch
    inter.intersect(cSk1);

    final UpdateSketch sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < 2 * k; i++) {
      sk2.update(i);  //est mode
    }
    final CompactSketch cSk2 = sk2.compact(true, null);
    final double cSk2Est = cSk2.getEstimate();
    println("cSk2Est: " + cSk2Est);
    assertEquals(cSk2Est, cSk1Est, 0.0);

    //2nd call with identical valid sketch
    inter.intersect(cSk2);
    final CompactSketch interResultCSk1 = inter.getResult(false, null);
    final double inter1est = interResultCSk1.getEstimate();
    assertEquals(inter1est, cSk1Est, 0.0);
    println("Inter1Est: " + inter1est);

    //Put the intersection into segment
    final byte[] byteArray = inter.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //Heapify
    final Intersection inter2 = (Intersection) SetOperation.heapify(seg);
    final CompactSketch heapifiedSk = inter2.getResult(false, null);
    final double heapifiedEst = heapifiedSk.getEstimate();
    assertEquals(heapifiedEst, cSk1Est, 0.0);
    println("HeapifiedEst: "+heapifiedEst);

    //Wrap
    final Intersection inter3 = Sketches.wrapIntersection(seg);
    final CompactSketch wrappedSk = inter3.getResult(false, null);
    final double wrappedEst = wrappedSk.getEstimate();
    assertEquals(wrappedEst, cSk1Est, 0.0);
    println("WrappedEst: "+ wrappedEst);

    inter.reset();
    inter2.reset();
    inter3.reset(); //??
  }


  /**
   * This proves that the hash of 7 is &lt; 0.5. This fact will be used in other tests involving P.
   */
  @Test
  public void checkPreject() {
    final UpdateSketch sk = UpdateSketch.builder().setP((float) .5).build();
    sk.update(7);
    assertEquals(sk.getRetainedEntries(), 0);
  }

  @Test
  public void checkHeapifyVirginEmpty() {
    final int lgK = 5;
    final int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    inter1 = SetOperation.builder().buildIntersection(); //virgin heap
    MemorySegment srcSeg = MemorySegment.ofArray(inter1.toByteArray()).asReadOnly();
    inter2 = (Intersection) SetOperation.heapify(srcSeg); //virgin heap, independent of inter1
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
    srcSeg = MemorySegment.ofArray(inter1.toByteArray()).asReadOnly();
    inter2 = (Intersection) SetOperation.heapify(srcSeg); //inter2 identical to inter1
    assertFalse(inter2.isEmpty());
    assertTrue(inter2.hasResult());

    //test the compaction path
    final CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    final Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    final byte[] byteArray = inter1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 2); //RF not used = 0
    SetOperation.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    final byte[] byteArray = inter1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 2);
    SetOperation.heapify(seg);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    final int k = 32;
    final Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
    final byte[] byteArray = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    @SuppressWarnings("unused")
    final
    Intersection inter1 = (Intersection) SetOperation.heapify(seg); //bad cast
    //println(inter1.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadEmptyState() {
    final Intersection inter1 = SetOperation.builder().buildIntersection(); //virgin
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    inter1.intersect(sk); //initializes to a true empty intersection.
    final byte[] byteArray = inter1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, 1);
    SetOperation.heapify(seg);
  }

  @Test
  public void checkEmpty() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().build();
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(usk);
    assertTrue(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(), 0);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    final long[] longArr = inter.getCache(); //only applies to stateful
    assertEquals(longArr.length, 0);
  }

  @Test
  public void checkOne() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(usk);
    assertFalse(inter.isEmpty());
    assertEquals(inter.getRetainedEntries(), 1);
    assertTrue(inter.getSeedHash() != 0);
    assertEquals(inter.getThetaLong(), Long.MAX_VALUE);
    final long[] longArr = inter.getCache(); //only applies to stateful
    assertEquals(longArr.length, 32);
  }

  @Test
  public void checkGetResult() {
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();

    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(sk);
    final CompactSketch csk = inter.getResult();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test
  public void checkFamily() {
    final IntersectionImpl impl = IntersectionImpl.initNewHeapInstance(ThetaUtil.DEFAULT_UPDATE_SEED);
    assertEquals(impl.getFamily(), Family.INTERSECTION);
  }

  @Test
  public void checkPairIntersectSimple() {
    final UpdateSketch skA = Sketches.updateSketchBuilder().build();
    final UpdateSketch skB = Sketches.updateSketchBuilder().build();
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    final CompactSketch csk = inter.intersect(skA, skB);
    assertEquals(csk.getCompactBytes(), 8);
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
