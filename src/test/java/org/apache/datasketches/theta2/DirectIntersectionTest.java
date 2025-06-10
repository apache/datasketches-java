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
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta2.SetOperation.CONST_PREAMBLE_LONGS;
import static org.apache.datasketches.theta2.SetOperation.getMaxIntersectionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectIntersectionTest {
  private static final int PREBYTES = CONST_PREAMBLE_LONGS << 3; //24

  @Test
  public void checkExactIntersectionNoOverlap() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k/2; i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    inter = SetOperation.builder().buildIntersection(iMem);

    inter.intersect(usk1);
    inter.intersect(usk2);

    final long[] cache = inter.getCache(); //only applies to stateful
    assertEquals(cache.length, 32);

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
  }

  @Test
  public void checkExactIntersectionFullOverlap() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    final UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
    }
    for (int i=0; i<k; i++) {
      usk2.update(i);
    }

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    inter = SetOperation.builder().buildIntersection(iMem);
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
    for (int i=u/2; i<u + u/2; i++) {
      usk2.update(i);
    }

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    final CompactSketch csk1 = usk1.compact(true, null);
    final CompactSketch csk2 = usk2.compact(true, null);

    final Intersection inter =
        SetOperation.builder().buildIntersection(iMem);
    inter.intersect(csk1);
    inter.intersect(csk2);

    final CompactSketch rsk1 = inter.getResult(true, null);
    println(""+rsk1.getEstimate());
  }

  //Calling getResult on a virgin Intersect is illegal
  @Test(expectedExceptions = SketchesStateException.class)
  public void checkNoCall() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    inter = SetOperation.builder().buildIntersection(iMem);
    assertFalse(inter.hasResult());
    inter.getResult(false, null);
  }

  @Test
  public void checkIntersectionNull() {
    final int lgK = 9;
    final int k = 1<<lgK;
    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);
    final Intersection inter = SetOperation.builder().buildIntersection(iMem);
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

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = empty
    sk = UpdateSketch.builder().setNominalEntries(k).build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.intersect(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 0.0, 0.0);
    println("Est: "+est); // = 0

    //1st call = valid and not empty
    sk = UpdateSketch.builder().setNominalEntries(k).build();
    sk.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
    inter.intersect(sk);
    rsk1 = inter.getResult(false, null);
    est = rsk1.getEstimate();
    assertEquals(est, 1.0, 0.0);
    println("Est: "+est); // = 1
  }

  @Test
  public void check2ndCallAfterEmpty() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = empty
    sk1 = UpdateSketch.builder().build(); //empty
    inter = SetOperation.builder().buildIntersection(iMem);
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
    inter = SetOperation.builder().buildIntersection(iMem);
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
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1;
    double est;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().build();
    sk1.update(1);
    inter = SetOperation.builder().buildIntersection(iMem);
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
    inter = SetOperation.builder().buildIntersection(iMem);
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
    inter = SetOperation.builder().buildIntersection(iMem);
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

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
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

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkOverflow() {
    final int lgK = 9; //512
    final int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1;
    final UpdateSketch sk2;
    final CompactSketch comp1;
    final double est;

    final int reqBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[reqBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(2 * k).build(); // bigger sketch
    for (int i=0; i<4*k; i++)
     {
      sk1.update(i);  //force est mode
    }
    println("sk1est: "+sk1.getEstimate());
    println("sk1cnt: "+sk1.getRetainedEntries(true));

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.intersect(sk1);
  }

  @Test
  public void checkHeapify() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter;
    UpdateSketch sk1, sk2;
    CompactSketch comp1, comp2;
    double est, est2;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
     {
      sk1.update(i);  //est mode
    }
    println("sk1: "+sk1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
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

    final byte[] byteArray = inter.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    final Intersection inter2 = (Intersection) SetOperation.heapify(seg);
    comp2 = inter2.getResult(false, null);
    est2 = comp2.getEstimate();
    println("Est2: "+est2);
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
  public void checkWrapVirginEmpty() {
    final int lgK = 5;
    final int k = 1 << lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    final int segBytes = getMaxIntersectionBytes(k);
    MemorySegment iMem = MemorySegment.ofArray(new byte[segBytes]);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin off-heap
    inter2 = Sketches.wrapIntersection(iMem); //virgin off-heap, identical to inter1
    //both in virgin state, empty = false
    //note: both inter1 and inter2 are tied to the same MemorySegment,
    // so an intersect to one also affects the other.  Don't do what I do!
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());

    //This constructs a sketch with 0 entries and theta < 1.0
    sk1 = UpdateSketch.builder().setP((float) .5).setNominalEntries(k).build();
    sk1.update(7); //will be rejected by P, see proof above.

    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false!
    inter1.intersect(sk1);
    assertFalse(inter1.isEmpty());
    assertTrue(inter1.hasResult());
    //note that inter2 is not independent
    assertFalse(inter2.isEmpty());
    assertTrue(inter2.hasResult());

    //test the path via toByteArray, now in a different state
    iMem = MemorySegment.ofArray(inter1.toByteArray());
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter2.hasResult()); //still true

    //test the compaction path
    final CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }

  @Test
  public void checkWrapNullEmpty2() {
    final int lgK = 5;
    final int k = 1<<lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    inter2 = Sketches.wrapIntersection(iMem);
    //both in virgin state, empty = false
    assertFalse(inter1.hasResult());
    assertFalse(inter2.hasResult());

    sk1 = UpdateSketch.builder().setP((float) .005).setFamily(Family.QUICKSELECT).setNominalEntries(k).build();
    sk1.update(1); //very unlikely to go into cache due to p.
    //A virgin intersection (empty = false) intersected with a not-empty zero cache sketch
    //remains empty = false.

    inter1.intersect(sk1);
    inter2 = Sketches.wrapIntersection(iMem);
    assertTrue(inter1.hasResult());
    assertTrue(inter2.hasResult());
    final CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizeLowerLimit() {
    final int k = 8;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    SetOperation.builder().buildIntersection(iMem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSizedTooSmall() {
    final int lgK = 5;
    final int k = 1<<lgK;
    final int u = 4*k;

    final int segBytes = getMaxIntersectionBytes(k/2);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    final UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk1.update(i);
    }

    final CompactSketch csk1 = usk1.compact(true, null);

    final Intersection inter = SetOperation.builder().buildIntersection(iMem);
    inter.intersect(csk1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    final int k = 32;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    final Intersection inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    final byte[] byteArray = inter1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 2);//RF not used = 0
    Sketches.wrapIntersection(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final int k = 32;
    Intersection inter1;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin
    final byte[] byteArray = inter1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 2);
    Sketches.wrapIntersection(seg); //throws in SetOperations
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void checkFamilyID() {
    final int k = 32;
    Union union;

    union = SetOperation.builder().setNominalEntries(k).buildUnion();
    final byte[] byteArray = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    Sketches.wrapIntersection(seg);
  }

  @Test
  public void checkWrap() {
    final int lgK = 9;
    final int k = 1<<lgK;
    Intersection inter, inter2, inter3;
    UpdateSketch sk1, sk2;
    CompactSketch resultComp1, resultComp2;
    double est, est2;

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr1 = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr1);

    //1st call = valid
    sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
     {
      sk1.update(i);  //est mode
    }
    final CompactSketch compSkIn1 = sk1.compact(true, null);
    println("compSkIn1: "+compSkIn1.getEstimate());

    inter = SetOperation.builder().buildIntersection(iMem);
    inter.intersect(compSkIn1);

    final byte[] segArr2 = inter.toByteArray();
    final MemorySegment srcMem = MemorySegment.ofArray(segArr2);
    inter2 = Sketches.wrapIntersection(srcMem);

    //2nd call = valid intersecting
    sk2 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<2*k; i++)
     {
      sk2.update(i);  //est mode
    }
    final CompactSketch compSkIn2 = sk2.compact(true, null);
    println("sk2: "+compSkIn2.getEstimate());

    inter2.intersect(compSkIn2);
    resultComp1 = inter2.getResult(false, null);
    est = resultComp1.getEstimate();
    assertTrue(est > k);
    println("Est: "+est);

    final byte[] segArr3 = inter2.toByteArray();
    final MemorySegment srcMem2 = MemorySegment.ofArray(segArr3);
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
    final int k = 32;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*8 + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(ThetaUtil.DEFAULT_UPDATE_SEED, seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptionMinSize() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*8 + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(ThetaUtil.DEFAULT_UPDATE_SEED, seg);
  }

  @Test
  public void checkGetResult() {
    final int k = 1024;
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();

    final int segBytes = getMaxIntersectionBytes(k);
    final byte[] segArr = new byte[segBytes];
    final MemorySegment iMem = MemorySegment.ofArray(segArr);

    final Intersection inter = Sketches.setOperationBuilder().buildIntersection(iMem);
    inter.intersect(sk);
    final CompactSketch csk = inter.getResult();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test
  public void checkFamily() {
    //cheap trick
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 + PREBYTES]);
    final IntersectionImpl impl = IntersectionImpl.initNewDirectInstance(ThetaUtil.DEFAULT_UPDATE_SEED, seg);
    assertEquals(impl.getFamily(), Family.INTERSECTION);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptions1() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(ThetaUtil.DEFAULT_UPDATE_SEED, seg);
    //corrupt SerVer
    seg.set(JAVA_BYTE, PreambleUtil.SER_VER_BYTE, (byte) 2);
    IntersectionImpl.wrapInstance(seg, ThetaUtil.DEFAULT_UPDATE_SEED, false);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptions2() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(ThetaUtil.DEFAULT_UPDATE_SEED, seg);
    //seg now has non-empty intersection
    //corrupt empty and CurCount
    Util.setBits(seg, PreambleUtil.FLAGS_BYTE, (byte) PreambleUtil.EMPTY_FLAG_MASK);
    seg.set(JAVA_INT_UNALIGNED, PreambleUtil.RETAINED_ENTRIES_INT, 2);
    IntersectionImpl.wrapInstance(seg, ThetaUtil.DEFAULT_UPDATE_SEED, false);
  }

  //Check Alex's bug intersecting 2 direct full sketches with only overlap of 2
  //
  @Test
  public void checkOverlappedDirect() {
    final int k = 1 << 4;
    final int segBytes = 2*k*16 +PREBYTES; //plenty of room
    final UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    final UpdateSketch sk2 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sk1.update(i);
      sk2.update(k-2 +i); //overlap by 2
    }
    final MemorySegment segIn1 = MemorySegment.ofArray(new byte[segBytes]);
    final MemorySegment segIn2 = MemorySegment.ofArray(new byte[segBytes]);
    final MemorySegment segInter = MemorySegment.ofArray(new byte[segBytes]);
    final MemorySegment segComp = MemorySegment.ofArray(new byte[segBytes]);
    final CompactSketch csk1 = sk1.compact(true, segIn1);
    final CompactSketch csk2 = sk2.compact(true, segIn2);
    final Intersection inter = Sketches.setOperationBuilder().buildIntersection(segInter);
    inter.intersect(csk1);
    inter.intersect(csk2);
    final CompactSketch cskOut = inter.getResult(true, segComp);
    assertEquals(cskOut.getEstimate(), 2.0, 0.0);

    final Intersection interRO = (Intersection) SetOperation.wrap(segInter.asReadOnly());
    try {
      interRO.intersect(sk1, sk2);
      fail();
    } catch (final SketchesReadOnlyException e) { }
    try {
      interRO.reset();
      fail();
    } catch (final SketchesReadOnlyException e) { }
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
