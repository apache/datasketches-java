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
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.SetOperation.CONST_PREAMBLE_LONGS;
import static org.apache.datasketches.theta.SetOperation.getMaxIntersectionBytes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesReadOnlyException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
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

    inter.intersect(usk1);
    inter.intersect(usk2);

    long[] cache = inter.getCache(); //only applies to stateful
    assertEquals(cache.length, 32);

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
    inter.intersect(csk1);
    inter.intersect(csk2);

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
  public void checkIntersectionNull() {
    int lgK = 9;
    int k = 1<<lgK;
    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);
    Intersection inter = SetOperation.builder().buildIntersection(iMem);
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

    int memBytes = getMaxIntersectionBytes(k);
    byte[] memArr = new byte[memBytes];
    WritableMemory iMem = WritableMemory.wrap(memArr);

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
    inter.intersect(sk1);
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

    byte[] byteArray = inter.toByteArray();
    Memory mem = Memory.wrap(byteArray);
    Intersection inter2 = (Intersection) SetOperation.heapify(mem);
    comp2 = inter2.getResult(false, null);
    est2 = comp2.getEstimate();
    println("Est2: "+est2);
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
  public void checkWrapVirginEmpty() {
    int lgK = 5;
    int k = 1 << lgK;
    Intersection inter1, inter2;
    UpdateSketch sk1;

    int memBytes = getMaxIntersectionBytes(k);
    WritableMemory iMem = WritableMemory.wrap(new byte[memBytes]);

    inter1 = SetOperation.builder().buildIntersection(iMem); //virgin off-heap
    inter2 = Sketches.wrapIntersection(iMem); //virgin off-heap, identical to inter1
    //both in virgin state, empty = false
    //note: both inter1 and inter2 are tied to the same memory,
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

    //test the path via toByteArray, wrap, now in a different state
    iMem = WritableMemory.wrap(inter1.toByteArray());
    inter2 = Sketches.wrapIntersection((Memory)iMem);
    assertTrue(inter2.hasResult()); //still true

    //test the compaction path
    CompactSketch comp = inter2.getResult(true, null);
    assertEquals(comp.getRetainedEntries(false), 0);
    assertFalse(comp.isEmpty());
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

    inter1.intersect(sk1);
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
    inter.intersect(csk1);
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
    inter.intersect(compSkIn1);

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

    inter2.intersect(compSkIn2);
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
    inter.intersect(sk);
    CompactSketch csk = inter.getResult();
    assertEquals(csk.getCompactBytes(), 8);
  }

  @Test
  public void checkFamily() {
    //cheap trick
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + PREBYTES]);
    IntersectionImpl impl = IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
    assertEquals(impl.getFamily(), Family.INTERSECTION);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkExceptions1() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) + PREBYTES]);
    IntersectionImpl.initNewDirectInstance(DEFAULT_UPDATE_SEED, mem);
    //corrupt SerVer
    mem.putByte(PreambleUtil.SER_VER_BYTE, (byte) 2);
    IntersectionImpl.wrapInstance(mem, DEFAULT_UPDATE_SEED, false);
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
    IntersectionImpl.wrapInstance(mem, DEFAULT_UPDATE_SEED, false);
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
    WritableMemory memInter = WritableMemory.wrap(new byte[memBytes]);
    WritableMemory memComp = WritableMemory.wrap(new byte[memBytes]);
    CompactSketch csk1 = sk1.compact(true, memIn1);
    CompactSketch csk2 = sk2.compact(true, memIn2);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection(memInter);
    inter.intersect(csk1);
    inter.intersect(csk2);
    CompactSketch cskOut = inter.getResult(true, memComp);
    assertEquals(cskOut.getEstimate(), 2.0, 0.0);

    Intersection interRO = (Intersection) SetOperation.wrap((Memory)memInter);
    try {
      interRO.intersect(sk1, sk2);
      fail();
    } catch (SketchesReadOnlyException e) { }
    try {
      interRO.reset();
      fail();
    } catch (SketchesReadOnlyException e) { }
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
