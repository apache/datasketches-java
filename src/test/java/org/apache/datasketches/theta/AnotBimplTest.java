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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class AnotBimplTest {

  @Test
  public void checkExactAnotB_AvalidNoOverlap() {
    final int k = 512;

    final UpdatableThetaSketch usk1 = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    final UpdatableThetaSketch usk2 = UpdatableThetaSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k/2; i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    final ThetaAnotB aNb = ThetaSetOperation.builder().buildANotB();
    assertTrue(aNb.isEmpty());  //only applies to stateful
    assertTrue(aNb.getCache().length == 0); //only applies to stateful
    assertEquals(aNb.getThetaLong(), Long.MAX_VALUE); //only applies to stateful
    assertEquals(aNb.getSeedHash(), Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED));

    aNb.setA(usk1);
    aNb.notB(usk2);
    assertEquals(aNb.getRetainedEntries(), k/2);

    CompactThetaSketch rsk1;

    rsk1 = aNb.getResult(false, null, true); //not ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(true, null, true); //ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    final int bytes = rsk1.getCurrentBytes();
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(false, wseg, true); //unordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(true, wseg, true); //ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);
  }

  @Test
  public void checkCombinations() {
    final int k = 512;
    final UpdatableThetaSketch aNull = null;
    final UpdatableThetaSketch bNull = null;
    final UpdatableThetaSketch aEmpty = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    final UpdatableThetaSketch bEmpty = UpdatableThetaSketch.builder().setNominalEntries(k).build();

    final UpdatableThetaSketch aHT = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      aHT.update(i);
    }
    final CompactThetaSketch aC = aHT.compact(false, null);
    final CompactThetaSketch aO = aHT.compact(true,  null);

    final UpdatableThetaSketch bHT = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<k+k/2; i++) {
      bHT.update(i); //overlap is k/2
    }
    final CompactThetaSketch bC = bHT.compact(false, null);
    final CompactThetaSketch bO = bHT.compact(true,  null);

    CompactThetaSketch result;
    ThetaAnotB aNb;
    final boolean ordered = true;

    aNb = ThetaSetOperation.builder().buildANotB();

    try { aNb.setA(aNull); fail();} catch (final SketchesArgumentException e) {}

    aNb.notB(bNull); //ok

    try { aNb.aNotB(aNull, bNull); fail(); } catch (final SketchesArgumentException e) {}
    try { aNb.aNotB(aNull, bEmpty); fail(); } catch (final SketchesArgumentException e) {}
    try { aNb.aNotB(aEmpty, bNull); fail(); } catch (final SketchesArgumentException e) {}

    result = aNb.aNotB(aEmpty, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), 0.0);
    assertTrue(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aEmpty, bC, !ordered, null);
    assertEquals(result.getEstimate(), 0.0);
    assertTrue(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aEmpty, bO, !ordered, null);
    assertEquals(result.getEstimate(), 0.0);
    assertTrue(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aEmpty, bHT, !ordered, null);
    assertEquals(result.getEstimate(), 0.0);
    assertTrue(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bC, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bO, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bHT, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bC, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bO, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result =  aNb.aNotB(aO, bHT, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bC, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bO, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bHT, !ordered, null);
    assertEquals(result.getEstimate(), k / 2.0);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);
  }

  @Test
  public void checkAnotBnotC() {
    final int k = 1024;
    final boolean ordered = true;

    final UpdatableThetaSketch aU = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { aU.update(i); }  //All 1024

    final UpdatableThetaSketch bU = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k/2; i++) { bU.update(i); } //first 512

    final UpdatableThetaSketch cU = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<3*k/4; i++) { cU.update(i); } //third 256

    final int segBytes = ThetaSketch.getMaxUpdateSketchBytes(k);
    CompactThetaSketch result1, result2, result3;

    final MemorySegment wseg1 = MemorySegment.ofArray(new byte[segBytes]);
    final MemorySegment wseg2 = MemorySegment.ofArray(new byte[segBytes]);
    final MemorySegment wseg3 = MemorySegment.ofArray(new byte[segBytes]);

    final ThetaAnotB aNb = ThetaSetOperation.builder().buildANotB();

    //Note: stateful and stateless operations can be interleaved, they are independent.

    aNb.setA(aU);                                     //stateful

    result1 = aNb.aNotB(aU, bU, ordered, wseg1);      //stateless

    aNb.notB(bU);                                     //stateful

    result2 = aNb.aNotB(result1, cU, ordered, wseg2); //stateless

    aNb.notB(cU);                                     //stateful

    final double est2 = result2.getEstimate();              //stateless result
    println("est: "+est2);
    assertEquals(est2, k/4.0, 0.0);

    result3 = aNb.getResult(ordered, wseg3, true);    //stateful result, then reset
    final double est3 = result3.getEstimate();
    assertEquals(est3, k/4.0, 0.0);
  }

  @Test
  public void checkAnotBnotC_sameMemorySegment() {
    final int k = 1024;
    final boolean ordered = true;

    final UpdatableThetaSketch a = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { a.update(i); }       //All 1024

    final UpdatableThetaSketch b = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k/2; i++) { b.update(i); }     //first 512

    final UpdatableThetaSketch c = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<3*k/4; i++) { c.update(i); }  //third 256

    final int segBytes = ThetaSketch.getMaxCompactSketchBytes(a.getRetainedEntries(true));
    final MemorySegment seg = MemorySegment.ofArray(new byte[segBytes]);

    CompactThetaSketch result1, result2;
    final ThetaAnotB aNb = ThetaSetOperation.builder().buildANotB();

    //Note: stateful and stateless operations can be interleaved, they are independent.

    aNb.setA(a);                                    //stateful

    result1 = aNb.aNotB(a, b, ordered, seg);        //stateless

    aNb.notB(b);                                    //stateful

    result1 = aNb.aNotB(result1, c, ordered, seg);  //stateless

    aNb.notB(c);                                    //stateful

    result2 = aNb.getResult(ordered, seg, true);    //stateful result, then reset

    final double est1 = result1.getEstimate();            //check stateless result
    println("est: "+est1);
    assertEquals(est1, k/4.0, 0.0);

    final double est2 = result2.getEstimate();            //check stateful result
    assertEquals(est2, k/4.0, 0.0);
  }

  @Test
  public void checkAnotBsimple() {
    final UpdatableThetaSketch skA = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch skB =UpdatableThetaSketch.builder().build();
    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk = aNotB.aNotB(skA, skB);
    assertEquals(csk.getCurrentBytes(), 8);
  }

  @Test
  public void checkGetResult() {
    final UpdatableThetaSketch skA = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch skB = UpdatableThetaSketch.builder().build();
    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk = aNotB.aNotB(skA, skB);
    assertEquals(csk.getCurrentBytes(), 8);
  }

  @Test
  public void checkGetFamily() {
    //cheap trick
    final ThetaAnotBimpl anotb = new ThetaAnotBimpl(Util.DEFAULT_UPDATE_SEED);
    assertEquals(anotb.getFamily(), Family.A_NOT_B);
  }

  @Test
  public void checkGetMaxBytes() {
    final int bytes = ThetaSetOperation.getMaxAnotBResultBytes(10);
    assertEquals(bytes, 16 * 15 + 24);
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
