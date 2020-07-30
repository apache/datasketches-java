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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class AnotBimplTest {

  @Test
  public void checkExactAnotB_AvalidNoOverlap() {
    int k = 512;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<(k/2); i++) {
      usk1.update(i);
    }
    for (int i=k/2; i<k; i++) {
      usk2.update(i);
    }

    AnotB aNb = SetOperation.builder().buildANotB();
    assertTrue(aNb.isEmpty());  //only applies to stateful
    assertTrue(aNb.getCache().length == 0); //only applies to stateful
    assertEquals(aNb.getThetaLong(), Long.MAX_VALUE); //only applies to stateful
    assertEquals(aNb.getSeedHash(), Util.computeSeedHash(DEFAULT_UPDATE_SEED));

    aNb.setA(usk1);
    aNb.notB(usk2);
    assertEquals(aNb.getRetainedEntries(), k/2);

    CompactSketch rsk1;

    rsk1 = aNb.getResult(false, null, true); //not ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(true, null, true); //ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    int bytes = rsk1.getCurrentBytes();
    WritableMemory wmem = WritableMemory.allocate(bytes);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(false, wmem, true); //unordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);

    aNb.setA(usk1);
    aNb.notB(usk2);
    rsk1 = aNb.getResult(true, wmem, true); //ordered, reset
    assertEquals(rsk1.getEstimate(), k/2.0);
  }

  @Test
  public void checkCombinations() {
    int k = 512;
    UpdateSketch aNull = null;
    UpdateSketch bNull = null;
    UpdateSketch aEmpty = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch bEmpty = UpdateSketch.builder().setNominalEntries(k).build();

    UpdateSketch aHT = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      aHT.update(i);
    }
    CompactSketch aC = aHT.compact(false, null);
    CompactSketch aO = aHT.compact(true,  null);

    UpdateSketch bHT = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<(k+(k/2)); i++) {
      bHT.update(i); //overlap is k/2
    }
    CompactSketch bC = bHT.compact(false, null);
    CompactSketch bO = bHT.compact(true,  null);

    CompactSketch result;
    AnotB aNb;
    boolean ordered = true;

    aNb = SetOperation.builder().buildANotB();

    try { aNb.setA(aNull); fail();} catch (SketchesArgumentException e) {}

    aNb.notB(bNull); //ok

    try { aNb.aNotB(aNull, bNull); fail(); } catch (SketchesArgumentException e) {}
    try { aNb.aNotB(aNull, bEmpty); fail(); } catch (SketchesArgumentException e) {}
    try { aNb.aNotB(aEmpty, bNull); fail(); } catch (SketchesArgumentException e) {}

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
    assertEquals(result.getEstimate(), (double) k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bC, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bO, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aC, bHT, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), (double) k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bC, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aO, bO, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result =  aNb.aNotB(aO, bHT, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bEmpty, !ordered, null);
    assertEquals(result.getEstimate(), (double) k);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bC, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bO, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);

    result = aNb.aNotB(aHT, bHT, !ordered, null);
    assertEquals(result.getEstimate(), (double) k/2);
    assertFalse(result.isEmpty());
    assertEquals(result.getThetaLong(), Long.MAX_VALUE);
  }

  @Test
  public void checkAnotBnotC() {
    int k = 1024;
    boolean ordered = true;

    UpdateSketch aU = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { aU.update(i); }  //All 1024

    UpdateSketch bU = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(k/2); i++) { bU.update(i); } //first 512

    UpdateSketch cU = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<((3*k)/4); i++) { cU.update(i); } //third 256

    int memBytes = Sketch.getMaxUpdateSketchBytes(k);
    CompactSketch result1, result2, result3;

    WritableMemory wmem1 = WritableMemory.allocate(memBytes);
    WritableMemory wmem2 = WritableMemory.allocate(memBytes);
    WritableMemory wmem3 = WritableMemory.allocate(memBytes);

    AnotB aNb = SetOperation.builder().buildANotB();

    //Note: stateful and stateless operations can be interleaved, they are independent.

    aNb.setA(aU);                                     //stateful

    result1 = aNb.aNotB(aU, bU, ordered, wmem1);      //stateless

    aNb.notB(bU);                                     //stateful

    result2 = aNb.aNotB(result1, cU, ordered, wmem2); //stateless

    aNb.notB(cU);                                     //stateful

    double est2 = result2.getEstimate();              //stateless result
    println("est: "+est2);
    assertEquals(est2, k/4.0, 0.0);

    result3 = aNb.getResult(ordered, wmem3, true);    //stateful result, then reset
    double est3 = result3.getEstimate();
    assertEquals(est3, k/4.0, 0.0);
  }

  @Test
  public void checkAnotBnotC_sameMemory() {
    int k = 1024;
    boolean ordered = true;

    UpdateSketch a = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { a.update(i); }       //All 1024

    UpdateSketch b = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<(k/2); i++) { b.update(i); }     //first 512

    UpdateSketch c = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=k/2; i<((3*k)/4); i++) { c.update(i); }  //third 256

    int memBytes = Sketch.getMaxCompactSketchBytes(a.getRetainedEntries(true));
    WritableMemory mem = WritableMemory.allocate(memBytes);

    CompactSketch result1, result2;
    AnotB aNb = SetOperation.builder().buildANotB();

    //Note: stateful and stateless operations can be interleaved, they are independent.

    aNb.setA(a);                                    //stateful

    result1 = aNb.aNotB(a, b, ordered, mem);        //stateless

    aNb.notB(b);                                    //stateful

    result1 = aNb.aNotB(result1, c, ordered, mem);  //stateless

    aNb.notB(c);                                    //stateful

    result2 = aNb.getResult(ordered, mem, true);    //stateful result, then reset

    double est1 = result1.getEstimate();            //check stateless result
    println("est: "+est1);
    assertEquals(est1, k/4.0, 0.0);

    double est2 = result2.getEstimate();            //check stateful result
    assertEquals(est2, k/4.0, 0.0);
  }

  @Test
  public void checkAnotBsimple() {
    UpdateSketch skA = Sketches.updateSketchBuilder().build();
    UpdateSketch skB = Sketches.updateSketchBuilder().build();
    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    CompactSketch csk = aNotB.aNotB(skA, skB);
    assertEquals(csk.getCurrentBytes(), 8);
  }

  @Test
  public void checkGetResult() {
    UpdateSketch skA = Sketches.updateSketchBuilder().build();
    UpdateSketch skB = Sketches.updateSketchBuilder().build();

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    CompactSketch csk = aNotB.aNotB(skA, skB);
    assertEquals(csk.getCurrentBytes(), 8);
  }

  @Test
  public void checkGetFamily() {
    //cheap trick
    AnotBimpl anotb = new AnotBimpl(Util.DEFAULT_UPDATE_SEED);
    assertEquals(anotb.getFamily(), Family.A_NOT_B);
  }

  @Test
  public void checkGetMaxBytes() {
    int bytes = Sketches.getMaxAnotBResultBytes(10);
    assertEquals(bytes, (16 * 15) + 24);
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
