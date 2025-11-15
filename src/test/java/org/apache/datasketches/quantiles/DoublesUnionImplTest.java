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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.DirectUpdateDoublesSketchTest.buildAndLoadDQS;
import static org.apache.datasketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DoublesUnionImplTest {

  @Test
  public void checkUnion1() {
    QuantilesDoublesSketch result;
    final QuantilesDoublesSketch qs1;
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256

    final QuantilesDoublesSketch qs0 = buildAndLoadQS(256, 500);
    union.union(qs0); //me = null, that = valid, exact
    result = union.getResult();
    assertEquals(result.getN(), 500);
    assertEquals(result.getK(), 256);

    union.reset();
    qs1 = buildAndLoadQS(256, 1000); //first 1000
    union.union(qs1); //me = null,  that = valid, OK

    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);

    //check merge  me = valid, that = valid, both K's the same
    final QuantilesDoublesSketch qs2 = buildAndLoadQS(256, 1000, 1000); //add 1000
    union.union(qs2);
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }

  @Test
  public void checkUnion1Direct() {
    QuantilesDoublesSketch result;
    final QuantilesDoublesSketch qs1;
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256

    final QuantilesDoublesSketch qs0 = buildAndLoadDQS(256, 500);
    union.union(qs0); //me = null, that = valid, exact
    result = union.getResult();
    assertEquals(result.getN(), 500);
    assertEquals(result.getK(), 256);

    union.reset();
    qs1 = buildAndLoadDQS(256, 1000); //first 1000
    union.union(qs1); //me = null,  that = valid, OK

    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);

    //check merge  me = valid, that = valid, both K's the same
    final QuantilesDoublesSketch qs2 = buildAndLoadDQS(256, 1000, 1000).compact(); //add 1000
    union.union(qs2);
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }

  @Test
  public void checkUnion2() {
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(256, 1000).compact();
    final QuantilesDoublesSketch qs2 = buildAndLoadQS(128, 1000);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256
    assertEquals(union.getEffectiveK(), 256);

    union.union(qs1);
    final QuantilesDoublesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);

    union.union(qs2);
    final QuantilesDoublesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    assertEquals(union.getEffectiveK(), 128);
    println(union.toString());
  }

  @Test
  public void checkUnion2Direct() {
    final QuantilesDoublesSketch qs1 = buildAndLoadDQS(256, 1000);
    final QuantilesDoublesSketch qs2 = buildAndLoadDQS(128, 1000);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256
    assertEquals(union.getEffectiveK(), 256);

    union.union(qs1);
    final QuantilesDoublesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);

    union.union(qs2);
    final QuantilesDoublesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    assertEquals(union.getEffectiveK(), 128);
    println(union.toString());
  }

  @Test
  public void checkUnion3() { //Union is direct, empty and with larger K than valid input
    final int k1 = 128;
    final int n1 = 2 * k1;
    final int k2 = 256;
    final int n2 = 2000;
    final QuantilesDoublesSketch sketchIn1 = buildAndLoadQS(k1, n1);
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k2, n2);//just for size
    final MemorySegment seg = MemorySegment.ofArray(new byte[bytes]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(k2).build(seg, null); //virgin 256
    union.union(sketchIn1);
    assertEquals(union.getMaxK(), k2);
    assertEquals(union.getEffectiveK(), k1);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), n1, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getK(), k1);
  }

  @Test
  public void checkUnion3Direct() { //Union is direct, empty and with larger K than valid input
    final int k1 = 128;
    final int n1 = 2 * k1;
    final int k2 = 256;
    final int n2 = 2000;
    final QuantilesDoublesSketch sketchIn1 = buildAndLoadDQS(k1, n1);
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k2, n2);//just for size
    final MemorySegment seg = MemorySegment.ofArray(new byte[bytes]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(k2).build(seg, null); //virgin 256
    union.union(sketchIn1);
    assertEquals(union.getMaxK(), k2);
    assertEquals(union.getEffectiveK(), k1);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), n1, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getK(), k1);
  }

  @Test
  public void checkUnion4() { //Union is direct, valid and with larger K than valid input
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[bytes]);
    final UpdatableQuantilesDoublesSketch sketchIn1 = QuantilesDoublesSketch.builder().setK(k1).build(skSeg);
    for (int i = 0; i < n1; i++) { sketchIn1.update(i + 1); }

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[bytes]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(uSeg, null); //virgin 256
    //QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256
    union.union(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxItem(), n1, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final QuantilesDoublesSketch sketchIn2 = buildAndLoadQS(k2, n2, 17);
    union.reset();
    union.union(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxItem(), n2 + 17, 0.0);
    assertEquals(result.getMinItem(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion4Direct() { //Union is direct, valid and with larger K than valid input
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[bytes]);
    final UpdatableQuantilesDoublesSketch sketchIn1 = QuantilesDoublesSketch.builder().setK(k1).build(skSeg);
    for (int i = 0; i < n1; i++) { sketchIn1.update(i + 1); }

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[bytes]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(uSeg, null); //virgin 256
    union.union(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxItem(), n1, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final QuantilesDoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17);
    union.reset();
    union.union(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxItem(), n2 + 17, 0.0);
    assertEquals(result.getMinItem(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion4DirectCompact() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 5 * k2; //8
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[bytes]);
    final UpdatableQuantilesDoublesSketch sketchIn0 = QuantilesDoublesSketch.builder().setK(k1).build(skSeg);
    for (int i = 0; i < n1; i++) { sketchIn0.update(i + 1); }
    final CompactQuantilesDoublesSketch sketchIn1 = sketchIn0.compact();

    final MemorySegment uSeg = MemorySegment.ofArray(new byte[bytes]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(uSeg, null); //virgin 256
    union.union(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxItem(), n1, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final CompactQuantilesDoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17).compact();
    union.reset();
    union.union(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxItem(), n2 + 17, 0.0);
    assertEquals(result.getMinItem(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion5() { //Union is direct, valid and with larger K than valid input
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(256, 50);//big enough
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[bytes]);
    QuantilesDoublesSketch.builder().setK(256).build(skSeg);

    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapifyInstance(skSeg);
    assertEquals(union.getResult().getN(), 0);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), 256);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getK(), 256);

    final QuantilesDoublesSketch sketchIn2 = buildAndLoadQS(k2, n2, 17);
    union.union(sketchIn2);
    println("\nFinal" + union.getResult().toString(true, true));
    assertEquals(union.getResult().getN(), n2);
  }

  @Test
  public void checkUnion5Direct() { //Union is direct, valid and with larger K than valid input
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(256, 50);//big enough
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[bytes]);
    QuantilesDoublesSketch.builder().setK(256).build(skSeg);

    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapifyInstance(skSeg);
    assertEquals(union.getResult().getN(), 0);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), 256);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getK(), 256);

    final QuantilesDoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17);
    union.union(sketchIn2);
    println("\nFinal" + union.getResult().toString(true, true));
    assertEquals(union.getResult().getN(), n2);
  }

  @Test
  public void checkUnion6() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 16;
    final int n2 = 2 * k2; //32
    final QuantilesDoublesSketch sk1 = buildAndLoadQS(k1, n1, 0);
    final QuantilesDoublesSketch sk2 = buildAndLoadQS(k2, n2, n1);
    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapifyInstance(sk1);
    union.union(sk2);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), n1 + n2, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion6Direct() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 16;
    final int n2 = 2 * k2; //32
    final QuantilesDoublesSketch sk1 = buildAndLoadDQS(k1, n1, 0);
    final QuantilesDoublesSketch sk2 = buildAndLoadDQS(k2, n2, n1);
    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapifyInstance(sk1);
    union.union(sk2);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), n1 + n2, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion7() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapInstance(16);
    final QuantilesDoublesSketch skEst = buildAndLoadQS(32, 64); //other is bigger, est
    union.union(skEst);
    println(skEst.toString(true, true));
    println(union.toString(true, true));
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), 64, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
  }

  @Test
  public void checkUnionQuantiles() {
    final int k = 128;
    final int n1 = k * 13;
    final int n2 = (k * 8) + (k / 2);
    final int n = n1 + n2;
    final double errorTolerance = 0.0175 * n; // assuming k = 128
    final UpdatableQuantilesDoublesSketch sketch1 = buildAndLoadQS(k, n1);
    final CompactQuantilesDoublesSketch sketch2 = buildAndLoadQS(k, n2, n1).compact();
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(256).build(); //virgin 256
    union.union(sketch2);
    union.union(sketch1);
    final MemorySegment seg = MemorySegment.ofArray(union.getResult().toByteArray(true));
    final QuantilesDoublesSketch result = QuantilesDoublesSketch.wrap(seg);
    assertEquals(result.getN(), n1 + n2);
    assertEquals(result.getK(), k);

    for (double fraction = 0.05; fraction < 1.0; fraction += 0.05) {
      assertEquals(result.getQuantile(fraction), fraction * n, errorTolerance);
    }
  }

  @Test
  public void checkUnion7Direct() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapInstance(16);
    final QuantilesDoublesSketch skEst = buildAndLoadDQS(32, 64); //other is bigger, est
    union.union(skEst);
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getMaxItem(), 64, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    //    println(skEst.toString(true, true));
    //    println(union.toString(true, true));
  }

  @Test
  public void checkUpdateMemory() {
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(256, 1000);
    final int bytes = qs1.getCurrentCompactSerializedSizeBytes();
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes]);
    qs1.putIntoMemorySegment(dstSeg);
    final MemorySegment srcSeg = dstSeg;

    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build(); //virgin
    union.union(srcSeg);
    for (int i = 1000; i < 2000; i++) { union.update(i); }
    final QuantilesDoublesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxItem(), 1999, 0.0);
    final String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }

  @Test
  public void checkUpdateMemoryDirect() {
    final QuantilesDoublesSketch qs1 = buildAndLoadDQS(256, 1000);
    final int bytes = qs1.getCurrentCompactSerializedSizeBytes();
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes]);
    qs1.putIntoMemorySegment(dstSeg);
    final MemorySegment srcSeg = dstSeg;

    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build(); //virgin
    union.union(srcSeg);
    for (int i = 1000; i < 2000; i++) { union.update(i); }
    final QuantilesDoublesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxItem(), 1999, 0.0);
    final String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }

  @Test
  public void checkUnionUpdateLogic() {
    final HeapUpdateDoublesSketch qs1 = null;
    final HeapUpdateDoublesSketch qs2 = (HeapUpdateDoublesSketch) buildAndLoadQS(256, 0);
    QuantilesDoublesUnionImpl.updateLogic(256, qs1, qs2); //null, empty
    QuantilesDoublesUnionImpl.updateLogic(256, qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    final QuantilesDoublesSketch result = QuantilesDoublesUnionImpl.updateLogic(256, qs2, qs1); //valid, null
    assertEquals(result.getMaxItem(), result.getMinItem(), 0.0);
  }

  @Test
  public void checkUnionUpdateLogicDirect() {
    final HeapUpdateDoublesSketch qs1 = null;
    final DirectUpdateDoublesSketch qs2 = (DirectUpdateDoublesSketch) buildAndLoadDQS(256, 0);
    QuantilesDoublesUnionImpl.updateLogic(256, qs1, qs2); //null, empty
    QuantilesDoublesUnionImpl.updateLogic(256, qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    final QuantilesDoublesSketch result = QuantilesDoublesUnionImpl.updateLogic(256, qs2, qs1); //valid, null
    assertEquals(result.getMaxItem(), result.getMinItem(), 0.0);
  }

  @Test
  public void checkUnionUpdateLogicDirectDownsampled() {
    final DirectUpdateDoublesSketch qs1 = (DirectUpdateDoublesSketch) buildAndLoadDQS(256, 1000);
    final DirectUpdateDoublesSketch qs2 = (DirectUpdateDoublesSketch) buildAndLoadDQS(128, 2000);
    final QuantilesDoublesSketch result = QuantilesDoublesUnionImpl.updateLogic(128, qs1, qs2);
    assertEquals(result.getMaxItem(), 2000.0, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
    assertEquals(result.getN(), 3000);
    assertEquals(result.getK(), 128);
  }

  @Test
  public void checkUnionUpdateLogic2() {
    QuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build();
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().build();
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();
    union.union(qs1);
    union.union(qs2); //case 5
    qs1 = buildAndLoadQS(128, 1000);
    union.union(qs1);
    union.union(qs2); //case 9
    final QuantilesDoublesSketch result = union.getResult();
    //println(union.toString(true, true));
    assertEquals(result.getMaxItem(), 1000.0, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);

  }

  @Test
  public void checkUnionUpdateLogic2Direct() {
    QuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build();
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().build();
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();
    union.union(qs1);
    union.union(qs2); //case 5
    qs1 = buildAndLoadDQS(128, 1000);
    union.union(qs1);
    union.union(qs2); //case 9
    final QuantilesDoublesSketch result = union.getResult();
    //println(union.toString(true, true));
    assertEquals(result.getMaxItem(), 1000.0, 0.0);
    assertEquals(result.getMinItem(), 1.0, 0.0);
  }

  @Test
  public void checkResultAndReset() {
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(256, 0);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.heapify(qs1);
    final QuantilesDoublesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void checkResultAndResetDirect() {
    final QuantilesDoublesSketch qs1 = buildAndLoadDQS(256, 0);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.heapify(qs1);
    final QuantilesDoublesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void checkResultViaMemory() {
    // empty gadget
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();

    // MemorySegment too small
    MemorySegment seg = MemorySegment.ofArray(new byte[1]);
    try {
      union.getResult(seg, null);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // sufficient MemorySegment
    seg = MemorySegment.ofArray(new byte[8]);
    QuantilesDoublesSketch result = union.getResult(seg, null);
    assertTrue(result.isEmpty());

    final int k = 128;
    final int n = 1392;
    seg = MemorySegment.ofArray(new byte[QuantilesDoublesSketch.getUpdatableStorageBytes(k, n)]);
    final QuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    union.union(qs);
    result = union.getResult(seg, null);
    DoublesSketchTest.testSketchEquality(result, qs);
  }

  @Test
  public void updateWithDoubleValueOnly() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();
    union.update(123.456);
    final QuantilesDoublesSketch qs = union.getResultAndReset();
    assertEquals(qs.getN(), 1);
  }

  @Test
  public void checkEmptyUnion() {
    final QuantilesDoublesUnionImpl union = QuantilesDoublesUnionImpl.heapInstance(128);
    final QuantilesDoublesSketch sk = union.getResult();
    assertNotNull(sk);
    final byte[] bytes = union.toByteArray();
    assertEquals(bytes.length, 8); //
    final String s = union.toString();
    assertNotNull(s);
  }

  @Test
  public void checkUnionNulls() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnionImpl.heapInstance(128);
    final QuantilesDoublesSketch sk1 = union.getResultAndReset();
    final QuantilesDoublesSketch sk2 = union.getResultAndReset();
    assertNull(sk1);
    assertNull(sk2);
    try { union.union(sk2); fail(); }
    catch (final NullPointerException e) { }
    final QuantilesDoublesSketch sk3 = union.getResultAndReset();
    assertNull(sk3);
  }

  @Test
  public void differentLargerK() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(128).build();
    final UpdatableQuantilesDoublesSketch sketch1 = buildAndLoadQS(256, 0);
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentLargerKDirect() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(128).build();
    final UpdatableQuantilesDoublesSketch sketch1 = buildAndLoadDQS(256, 0);
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.union(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentEmptySmallerK() {
    final int k128 = 128;
    final int k64 = 64;
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(k128).build();
    assertTrue(union.isEmpty()); //gadget is null
    assertFalse(union.hasMemorySegment());
    assertFalse(union.isOffHeap());

    //    byte[] unionByteArr = union.toByteArray();
    //    Assert.assertEquals(unionByteArr.length, 32 + 32); //empty

    final UpdatableQuantilesDoublesSketch sketch1 = buildAndLoadQS(k64, 0); //build smaller empty sketch
    union.union(sketch1);
    assertTrue(union.isEmpty()); //gadget is valid
    assertFalse(union.hasMemorySegment());
    assertFalse(union.isOffHeap());

    //    unionByteArr = union.toByteArray();
    //    int udBytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k64, 0);
    //    Assert.assertEquals(unionByteArr.length, udBytes); //empty

    assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.union(sketch1);
    assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentEmptySmallerKDirect() {
    final int k128 = 128;
    final int k64 = 64;
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().setMaxK(k128).build();
    assertTrue(union.isEmpty()); //gadget is null
    assertFalse(union.hasMemorySegment());
    assertFalse(union.isOffHeap());

    //    byte[] unionByteArr = union.toByteArray();
    //    Assert.assertEquals(unionByteArr.length, 32 + 32); //empty

    final UpdatableQuantilesDoublesSketch sketch1 = buildAndLoadDQS(k64, 0); //build smaller empty sketch
    union.union(sketch1);
    assertTrue(union.isEmpty()); //gadget is valid
    assertFalse(union.hasMemorySegment());
    assertFalse(union.isOffHeap());

    //    unionByteArr = union.toByteArray();
    //    int udBytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k64, 0);
    //    Assert.assertEquals(unionByteArr.length, udBytes); //empty

    assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.union(sketch1);
    assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void checkDirectInstance() {
    final int k = 128;
    final int n = 1000;
    final QuantilesDoublesUnionBuilder bldr = QuantilesDoublesUnion.builder();
    bldr.setMaxK(k);
    assertEquals(bldr.getMaxK(), k);
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k, n);
    final byte[] byteArr = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final QuantilesDoublesUnion union = bldr.build(seg, null);
    assertTrue(union.isEmpty());
    assertTrue(union.hasMemorySegment());
    assertFalse(union.isOffHeap());
    for (int i = 1; i <= n; i++) {
      union.update(i);
    }
    assertFalse(union.isEmpty());
    final QuantilesDoublesSketch res = union.getResult();
    final double median = res.getQuantile(.5);
    assertEquals(median, 500, 10);
    println(union.toString());
  }

  @Test
  public void checkWrapInstance() {
    final int k = 128;
    final int n = 1000;
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().setK(k).build();
    for (int i = 1; i <= n; i++) {
      sketch.update(i);
    }
    final double skMedian = sketch.getQuantile(.5);
    Assert.assertEquals(skMedian, 500, 10);

    final byte[] byteArr = sketch.toByteArray(false);
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.wrap(seg, null);
    Assert.assertFalse(union.isEmpty());
    assertTrue(union.hasMemorySegment());
    assertFalse(union.isOffHeap());
    final QuantilesDoublesSketch sketch2 = union.getResult();
    final double uMedian = sketch2.getQuantile(0.5);
    Assert.assertEquals(skMedian, uMedian, 0.0);

    // check serializing again
    final byte[] bytesOut = union.toByteArray();
    assertEquals(bytesOut.length, byteArr.length);
    assertEquals(bytesOut, byteArr); // wrapped, so should be exact
  }

  @Test
  public void isSameResourceHeap() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();
    Assert.assertFalse(union.isSameResource(null));
  }

  @Test
  public void isSameResourceDirect() {
    final MemorySegment seg1 = MemorySegment.ofArray(new byte[1000000]);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build(seg1, null);
    Assert.assertTrue(union.isSameResource(seg1));
    final MemorySegment seg2 = MemorySegment.ofArray(new byte[1000000]);
    Assert.assertFalse(union.isSameResource(seg2));
  }

  @Test
  public void emptyUnionSerDeIssue195() {
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.builder().build();
    final byte[] byteArr = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final QuantilesDoublesUnion union2 = QuantilesDoublesUnion.heapify(seg);
    Assert.assertEquals(seg.byteSize(), 8L);
    Assert.assertTrue(union2.isEmpty());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}
