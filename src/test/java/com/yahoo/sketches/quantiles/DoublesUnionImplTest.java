/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DirectUpdateDoublesSketchTest.buildAndLoadDQS;
import static com.yahoo.sketches.quantiles.HeapUpdateDoublesSketchTest.buildAndLoadQS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

public class DoublesUnionImplTest {

  @Test
  public void checkUnion1() {
    DoublesSketch result;
    final DoublesSketch qs1;
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256

    final DoublesSketch qs0 = buildAndLoadQS(256, 500);
    union.update(qs0); //me = null, that = valid, exact
    result = union.getResult();
    assertEquals(result.getN(), 500);
    assertEquals(result.getK(), 256);

    union.reset();
    qs1 = buildAndLoadQS(256, 1000); //first 1000
    union.update(qs1); //me = null,  that = valid, OK

    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);

    //check merge  me = valid, that = valid, both K's the same
    final DoublesSketch qs2 = buildAndLoadQS(256, 1000, 1000); //add 1000
    union.update(qs2);
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }

  @Test
  public void checkUnion1Direct() {
    DoublesSketch result;
    final DoublesSketch qs1;
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256

    final DoublesSketch qs0 = buildAndLoadDQS(256, 500);
    union.update(qs0); //me = null, that = valid, exact
    result = union.getResult();
    assertEquals(result.getN(), 500);
    assertEquals(result.getK(), 256);

    union.reset();
    qs1 = buildAndLoadDQS(256, 1000); //first 1000
    union.update(qs1); //me = null,  that = valid, OK

    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);

    //check merge  me = valid, that = valid, both K's the same
    final DoublesSketch qs2 = buildAndLoadDQS(256, 1000, 1000).compact(); //add 1000
    union.update(qs2);
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }

  @Test
  public void checkUnion2() {
    final DoublesSketch qs1 = buildAndLoadQS(256, 1000).compact();
    final DoublesSketch qs2 = buildAndLoadQS(128, 1000);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256
    assertEquals(union.getEffectiveK(), 256);

    union.update(qs1);
    final DoublesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);

    union.update(qs2);
    final DoublesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    assertEquals(union.getEffectiveK(), 128);
    println(union.toString());
  }

  @Test
  public void checkUnion2Direct() {
    final DoublesSketch qs1 = buildAndLoadDQS(256, 1000);
    final DoublesSketch qs2 = buildAndLoadDQS(128, 1000);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256
    assertEquals(union.getEffectiveK(), 256);

    union.update(qs1);
    final DoublesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);

    union.update(qs2);
    final DoublesSketch res2 = union.getResult();
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
    final DoublesSketch sketchIn1 = buildAndLoadQS(k1, n1);
    final int bytes = DoublesSketch.getUpdatableStorageBytes(k2, n2);//just for size
    final WritableMemory mem = WritableMemory.wrap(new byte[bytes]);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k2).build(mem); //virgin 256
    union.update(sketchIn1);
    assertEquals(union.getMaxK(), k2);
    assertEquals(union.getEffectiveK(), k1);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), n1, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getK(), k1);
  }

  @Test
  public void checkUnion3Direct() { //Union is direct, empty and with larger K than valid input
    final int k1 = 128;
    final int n1 = 2 * k1;
    final int k2 = 256;
    final int n2 = 2000;
    final DoublesSketch sketchIn1 = buildAndLoadDQS(k1, n1);
    final int bytes = DoublesSketch.getUpdatableStorageBytes(k2, n2);//just for size
    final WritableMemory mem = WritableMemory.wrap(new byte[bytes]);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k2).build(mem); //virgin 256
    union.update(sketchIn1);
    assertEquals(union.getMaxK(), k2);
    assertEquals(union.getEffectiveK(), k1);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), n1, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getK(), k1);
  }

  @Test
  public void checkUnion4() { //Union is direct, valid and with larger K than valid input
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = DoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final WritableMemory skMem = WritableMemory.wrap(new byte[bytes]);
    final UpdateDoublesSketch sketchIn1 = DoublesSketch.builder().setK(k1).build(skMem);
    for (int i = 0; i < n1; i++) { sketchIn1.update(i + 1); }

    final WritableMemory uMem = WritableMemory.wrap(new byte[bytes]);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(uMem); //virgin 256
    //DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256
    union.update(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    DoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxValue(), n1, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final DoublesSketch sketchIn2 = buildAndLoadQS(k2, n2, 17);
    union.reset();
    union.update(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxValue(), n2 + 17, 0.0);
    assertEquals(result.getMinValue(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion4Direct() { //Union is direct, valid and with larger K than valid input
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = DoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final WritableMemory skMem = WritableMemory.wrap(new byte[bytes]);
    final UpdateDoublesSketch sketchIn1 = DoublesSketch.builder().setK(k1).build(skMem);
    for (int i = 0; i < n1; i++) { sketchIn1.update(i + 1); }

    final WritableMemory uMem = WritableMemory.wrap(new byte[bytes]);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(uMem); //virgin 256
    union.update(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    DoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxValue(), n1, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final DoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17);
    union.reset();
    union.update(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxValue(), n2 + 17, 0.0);
    assertEquals(result.getMinValue(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion4DirectCompact() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 4;
    final int n2 = 5 * k2; //8
    final int bytes = DoublesSketch.getUpdatableStorageBytes(256, 50);//just for size
    final WritableMemory skMem = WritableMemory.wrap(new byte[bytes]);
    final UpdateDoublesSketch sketchIn0 = DoublesSketch.builder().setK(k1).build(skMem);
    for (int i = 0; i < n1; i++) { sketchIn0.update(i + 1); }
    final CompactDoublesSketch sketchIn1 = sketchIn0.compact();

    final WritableMemory uMem = WritableMemory.wrap(new byte[bytes]);
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(uMem); //virgin 256
    union.update(sketchIn1);
    assertEquals(union.getResult().getN(), n1);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), k1);
    DoublesSketch result = union.getResult();
    assertEquals(result.getN(), 16);
    assertEquals(result.getMaxValue(), n1, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getK(), k1);

    final CompactDoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17).compact();
    union.reset();
    union.update(sketchIn2);
    result = union.getResult();
    assertEquals(result.getMaxValue(), n2 + 17, 0.0);
    assertEquals(result.getMinValue(), 1.0 + 17, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion5() { //Union is direct, valid and with larger K than valid input
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = DoublesSketch.getUpdatableStorageBytes(256, 50);//big enough
    final WritableMemory skMem = WritableMemory.wrap(new byte[bytes]);
    DoublesSketch.builder().setK(256).build(skMem);

    final DoublesUnion union = DoublesUnionImpl.heapifyInstance(skMem);
    assertEquals(union.getResult().getN(), 0);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), 256);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getK(), 256);

    final DoublesSketch sketchIn2 = buildAndLoadQS(k2, n2, 17);
    union.update(sketchIn2);
    println("\nFinal" + union.getResult().toString(true, true));
    assertEquals(union.getResult().getN(), n2);
  }

  @Test
  public void checkUnion5Direct() { //Union is direct, valid and with larger K than valid input
    final int k2 = 4;
    final int n2 = 2 * k2; //8
    final int bytes = DoublesSketch.getUpdatableStorageBytes(256, 50);//big enough
    final WritableMemory skMem = WritableMemory.wrap(new byte[bytes]);
    DoublesSketch.builder().setK(256).build(skMem);

    final DoublesUnion union = DoublesUnionImpl.heapifyInstance(skMem);
    assertEquals(union.getResult().getN(), 0);
    assertEquals(union.getMaxK(), 256);
    assertEquals(union.getEffectiveK(), 256);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getK(), 256);

    final DoublesSketch sketchIn2 = buildAndLoadDQS(k2, n2, 17);
    union.update(sketchIn2);
    println("\nFinal" + union.getResult().toString(true, true));
    assertEquals(union.getResult().getN(), n2);
  }

  @Test
  public void checkUnion6() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 16;
    final int n2 = 2 * k2; //32
    final DoublesSketch sk1 = buildAndLoadQS(k1, n1, 0);
    final DoublesSketch sk2 = buildAndLoadQS(k2, n2, n1);
    final DoublesUnion union = DoublesUnionImpl.heapifyInstance(sk1);
    union.update(sk2);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), n1 + n2, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion6Direct() {
    final int k1 = 8;
    final int n1 = 2 * k1; //16
    final int k2 = 16;
    final int n2 = 2 * k2; //32
    final DoublesSketch sk1 = buildAndLoadDQS(k1, n1, 0);
    final DoublesSketch sk2 = buildAndLoadDQS(k2, n2, n1);
    final DoublesUnion union = DoublesUnionImpl.heapifyInstance(sk1);
    union.update(sk2);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), n1 + n2, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    println("\nFinal" + union.getResult().toString(true, true));
  }

  @Test
  public void checkUnion7() {
    final DoublesUnion union = DoublesUnionImpl.heapInstance(16);
    final DoublesSketch skEst = buildAndLoadQS(32, 64); //other is bigger, est
    union.update(skEst);
    println(skEst.toString(true, true));
    println(union.toString(true, true));
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), 64, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
  }

  @Test
  public void checkUnionQuantiles() {
    final int k = 128;
    final int n1 = k * 13;
    final int n2 = (k * 8) + (k / 2);
    final int n = n1 + n2;
    final double errorTolerance = 0.0175 * n; // assuming k = 128
    final UpdateDoublesSketch sketch1 = buildAndLoadQS(k, n1);
    final CompactDoublesSketch sketch2 = buildAndLoadQS(k, n2, n1).compact();
    final DoublesUnion union = DoublesUnion.builder().setMaxK(256).build(); //virgin 256
    union.update(sketch2);
    union.update(sketch1);
    final Memory mem = Memory.wrap(union.getResult().toByteArray(true));
    final DoublesSketch result = DoublesSketch.wrap(mem);
    assertEquals(result.getN(), n1 + n2);
    assertEquals(result.getK(), k);

    for (double fraction = 0.05; fraction < 1.0; fraction += 0.05) {
      assertEquals(result.getQuantile(fraction), fraction * n, errorTolerance);
    }
  }

  @Test
  public void checkUnion7Direct() {
    final DoublesUnion union = DoublesUnionImpl.heapInstance(16);
    final DoublesSketch skEst = buildAndLoadDQS(32, 64); //other is bigger, est
    union.update(skEst);
    final DoublesSketch result = union.getResult();
    assertEquals(result.getMaxValue(), 64, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    //    println(skEst.toString(true, true));
    //    println(union.toString(true, true));
  }

  @Test
  public void checkUpdateMemory() {
    final DoublesSketch qs1 = buildAndLoadQS(256, 1000);
    final int bytes = qs1.getCompactStorageBytes();
    final WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
    qs1.putMemory(dstMem);
    final Memory srcMem = dstMem;

    final DoublesUnion union = DoublesUnion.builder().build(); //virgin
    union.update(srcMem);
    for (int i = 1000; i < 2000; i++) { union.update(i); }
    final DoublesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxValue(), 1999, 0.0);
    final String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }

  @Test
  public void checkUpdateMemoryDirect() {
    final DoublesSketch qs1 = buildAndLoadDQS(256, 1000);
    final int bytes = qs1.getCompactStorageBytes();
    final WritableMemory dstMem = WritableMemory.wrap(new byte[bytes]);
    qs1.putMemory(dstMem);
    final Memory srcMem = dstMem;

    final DoublesUnion union = DoublesUnion.builder().build(); //virgin
    union.update(srcMem);
    for (int i = 1000; i < 2000; i++) { union.update(i); }
    final DoublesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxValue(), 1999, 0.0);
    final String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }

  @Test
  public void checkUnionUpdateLogic() {
    final HeapUpdateDoublesSketch qs1 = null;
    final HeapUpdateDoublesSketch qs2 = (HeapUpdateDoublesSketch) buildAndLoadQS(256, 0);
    DoublesUnionImpl.updateLogic(256, qs1, qs2); //null, empty
    DoublesUnionImpl.updateLogic(256, qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    final DoublesSketch result = DoublesUnionImpl.updateLogic(256, qs2, qs1); //valid, null
    assertEquals(result.getMaxValue(), result.getMinValue(), 0.0);
  }

  @Test
  public void checkUnionUpdateLogicDirect() {
    final HeapUpdateDoublesSketch qs1 = null;
    final DirectUpdateDoublesSketch qs2 = (DirectUpdateDoublesSketch) buildAndLoadDQS(256, 0);
    DoublesUnionImpl.updateLogic(256, qs1, qs2); //null, empty
    DoublesUnionImpl.updateLogic(256, qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    final DoublesSketch result = DoublesUnionImpl.updateLogic(256, qs2, qs1); //valid, null
    assertEquals(result.getMaxValue(), result.getMinValue(), 0.0);
  }

  @Test
  public void checkUnionUpdateLogicDirectDownsampled() {
    final DirectUpdateDoublesSketch qs1 = (DirectUpdateDoublesSketch) buildAndLoadDQS(256, 1000);
    final DirectUpdateDoublesSketch qs2 = (DirectUpdateDoublesSketch) buildAndLoadDQS(128, 2000);
    final DoublesSketch result = DoublesUnionImpl.updateLogic(128, qs1, qs2);
    assertEquals(result.getMaxValue(), 2000.0, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
    assertEquals(result.getN(), 3000);
    assertEquals(result.getK(), 128);
  }

  @Test
  public void checkUnionUpdateLogic2() {
    DoublesSketch qs1 = DoublesSketch.builder().build();
    final DoublesSketch qs2 = DoublesSketch.builder().build();
    final DoublesUnion union = DoublesUnion.builder().build();
    union.update(qs1);
    union.update(qs2); //case 5
    qs1 = buildAndLoadQS(128, 1000);
    union.update(qs1);
    union.update(qs2); //case 9
    final DoublesSketch result = union.getResult();
    //println(union.toString(true, true));
    assertEquals(result.getMaxValue(), 1000.0, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);

  }

  @Test
  public void checkUnionUpdateLogic2Direct() {
    DoublesSketch qs1 = DoublesSketch.builder().build();
    final DoublesSketch qs2 = DoublesSketch.builder().build();
    final DoublesUnion union = DoublesUnion.builder().build();
    union.update(qs1);
    union.update(qs2); //case 5
    qs1 = buildAndLoadDQS(128, 1000);
    union.update(qs1);
    union.update(qs2); //case 9
    final DoublesSketch result = union.getResult();
    //println(union.toString(true, true));
    assertEquals(result.getMaxValue(), 1000.0, 0.0);
    assertEquals(result.getMinValue(), 1.0, 0.0);
  }

  @Test
  public void checkResultAndReset() {
    final DoublesSketch qs1 = buildAndLoadQS(256, 0);
    final DoublesUnion union = DoublesUnion.heapify(qs1);
    final DoublesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void checkResultAndResetDirect() {
    final DoublesSketch qs1 = buildAndLoadDQS(256, 0);
    final DoublesUnion union = DoublesUnion.heapify(qs1);
    final DoublesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void checkResultViaMemory() {
    // empty gadget
    final DoublesUnion union = DoublesUnion.builder().build();

    // memory too small
    WritableMemory mem = WritableMemory.allocate(1);
    try {
      union.getResult(mem);
      fail();
    } catch (final SketchesArgumentException e) {
      // expected
    }

    // sufficient memory
    mem = WritableMemory.allocate(8);
    DoublesSketch result = union.getResult(mem);
    assertTrue(result.isEmpty());

    final int k = 128;
    final int n = 1392;
    mem = WritableMemory.allocate(DoublesSketch.getUpdatableStorageBytes(k, n));
    final DoublesSketch qs = buildAndLoadQS(k, n);
    union.update(qs);
    result = union.getResult(mem);
    DoublesSketchTest.testSketchEquality(result, qs);
  }

  @Test
  public void updateWithDoubleValueOnly() {
    final DoublesUnion union = DoublesUnion.builder().build();
    union.update(123.456);
    final DoublesSketch qs = union.getResultAndReset();
    assertEquals(qs.getN(), 1);
  }

  @Test
  public void checkEmptyUnion() {
    final DoublesUnionImpl union = DoublesUnionImpl.heapInstance(128);
    final DoublesSketch sk = union.getResult();
    assertNotNull(sk);
    final byte[] bytes = union.toByteArray();
    assertEquals(bytes.length, 8); //
    final String s = union.toString();
    assertNotNull(s);
  }

  @Test
  public void checkUnionNulls() {
    final DoublesUnion union = DoublesUnionImpl.heapInstance(128);
    final DoublesSketch sk1 = union.getResultAndReset();
    final DoublesSketch sk2 = union.getResultAndReset();
    assertNull(sk1);
    assertNull(sk2);
    union.update(sk2);
    final DoublesSketch sk3 = union.getResultAndReset();
    assertNull(sk3);
  }

  @Test
  public void differentLargerK() {
    final DoublesUnion union = DoublesUnion.builder().setMaxK(128).build();
    final UpdateDoublesSketch sketch1 = buildAndLoadQS(256, 0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentLargerKDirect() {
    final DoublesUnion union = DoublesUnion.builder().setMaxK(128).build();
    final UpdateDoublesSketch sketch1 = buildAndLoadDQS(256, 0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentEmptySmallerK() {
    final int k128 = 128;
    final int k64 = 64;
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k128).build();
    assertTrue(union.isEmpty()); //gadget is null
    Assert.assertFalse(union.isDirect());

    //    byte[] unionByteArr = union.toByteArray();
    //    Assert.assertEquals(unionByteArr.length, 32 + 32); //empty

    final UpdateDoublesSketch sketch1 = buildAndLoadQS(k64, 0); //build smaller empty sketch
    union.update(sketch1);
    assertTrue(union.isEmpty()); //gadget is valid
    Assert.assertFalse(union.isDirect());

    //    unionByteArr = union.toByteArray();
    //    int udBytes = DoublesSketch.getUpdatableStorageBytes(k64, 0);
    //    Assert.assertEquals(unionByteArr.length, udBytes); //empty

    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentEmptySmallerKDirect() {
    final int k128 = 128;
    final int k64 = 64;
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k128).build();
    assertTrue(union.isEmpty()); //gadget is null
    Assert.assertFalse(union.isDirect());

    //    byte[] unionByteArr = union.toByteArray();
    //    Assert.assertEquals(unionByteArr.length, 32 + 32); //empty

    final UpdateDoublesSketch sketch1 = buildAndLoadDQS(k64, 0); //build smaller empty sketch
    union.update(sketch1);
    assertTrue(union.isEmpty()); //gadget is valid
    Assert.assertFalse(union.isDirect());

    //    unionByteArr = union.toByteArray();
    //    int udBytes = DoublesSketch.getUpdatableStorageBytes(k64, 0);
    //    Assert.assertEquals(unionByteArr.length, udBytes); //empty

    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void checkDirectInstance() {
    final int k = 128;
    final int n = 1000;
    final DoublesUnionBuilder bldr = DoublesUnion.builder();
    bldr.setMaxK(k);
    Assert.assertEquals(bldr.getMaxK(), k);
    final int bytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    final byte[] byteArr = new byte[bytes];
    final WritableMemory mem = WritableMemory.wrap(byteArr);
    final DoublesUnion union = bldr.build(mem);
    assertTrue(union.isEmpty());
    assertTrue(union.isDirect());
    for (int i = 1; i <= n; i++) {
      union.update(i);
    }
    Assert.assertFalse(union.isEmpty());
    final DoublesSketch res = union.getResult();
    final double median = res.getQuantile(.5);
    Assert.assertEquals(median, 500, 10);
    println(union.toString());
  }

  @Test
  public void checkWrapInstance() {
    final int k = 128;
    final int n = 1000;
    final UpdateDoublesSketch sketch = DoublesSketch.builder().setK(k).build();
    for (int i = 1; i <= n; i++) {
      sketch.update(i);
    }
    final double skMedian = sketch.getQuantile(.5);
    Assert.assertEquals(skMedian, 500, 10);

    final byte[] byteArr = sketch.toByteArray(false);
    final WritableMemory mem = WritableMemory.wrap(byteArr);
    final DoublesUnion union = DoublesUnion.wrap(mem);
    Assert.assertFalse(union.isEmpty());
    assertTrue(union.isDirect());
    final DoublesSketch sketch2 = union.getResult();
    final double uMedian = sketch2.getQuantile(0.5);
    Assert.assertEquals(skMedian, uMedian, 0.0);

    // check serializing again
    final byte[] bytesOut = union.toByteArray();
    assertEquals(bytesOut.length, byteArr.length);
    assertEquals(bytesOut, byteArr); // wrapped, so should be exact
  }

  @Test
  public void isSameResourceHeap() {
    DoublesUnion union = DoublesUnion.builder().build();
    Assert.assertFalse(union.isSameResource(null));
  }

  @Test
  public void isSameResourceDirect() {
    WritableMemory mem1 = WritableMemory.wrap(new byte[1000000]);
    DoublesUnion union = DoublesUnion.builder().build(mem1);
    Assert.assertTrue(union.isSameResource(mem1));
    WritableMemory mem2 = WritableMemory.wrap(new byte[1000000]);
    Assert.assertFalse(union.isSameResource(mem2));
  }

  @Test
  public void emptyUnionSerDeIssue195() {
    DoublesUnion union = DoublesUnion.builder().build();
    byte[] byteArr = union.toByteArray();
    Memory mem = Memory.wrap(byteArr);
    DoublesUnion union2 = DoublesUnion.heapify(mem);
    Assert.assertEquals(mem.getCapacity(), 8L);
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
