/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.HeapDoublesSketchTest.buildQS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

public class DoublesUnionImplTest {

  @Test
  public void checkUnion1() {
    DoublesSketch result;
    DoublesSketch qs1 = null;
    DoublesUnion union = DoublesUnion.builder().setK(256).build(); //virgin 256

    qs1 = buildQS(256, 1000); //first 1000
    union.update(qs1); //copy   me = null,  that = valid, OK

    //check copy   me = null,  that = valid
    result = union.getResult();
    assertEquals(result.getN(), 1000);
    assertEquals(result.getK(), 256);

    //check merge  me = valid, that = valid, both K's the same
    DoublesSketch qs2 = buildQS(256, 1000, 1000); //add 1000
    union.update(qs2);
    result = union.getResult();
    assertEquals(result.getN(), 2000);
    assertEquals(result.getK(), 256);
  }

  @Test
  public void checkUnion2() {
    DoublesSketch qs1 = buildQS(256, 1000);
    DoublesSketch qs2 = buildQS(128, 1000);
    DoublesUnion union = DoublesUnion.builder().setK(256).build(); //virgin 256

    union.update(qs1);
    DoublesSketch res1 = union.getResult();
    //println(res1.toString());
    assertEquals(res1.getN(), 1000);
    assertEquals(res1.getK(), 256);

    union.update(qs2);
    DoublesSketch res2 = union.getResult();
    assertEquals(res2.getN(), 2000);
    assertEquals(res2.getK(), 128);
    //println(union.toString());
  }

  @Test
  public void checkUpdateMemory() {
    DoublesSketch qs1 = buildQS(256, 1000);
    int bytes = qs1.getCompactStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;

    DoublesUnion union = DoublesUnion.builder().build(); //virgin
    union.update(srcMem);
    for (int i=1000; i<2000; i++) union.update(i);
    DoublesSketch qs2 = union.getResult();
    assertEquals(qs2.getMaxValue(), 1999, 0.0);
    String s = union.toString();
    println(s); //enable printing to see
    union.reset(); //sets to null
  }

  @Test
  public void checkUnionUpdateLogic() {
    HeapDoublesSketch qs1 = null;
    HeapDoublesSketch qs2 = (HeapDoublesSketch)buildQS(256, 0);
    DoublesSketch result = DoublesUnionImpl.updateLogic(256, qs1, qs2); //null, empty
    result = DoublesUnionImpl.updateLogic(256, qs2, qs1); //empty, null
    qs2.update(1); //no longer empty
    result = DoublesUnionImpl.updateLogic(256, qs2, qs1); //valid, null
    assertEquals(result.getMaxValue(), result.getMinValue(), 0.0);
  }

  @Test
  public void checkUnionUpdateLogic2() {
    DoublesSketch qs1 = DoublesSketch.builder().build();
    DoublesSketch qs2 = DoublesSketch.builder().build();
    DoublesUnion union = DoublesUnion.builder().build();
    union.update(qs1);
    union.update(qs2); //case 5
    qs1 = buildQS(128, 1000);
    union.update(qs1);
    union.update(qs2); //case 9
  }

  @Test//(expectedExceptions = SketchesStateException.class)
  public void checkResultAndReset() {
    DoublesSketch qs1 = buildQS(256, 0);
    DoublesUnion union = DoublesUnionBuilder.heapify(qs1);
    DoublesSketch qs2 = union.getResultAndReset();
    assertEquals(qs2.getK(), 256);
  }

  @Test
  public void updateWithDoubleValueOnly() {
    DoublesUnion union = DoublesUnion.builder().build();
    union.update(123.456);
    DoublesSketch qs = union.getResultAndReset();
    assertEquals(qs.getN(), 1);
  }

  @Test
  public void checkEmptyUnion() {
    DoublesUnionImpl union = DoublesUnionImpl.heapInstance(128);
    DoublesSketch sk = union.getResult();
    assertNotNull(sk);
    String s = union.toString();
    assertNotNull(s);
  }

  @Test
  public void checkUnionNulls() {
    DoublesUnion union = DoublesUnionImpl.heapInstance(128);
    DoublesSketch sk1 = union.getResultAndReset();
    DoublesSketch sk2 = union.getResultAndReset();
    assertNull(sk1);
    assertNull(sk2);
    union.update(sk2);
    DoublesSketch sk3 = union.getResultAndReset();
    assertNull(sk3);
  }

  @Test
  public void differentLargerK() {
    DoublesUnion union = DoublesUnion.builder().setK(128).build();
    DoublesSketch sketch1 = buildQS(256, 0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 128);
  }

  @Test
  public void differentSmallerK() {
    int k1 = 128;
    int k2 = 64;
    DoublesUnion union = DoublesUnion.builder().setK(k1).build();
    Assert.assertTrue(union.isEmpty()); //gadget is null
    Assert.assertFalse(union.isDirect());

    byte[] unionByteArr = union.toByteArray();
    Assert.assertEquals(unionByteArr.length, 32 + 32); //empty

    DoublesSketch sketch1 = buildQS(k2, 0); //build smaller sketch
    union.update(sketch1);
    Assert.assertTrue(union.isEmpty()); //gadget is valid
    Assert.assertFalse(union.isDirect());
    unionByteArr = union.toByteArray();
    int udBytes = DoublesSketch.getUpdatableStorageBytes(k2, 0);
    Assert.assertEquals(unionByteArr.length, udBytes); //empty

    Assert.assertEquals(union.getResult().getK(), 64);
    sketch1.update(1.0);
    union.update(sketch1);
    Assert.assertEquals(union.getResult().getK(), 64);
  }

  @Test
  public void checkDirectInstance() {
    int k = 128;
    int n = 1000;
    DoublesUnionBuilder bldr = DoublesUnion.builder();
    bldr.setK(k);
    Assert.assertEquals(bldr.getK(), k);
    int bytes = DoublesSketch.getUpdatableStorageBytes(k, n);
    byte[] byteArr = new byte[bytes];
    Memory mem = new NativeMemory(byteArr);
    bldr.initMemory(mem);
    Assert.assertEquals(mem.getCapacity(), bldr.getMemory().getCapacity());
    DoublesUnion union = bldr.build();
    Assert.assertTrue(union.isEmpty());
    Assert.assertTrue(union.isDirect());
    for (int i = 1; i <= n; i++) {
      union.update(i);
    }
    Assert.assertFalse(union.isEmpty());
    DoublesSketch res = union.getResult();
    double median = res.getQuantile(.5);
    Assert.assertEquals(median, 500, 10);
  }

  @Test
  public void checkWrapInstance() {
    int k = 128;
    int n = 1000;
    DoublesSketch sketch = DoublesSketch.builder().build(k);
    for (int i = 1; i <= n; i++) {
      sketch.update(i);
    }
    double skMedian = sketch.getQuantile(.5);
    Assert.assertEquals(skMedian, 500, 10);

    byte[] byteArr = sketch.toByteArray(true, false);
    Memory mem = new NativeMemory(byteArr);
    DoublesUnion union = DoublesUnionBuilder.wrap(mem);
    Assert.assertFalse(union.isEmpty());
    Assert.assertTrue(union.isDirect());
    DoublesSketch sketch2 = union.getResult();
    double uMedian = sketch2.getQuantile(0.5);
    Assert.assertEquals(skMedian, uMedian, 0.0);
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
