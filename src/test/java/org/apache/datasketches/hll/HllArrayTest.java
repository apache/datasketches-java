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

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class HllArrayTest {

  @Test
  public void checkCompositeEst() {
    testComposite(4, HLL_8, 1000);
    testComposite(5, HLL_8, 1000);
    testComposite(6, HLL_8, 1000);
    testComposite(13, HLL_8, 10000);
  }

  @Test
  public void checkBigHipGetRse() {
    HllSketch sk = new HllSketch(13, HLL_8);
    for (int i = 0; i < 10000; i++) {
      sk.update(i);
    }
  }

  private static void testComposite(int lgK, TgtHllType tgtHllType, int n) {
    Union u = new Union(lgK);
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      u.update(i);
      sk.update(i);
    }
    u.update(sk); //merge
    HllSketch res = u.getResult(HLL_8);
    res.getCompositeEstimate();
  }

  @Test
  public void toByteArray_Heapify() {
    int lgK = 4;
    int u = 8;
    toByteArrayHeapify(lgK, HLL_4, u, true);
    toByteArrayHeapify(lgK, HLL_6, u, false);
    toByteArrayHeapify(lgK, HLL_8, u, true);

    lgK = 16;
    u = (((1 << (lgK - 3))*3)/4) + 100;
    toByteArrayHeapify(lgK, HLL_4, u, false);
    toByteArrayHeapify(lgK, HLL_6, u, true);
    toByteArrayHeapify(lgK, HLL_8, u, false);

    lgK = 21;
    u = (((1 << (lgK - 3))*3)/4) + 1000;
    toByteArrayHeapify(lgK, HLL_4, u, true);
    toByteArrayHeapify(lgK, HLL_6, u, false);
    toByteArrayHeapify(lgK, HLL_8, u, true);
  }

  private static void toByteArrayHeapify(int lgK, TgtHllType tgtHllType, int u, boolean direct) {
    HllSketch sk1;
    WritableMemory wmem = null;
    if (direct) {
      int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      wmem = WritableMemory.allocate(bytes);
      sk1 = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sk1 = new HllSketch(lgK, tgtHllType);
    }

    for (int i = 0; i < u; i++) {
      sk1.update(i);
    }
    assert sk1.hllSketchImpl instanceof AbstractHllArray;
    if (sk1.hllSketchImpl instanceof HllArray) {
      assertFalse(sk1.hllSketchImpl.isMemory());
      assertFalse(sk1.isSameResource(wmem));
    } else { //DirectHllArray
      assertTrue(sk1.hllSketchImpl.isMemory());
      assertTrue(sk1.isSameResource(wmem));
    }

    //sk1.update(u);
    double est1 = sk1.getEstimate();
    assertEquals(est1, u, u * .03);
    assertEquals(sk1.getHipEstimate(), est1, 0.0);

    //misc calls
    sk1.hllSketchImpl.putEmptyFlag(false);
    sk1.hllSketchImpl.putRebuildCurMinNumKxQFlag(true);
    sk1.hllSketchImpl.putRebuildCurMinNumKxQFlag(false);

    byte[] byteArray = sk1.toCompactByteArray();
    HllSketch sk2 = HllSketch.heapify(byteArray);
    double est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);

    byteArray = sk1.toUpdatableByteArray();
    sk2 = HllSketch.heapify(byteArray);
    est2 = sk2.getEstimate();
    assertEquals(est2, est1, 0.0);

    sk1.reset();
    assertEquals(sk1.getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkHll4Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    HllSketch skH4 = new HllSketch(lgK, HLL_4);
    for (int i = 0; i < k; i++) { skH4.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skH4.hllSketchImpl;
    try {
      absHllArr.updateSlotNoKxQ(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkDHll4Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, HLL_4);
    HllSketch skD4 = new HllSketch(lgK, HLL_4, WritableMemory.allocate(bytes));
    for (int i = 0; i < k; i++) { skD4.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skD4.hllSketchImpl;
    try {
      absHllArr.updateSlotNoKxQ(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkHll6Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    HllSketch skH6 = new HllSketch(lgK, HLL_6);
    for (int i = 0; i < k; i++) { skH6.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skH6.hllSketchImpl;
    try {
      absHllArr.getNibble(0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.putNibble(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.updateSlotNoKxQ(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkDHll6Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, HLL_6);
    HllSketch skD6 = new HllSketch(lgK, HLL_6, WritableMemory.allocate(bytes));
    for (int i = 0; i < k; i++) { skD6.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skD6.hllSketchImpl;
    try {
      absHllArr.getNibble(0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.putNibble(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.updateSlotNoKxQ(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkHll8Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    HllSketch skH6 = new HllSketch(lgK, HLL_8);
    for (int i = 0; i < k; i++) { skH6.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skH6.hllSketchImpl;
    try {
      absHllArr.getNibble(0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.putNibble(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkDHll8Exceptions() {
    int lgK = 4;
    int k = 1 << lgK;
    int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, HLL_8);
    HllSketch skD6 = new HllSketch(lgK, HLL_8, WritableMemory.allocate(bytes));
    for (int i = 0; i < k; i++) { skD6.update(i); }
    AbstractHllArray absHllArr = (AbstractHllArray)skD6.hllSketchImpl;
    try {
      absHllArr.getNibble(0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
    try {
      absHllArr.putNibble(0,0);
      fail();
    }
    catch (SketchesStateException e) { } //OK
  }

  @Test
  public void checkIsCompact() {
    HllSketch sk = new HllSketch(4);
    for (int i = 0; i < 8; i++) { sk.update(i); }
    assertFalse(sk.isCompact());
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
