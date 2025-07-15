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

package org.apache.datasketches.req;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.req.FloatBuffer;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ReqFloatBufferTest {

  @Test
  public void checkTrimCount() {
    checkTrimCountImpl(true);
    checkTrimCountImpl(false);
  }

  private static void checkTrimCountImpl(final boolean spaceAtBottom) {
    final FloatBuffer buf = new FloatBuffer(16, 4, spaceAtBottom);
    for (int i = 0; i < 8; i++) { buf.append(i+1); }
    assertEquals(buf.getCount(), 8);
    buf.trimCount(4);
    assertEquals(buf.getCount(), 4);
  }

  @Test
  public void checkGetEvensOrOdds() {
    checkGetEvensOrOddsImpl(false, false);
    checkGetEvensOrOddsImpl(false, true);
    checkGetEvensOrOddsImpl(true, false);
    checkGetEvensOrOddsImpl(true, true);
  }

  private static void checkGetEvensOrOddsImpl(final boolean odds, final boolean spaceAtBottom) {
    final int cap = 16;
    final FloatBuffer buf = new FloatBuffer(cap, 0, spaceAtBottom);
    for (int i = 0; i < (cap/2); i++) {
      buf.append(i);
    }
    final FloatBuffer out = buf.getEvensOrOdds(0, cap/2, odds);
    //println("odds: " + odds + ", spaceAtBottom: " + spaceAtBottom);
    for (int i = 0; i < out.getCount(); i++) {
      final int v = (int)out.getItem(i);
      if (odds) { assertTrue((v & 1) == 1); }
      else { assertTrue((v & 1) == 0); }
      //print(v + " ");
    }
    //println("");
  }

  @Test
  public void checkAppendAndSpaceTop() {
    checkAppendAndSpaceImpl(true);
    checkAppendAndSpaceImpl(false);
  }

  private static void checkAppendAndSpaceImpl(final boolean spaceAtBottom) {
    final FloatBuffer buf = new FloatBuffer(2, 2, spaceAtBottom);
    assertEquals(buf.getCount(), 0);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 2);
    buf.append(1);
    assertEquals(buf.getCount(), 1);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 1);
    buf.append(2);
    assertEquals(buf.getCount(), 2);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 0);
    buf.append(3);
    assertEquals(buf.getCount(), 3);
    assertEquals(buf.getCapacity(), 5);
    assertEquals(buf.getSpace(), 2);
  }

  @Test
  public void checkEnsureCapacity() {
    checkEnsureCapacityImpl(true);
    checkEnsureCapacityImpl(false);
  }

  private static void checkEnsureCapacityImpl(final boolean spaceAtBottom) {
    final FloatBuffer buf = new FloatBuffer(4, 2, spaceAtBottom);
    buf.append(2);
    buf.append(1);
    buf.append(3);
    buf.ensureCapacity(8);
    buf.sort();
    assertEquals(buf.getItem(0), 1.0f);
    assertEquals(buf.getItem(1), 2.0f);
    assertEquals(buf.getItem(2), 3.0f);
  }

  @Test
  public void checkCountLessThan() {
    checkCountLessThanImpl(true);
    checkCountLessThanImpl(false);
  }

  private static void checkCountLessThanImpl(final boolean spaceAtBottom) {
    final float[] sortedArr = {1,2,3,4,5,6,7};
    final FloatBuffer buf = FloatBuffer.wrap(sortedArr, true, spaceAtBottom);
    final FloatBuffer buf2 = new FloatBuffer(7,0, spaceAtBottom);
    buf2.mergeSortIn(buf);
    assertEquals(buf2.getCountWithCriterion(4, EXCLUSIVE), 3);
    buf2.mergeSortIn(buf);
    assertEquals(buf2.getCountWithCriterion(4, EXCLUSIVE), 6);
    assertEquals(buf2.getCount(), 14);
    buf2.trimCount(12);
    assertEquals(buf2.getCount(), 12);
  }

  @Test
  public void checkCountWcriteria() {
    final int delta = 0;
    final int cap = 16;
    final boolean spaceAtBottom = true;
    for (int len = 5; len < 10; len++) {
      iterateValues(createSortedFloatBuffer(cap, delta, spaceAtBottom, len), len);
      iterateValues(createSortedFloatBuffer(cap, delta, !spaceAtBottom, len), len);
    }
  }

  private static void iterateValues(final FloatBuffer buf, final int len) {
    for (float v = 0.5f; v <= (len + 0.5f); v += 0.5f) {
      checkCountWithCriteria(buf, v);
    }
  }

  //@Test
  public void checkCount() {
    final FloatBuffer buf = createSortedFloatBuffer(120, 0, true, 100);
    println("LT: " + buf.getCountWithCriterion(100, EXCLUSIVE));
    println("LE: " + buf.getCountWithCriterion(100, INCLUSIVE));
  }

  private static void checkCountWithCriteria(final FloatBuffer buf, final float v) {
    int count;
    final int len = buf.getCount();
    final int iv = (int) v;
    count = buf.getCountWithCriterion(v, EXCLUSIVE);
    assertEquals(count, v > len ? len : v <= 1 ? 0 : iv == v? iv - 1 : iv);
    count = buf.getCountWithCriterion(v, INCLUSIVE);
    assertEquals(count, v >= len ? len : v < 1 ? 0 : iv);
  }

  /**
   * Creates a FloatBuffer with data
   * @param cap size of the buffer
   * @param delta incremental growth size
   * @param sab space-at-bottom T/F
   * @param len number of values to load, starting at 1.0f.
   * @return FloatBuffer
   */
  private static FloatBuffer createSortedFloatBuffer(final int cap, final int delta,
      final boolean sab, final int len) {
    final FloatBuffer buf = new FloatBuffer(cap, delta, sab);
    for (int i = 0; i < len; i++) { buf.append(i + 1); }
    return buf;
  }

  @Test
  public void checkMergeSortIn() {
    checkMergeSortInImpl(true);
    checkMergeSortInImpl(false);
  }

  private static void checkMergeSortInImpl(final boolean spaceAtBottom) {
    final float[] arr1 = {1,2,5,6}; //both must be sorted
    final float[] arr2 = {3,4,7,8};
    final FloatBuffer buf1 = new FloatBuffer(12, 0, spaceAtBottom);
    final FloatBuffer buf2 = new FloatBuffer(12, 0, spaceAtBottom);
    for (int i = 0; i < arr1.length; i++) { buf1.append(arr1[i]); }
    for (int i = 0; i < arr2.length; i++) { buf2.append(arr2[i]); }

    assertEquals(buf1.getSpace(), 8);
    assertEquals(buf2.getSpace(), 8);
    assertEquals(buf1.getCount(), 4);
    assertEquals(buf2.getCount(), 4);

    buf1.sort();
    buf2.sort();
    buf1.mergeSortIn(buf2);

    assertEquals(buf1.getSpace(), 4);
    final int len = buf1.getCount();
    assertEquals(len, 8);

    for (int i = 0; i < len; i++) {
      final int item = (int)buf1.getItem(i);
      assertEquals(item, i+1);
      //print(item + " ");
    }
    //println("");
  }

  @Test
  private static void checkMergeSortInNotSorted() {
    final float[] arr1 = {6,5,2,1};
    final float[] arr2 = {8,7,4,3};
    final FloatBuffer buf1 = new FloatBuffer(4, 0, false);
    final FloatBuffer buf2 = new FloatBuffer(4, 0, false);
    for (int i = 0; i < 4; i++) {
      buf1.append(arr1[i]);
      buf2.append(arr2[i]);
    }
    try { buf1.mergeSortIn(buf2); fail(); }
    catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkGetCountLtOrEqOddRange() {
    final FloatBuffer buf = new FloatBuffer(8, 0, false);
    assertTrue(buf.isEmpty());
    buf.append(3); buf.append(2); buf.append(1);
    buf.trimCount(4);
    assertEquals(buf.getCount(), 3);
    final int cnt = buf.getCountWithCriterion(3.0f, INCLUSIVE);
    assertEquals(cnt, 3);
    assertEquals(buf.getItemFromIndex(2), 3.0f);
    try { buf.getEvensOrOdds(0, 3, false); fail(); } catch (final SketchesArgumentException e) {}
  }

  @Test
  public void checkTrimCapacityToCount() {
    final FloatBuffer buf = new FloatBuffer(100, 100, true);
    for (int i = 0; i <= 100; i++) { buf.append(i); }
    assertEquals(buf.getCapacity(), 201);
    assertEquals(buf.getCount(), 101);
    buf.trimCapacity();
    assertEquals(buf.getItemFromIndex(0), 100f);
    assertEquals(buf.getCapacity(), 101);
    assertEquals(buf.getCount(), 101);
  }

  @Test
  public void checkSerDe() {
    checkSerDeImpl(true);
    checkSerDeImpl(false);
  }

  private static void checkSerDeImpl(final boolean hra) {
    final FloatBuffer fbuf = new FloatBuffer(100, 100, hra);
    for (int i = 0; i <= 100; i++) { fbuf.append(i); }
    final int capacity = fbuf.getCapacity();
    final int count = fbuf.getCount();
    final int delta = fbuf.getDelta();
    final boolean sorted = fbuf.isSorted();
    final boolean sab = fbuf.isSpaceAtBottom();
    assertEquals(fbuf.getItemFromIndex(100), 100.0f);
    assertEquals(fbuf.getItemFromIndex(hra ? 199 : 1), 1.0f);
    assertEquals(fbuf.isSpaceAtBottom(), hra);
    //uses the serialization method
    final MemorySegment wseg = MemorySegment.ofArray(fbuf.floatsToBytes());
    final float[] farr2 = new float[101];
    MemorySegment.copy(wseg, JAVA_FLOAT_UNALIGNED, 0, farr2, 0, 101);
    //uses the deserialization method
    final FloatBuffer fbuf2 = FloatBuffer.reconstruct(farr2, count, capacity, delta, sorted, sab);
    assertEquals(fbuf2.getCapacity(), capacity);
    assertEquals(fbuf2.getCount(), count);
    assertEquals(fbuf2.getDelta(), delta);
    assertEquals(fbuf2.isSorted(), sorted);
    assertEquals(fbuf2.getItemFromIndex(100), 100.0f);
    assertEquals(fbuf2.getItemFromIndex(hra ? 199 : 1), 1.0f);
    assertEquals(fbuf2.isSpaceAtBottom(), sab);
  }

  static void print(final Object o) { System.out.print(o.toString()); }

  static void println(final Object o) { System.out.println(o.toString()); }
}
