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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class FloatBufferTest {

  @Test
  public void checkTrimLength() {
    FloatBuffer buf = new FloatBuffer(16, 4);
    for (int i = 0; i < 8; i++) { buf.append(i+1); }
    assertEquals(buf.getItemCount(), 8);
    buf.trimLength(4);
    assertEquals(buf.getItemCount(), 4);
  }

  @Test
  public void checkGetOdds() {
    int cap = 16;
    FloatBuffer buf = new FloatBuffer(cap, cap / 4);
    for (int i = 0; i < buf.getCapacity(); i++) {
      buf.append(i);
    }
    FloatBuffer out = buf.getOdds(0, cap);
    //println("");
    for (int i = 0; i < out.getLength(); i++) {
      int v = (int)out.getItem(i);
      assertTrue((v & 1) > 0);
      //print((int)out[i] + " ");
    }
  }

  @Test
  public void checkGetEvens() {
    int cap = 15;
    FloatBuffer buf = new FloatBuffer(cap, cap / 4);
    for (int i = 0; i < buf.getCapacity(); i++) {
      buf.append(i);
    }
    FloatBuffer out = buf.getEvens(0, buf.getCapacity());
    //println("");
    for (int i = 0; i < out.getLength(); i++) {
      int v = (int)out.getItem(i);
      assertTrue((v & 1) == 0);
      //print((int)out[i] + " ");
    }
  }

  @Test
  public void checkAppendAndSpace() {
    FloatBuffer buf = new FloatBuffer(2, 2);
    assertEquals(buf.getLength(), 0);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 2);
    buf.append(1);
    assertEquals(buf.getLength(), 1);
    assertEquals(buf.getItemCount(), 1);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 1);
    buf.append(2);
    assertEquals(buf.getLength(), 2);
    assertEquals(buf.getItemCount(), 2);
    assertEquals(buf.getCapacity(), 2);
    assertEquals(buf.getSpace(), 0);
    buf.append(3);
    assertEquals(buf.getLength(), 3);
    assertEquals(buf.getItemCount(), 3);
    assertEquals(buf.getCapacity(), 5);
    assertEquals(buf.getSpace(), 2);
  }

  @Test
  public void checkCountLessThan() {
    FloatBuffer buf = new FloatBuffer(14, 2);
    float[] sortedArr = {1,2,3,4,5,6,7};
    buf = FloatBuffer.wrap(sortedArr, true);
    FloatBuffer buf2 = new FloatBuffer(7,0);
    buf2.mergeSortIn(buf);
    assertEquals(buf2.getCountLtOrEq(4, false), 3);
    buf2.mergeSortIn(buf);
    assertEquals(buf2.getCountLtOrEq(4, false), 6);
    assertEquals(buf2.getLength(), 14);
    buf2.trimLength(12);
    assertEquals(buf2.getItemCount(), 12);
  }

  @Test
  public void checkMergeSortIn() {
    float[] arr1 = {1,2,5,6}; //both must be sorted
    float[] arr2 = {3,4,7,8};
    FloatBuffer buf1 = FloatBuffer.wrap(arr1, true);
    FloatBuffer buf2 = FloatBuffer.wrap(arr2, true);
    buf1.mergeSortIn(buf2);
    assertEquals(buf1.getSpace(), 0);
    assertEquals(buf1.getLength(), 8);
    assertEquals(buf1.getCapacity(), 8);
    int len = buf1.getItemCount();
    for (int i = 0; i < len; i++) {
      assertEquals((int)buf1.getItem(i), i+1);
    }
  }

  static void print(Object o) { System.out.print(o.toString()); }

  static void println(Object o) { System.out.println(o.toString()); }
}
