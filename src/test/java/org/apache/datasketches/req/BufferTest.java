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

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class BufferTest {

  @Test
  public void checkTrimLength() {
    Buffer buf = new Buffer(16, 4);
    for (int i = 0; i < 8; i++) { buf.append(i+1); }
    assertEquals(buf.length(), 8);
    buf.trimLength(4);
    assertEquals(buf.length(), 4);
  }

  @Test
  public void checkGetOdds() {
    int cap = 16;
    Buffer buf = new Buffer(cap, cap / 4);
    for (int i = 0; i < buf.capacity(); i++) {
      buf.append(i);
    }
    float[] out = buf.getOdds(0, cap);
    println("");
    for (int i = 0; i < out.length; i++) {
      print((int)out[i] + " ");
    }
  }

  @Test
  public void checkGetEvens() {
    int cap = 15;
    Buffer buf = new Buffer(cap, cap / 4);
    for (int i = 0; i < buf.capacity(); i++) {
      buf.append(i);
    }
    float[] out = buf.getEvens(0, buf.capacity());
    println("");
    for (int i = 0; i < out.length; i++) {
      print((int)out[i] + " ");
    }
  }

  @Test
  public void checkAppend() {
    Buffer buf = new Buffer(2, 2);
    buf.append(1);
    assertEquals(buf.length(), 1);
    buf.append(2);
    assertEquals(buf.length(), 2);
    buf.append(3);
    assertEquals(buf.capacity(), 4);
  }

  @Test
  public void checkCountLessThan() {
    Buffer buf = new Buffer(16, 2);
    buf.extend(new float[] {1,2,3,4,5,6,7,1});
    buf.setSorted(true);
    assertEquals(buf.countLessThan(4), 3);
    buf.setSorted(false);
    assertEquals(buf.countLessThan(4), 4);
    buf.clear(4, 7);
    assertEquals(buf.getItem(4), 0.0F);
    assertEquals(buf.getItem(5), 0.0F);
    assertEquals(buf.getItem(6), 0.0F);
  }

  @Test
  public void checkExtendArray() {
    Buffer buf = new Buffer(0, 2);
    float[] arr1 = {1,2};
    float[] arr2 = {3,4};
    buf.extend(arr1);
    buf.extend(arr2);
    for (int i = 0; i < buf.length(); i++) {
      println(buf.getItem(i));
    }
  }

  @Test
  public void checkExtendWithBuffer() {
    Buffer buf = new Buffer(0, 2);
    float[] arr1 = {1,2};
    buf.extend(arr1);
    Buffer buf2 = new Buffer(0, 2);
    float[] arr2 = {3,4};
    buf2.extend(arr2);
    buf.extend(buf2);
    float[] arr3 = buf.getArray();
    assertEquals(arr3.length, 4);

    float[] arr4 = buf.getOdds(0, 4);
    for (int i = 0; i < arr4.length; i++) {
      println(arr4[i]);
    }
    arr4 = buf.getEvens(0, 4);
    for (int i = 0; i < arr4.length; i++) {
      println(arr4[i]);
    }
    assertEquals(buf.isSorted(), false);
    assertEquals(buf.sort().isSorted(), true);
  }

  @Test
  public void checkMergeSortIn() {
    Buffer buf = new Buffer(4,0);
    float[] arr1 = {1,2,5,6};
    float[] arr2 = {3,4,4,7};
    buf.extend(arr1);
    buf.sort();
    buf.mergeSortIn(arr2);
    int len = buf.length();
    for (int i = 0; i < len; i++) { print(buf.getItem(i) + ", "); }
    println("");
  }

  static void print(Object o) { System.out.print(o.toString()); }

  static void println(Object o) { System.out.println(o.toString()); }
}
