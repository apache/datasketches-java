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
    assertEquals(buf.getItemCount(), 8);
    buf.trimLength(4);
    assertEquals(buf.getItemCount(), 4);
  }

  @Test
  public void checkGetOdds() {
    int cap = 16;
    Buffer buf = new Buffer(cap, cap / 4);
    for (int i = 0; i < buf.getCapacity(); i++) {
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
    for (int i = 0; i < buf.getCapacity(); i++) {
      buf.append(i);
    }
    float[] out = buf.getEvens(0, buf.getCapacity());
    println("");
    for (int i = 0; i < out.length; i++) {
      print((int)out[i] + " ");
    }
  }

  @Test
  public void checkAppend() {
    Buffer buf = new Buffer(2, 2);
    buf.append(1);
    assertEquals(buf.getItemCount(), 1);
    buf.append(2);
    assertEquals(buf.getItemCount(), 2);
    buf.append(3);
    assertEquals(buf.getCapacity(), 4);
  }

  @Test
  public void checkCountLessThan() {
    Buffer buf = new Buffer(16, 2);
    float[] unsortedArr = {1,7,3,6,5,2,4};
    buf.extend(unsortedArr); //unsorted flag
    assertEquals(buf.countLessThan(4), 3);
    buf = new Buffer(16, 2);
    float[] sortedArr = {1,2,3,4,5,6,7};
    buf.mergeSortIn(sortedArr);
    assertEquals(buf.countLessThan(4), 3);
    buf.mergeSortIn(sortedArr);
    assertEquals(buf.countLessThan(4), 6);
    buf.trimLength(12);
    assertEquals(buf.getItemCount(), 12);
    assertEquals(buf.getItem(12), 0.0F);
    assertEquals(buf.getItem(13), 0.0F);
  }

  @Test
  public void checkExtendArray() {
    Buffer buf = new Buffer(0, 2);
    float[] arr1 = {1,2};
    float[] arr2 = {3,4};
    buf.extend(arr1);
    buf.extend(arr2);
    for (int i = 0; i < buf.getItemCount(); i++) {
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
    int len = buf.getItemCount();
    for (int i = 0; i < len; i++) { print(buf.getItem(i) + ", "); }
    println("");
  }

  static void print(Object o) { System.out.print(o.toString()); }

  static void println(Object o) { System.out.println(o.toString()); }
}
