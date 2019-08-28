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

package org.apache.datasketches.hllmap;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class CouponTraverseMapTest {

  @Test
  public void getEstimateNoEntry() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    Assert.assertEquals(map.getEstimate(key), 0.0);
    Assert.assertEquals(map.getUpperBound(key), 0.0);
    Assert.assertEquals(map.getLowerBound(key), 0.0);
  }

  @Test
  public void oneKeyOneEntry() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    byte[] key = new byte[] {0, 0, 0 ,0};
    double estimate = map.update(key, (short) 1);
    Assert.assertEquals(estimate, 1.0);
    Assert.assertEquals(map.getEstimate(key), 1.0);
    Assert.assertTrue(map.getUpperBound(key) >= 1.0);
    Assert.assertTrue(map.getLowerBound(key) <= 1.0);
  }

  @Test
  public void delete() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    double estimate = map.update("1234".getBytes(UTF_8), (short) 1);
    Assert.assertEquals(estimate, 1.0);
    int index1 = map.findKey("1234".getBytes(UTF_8));
    Assert.assertTrue(index1 >= 0);
    map.deleteKey(index1);
    int index2 = map.findKey("1234".getBytes(UTF_8));
    // should be complement of the same index as before
    Assert.assertEquals(~index2, index1);
    Assert.assertEquals(map.getEstimate("1".getBytes(UTF_8)), 0.0);
  }

  @Test
  public void growAndShrink() {
    CouponTraverseMap map = CouponTraverseMap.getInstance(4, 1);
    long sizeBytes1 = map.getMemoryUsageBytes();
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      map.update(key, (short) 1);
    }
    long sizeBytes2 = map.getMemoryUsageBytes();
    Assert.assertTrue(sizeBytes2 > sizeBytes1);
    for (int i = 0; i < 1000; i ++) {
      byte[] key = String.format("%4s", i).getBytes(UTF_8);
      int index = map.findKey(key);
      Assert.assertTrue(index >= 0);
      map.deleteKey(index);
    }
    long sizeBytes3 = map.getMemoryUsageBytes();
    Assert.assertTrue(sizeBytes3 < sizeBytes2);
    println(map.toString());
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
