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

import static org.apache.datasketches.Util.getResourceBytes;

import org.apache.datasketches.memory.Memory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class ForwardCompatibilityTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.3.0.sk
  //Median2: 26.0
  public void check030_50() {
    int n = 50;
    String ver = "0.3.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.3.0.sk
  //Median2: 501.0
  public void check030_1000() {
    int n = 1000;
    String ver = "0.3.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.6.0.sk
  //Median2: 26.0
  public void check060_50() {
    int n = 50;
    String ver = "0.6.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.6.0.sk
  //Median2: 501.0
  public void check060_1000() {
    int n = 1000;
    String ver = "0.6.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.0.sk
  //Median2: 26.0
  public void check080_50() {
    int n = 50;
    String ver = "0.8.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.sk
  //Median2: 501.0
  public void check080_1000() {
    int n = 1000;
    String ver = "0.8.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.3.sk
  //Median2: 26.0
  public void check083_50() {
    int n = 50;
    String ver = "0.8.3";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.sk
  //Median2: 501.0
  public void check083_1000() {
    int n = 1000;
    String ver = "0.8.3";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  private static void getAndCheck(String ver, int n, double quantile) {
    DoublesSketch.rand.setSeed(131); //make deterministic
    //create fileName
    int k = 128;
    double nf = 0.5;
    String fileName = String.format("Qk%d_n%d_v%s.sk", k, n, ver);
    println("fullName: "+ fileName);
    println("Old Median: " + quantile);
    //Read File bytes
    byte[] byteArr2 = getResourceBytes(fileName);
    Memory srcMem = Memory.wrap(byteArr2);

    // heapify as update sketch
    DoublesSketch qs2 = UpdateDoublesSketch.heapify(srcMem);
    //Test the quantile
    double q2 = qs2.getQuantile(nf);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);

    // same thing with compact sketch
    qs2 = CompactDoublesSketch.heapify(srcMem);
    //Test the quantile
    q2 = qs2.getQuantile(nf);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  static void println(final Object o) {
    if (o == null) { print(LS); }
    else { print(o.toString() + LS); }
  }

  /**
   * @param o value to print
   */
  static void print(final Object o) {
    if (o != null) {
      //System.out.print(o.toString()); //disable here
    }
  }

}
