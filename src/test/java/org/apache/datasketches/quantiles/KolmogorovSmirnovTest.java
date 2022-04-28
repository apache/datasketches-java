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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Random;

import org.testng.annotations.Test;

public class KolmogorovSmirnovTest {

 @Test
 public void checkKomologorovSmirnovStatistic1() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + 100);
     s2.update(x);
   }

   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 1.0, 1E-6);
   println("D = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
 }

 @Test
 public void checkKomologorovSmirnovStatistic2() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
     s2.update(x);
   }

   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 0, .01);
   println("D = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
 }

 @Test
 public void checkKomologorovSmirnovStatistic3() {
   final int k = 2048;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject);
   assertFalse(reject);
 }

 @Test
 public void checkKomologorovSmirnovStatistic4() {
   final int k = 8192;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random();

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject);
   assertTrue(reject);
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
