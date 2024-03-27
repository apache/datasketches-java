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

package org.apache.datasketches.quantilescommon;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Random;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.testng.annotations.Test;

public class KolmogorovSmirnovTest {
  private static final String LS = System.getProperty("line.separator");

 @Test
 public void checkDisjointDistributionClassicDoubles() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + 500);
     s2.update(x);
   }
   final double eps = DoublesSketch.getNormalizedRankError(k, false);
   println("Disjoint Classic Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps) + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 1.0, 2 * eps);
 }

 @Test
 public void checkDisjointDistributionKllDoubles() {
   final int k = 256;
   final KllDoublesSketch s1 = KllDoublesSketch.newHeapInstance(k);
   final KllDoublesSketch s2 = KllDoublesSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + 500);
     s2.update(x);
   }
   final double eps = KllSketch.getNormalizedRankError(k, false);
   println("Disjoint KLL Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps));
   println("");
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 1.0, 2 * eps);
 }

 @Test
 public void checkDisjointDistributionKllFloats() {
   final int k = 256;
   final KllFloatsSketch s1 = KllFloatsSketch.newHeapInstance(k);
   final KllFloatsSketch s2 = KllFloatsSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final float x = (float)rand.nextGaussian();
     s1.update(x + 500);
     s2.update(x);
   }
   final double eps = KllSketch.getNormalizedRankError(k, false);
   println("Disjoint KLL Floats");
   println("D      = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps) + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 1.0, 2 * eps);
 }

 @Test
 public void checkIdenticalDistributionClassicDoubles() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
   }
   println("Identical Classic Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s1));
   println("2*eps = 0.0" + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s1), 0.0, 0.0);
 }

 @Test
 public void checkIdenticalDistributionKllDoubles() {
   final int k = 256;
   final KllDoublesSketch s1 = KllDoublesSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
   }
   println("Identical KLL Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s1));
   println("2*eps = 0.0" + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s1), 0.0, 0.0);
 }

 @Test
 public void checkIdenticalDistributionKllFloats() {
   final int k = 256;
   final KllFloatsSketch s1 = KllFloatsSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final float x = (float)rand.nextGaussian();
     s1.update(x);
   }
   println("Identical KLL Floats");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s1));
   println("2*eps = 0.0" + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s1), 0.0, 0.0);
 }

 @Test
 public void checkSameDistributionDifferentClassicDoublesSketches() {
   final int k = 256;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
     s2.update(x);
   }
   final double eps = DoublesSketch.getNormalizedRankError(k, false);
   println("Same Classic Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps) + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 0, 2 * eps);
 }

 @Test
 public void checkSameDistributionDifferentKllDoublesSketches() {
   final int k = 256;
   final KllDoublesSketch s1 = KllDoublesSketch.newHeapInstance(k);
   final KllDoublesSketch s2 = KllDoublesSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x);
     s2.update(x);
   }
   final double eps = KllSketch.getNormalizedRankError(k, false);
   println("Same KLL Doubles");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps) + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 0, 2 * eps);
 }

 @Test
 public void checkSameDistributionDifferentKllFloatsSketches() {
   final int k = 256;
   final KllFloatsSketch s1 = KllFloatsSketch.newHeapInstance(k);
   final KllFloatsSketch s2 = KllFloatsSketch.newHeapInstance(k);

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final float x = (float)rand.nextGaussian();
     s1.update(x);
     s2.update(x);
   }
   final double eps = KllSketch.getNormalizedRankError(k, false);
   println("Same KLL Floats");
   println("D     = " + KolmogorovSmirnov.computeKSDelta(s1, s2));
   println("2*eps = " + (2 * eps) + LS);
   assertEquals(KolmogorovSmirnov.computeKSDelta(s1, s2), 0, 2 * eps);
 }

 @Test
 public void mediumResolutionClassicDoubles() {
   final int k = 2048;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("MedRes Classic Doubles");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertFalse(reject);
 }

 @Test
 public void mediumResolutionKllDoubles() {
   final int k = 2048;
   final KllDoublesSketch s1 = KllDoublesSketch.newHeapInstance(k);
   final KllDoublesSketch s2 = KllDoublesSketch.newHeapInstance(k);
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("MedRes KLL Doubles");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertFalse(reject);
 }

 @Test
 public void mediumResolutionKllFloats() {
   final int k = 2048;
   final KllFloatsSketch s1 = KllFloatsSketch.newHeapInstance(k);
   final KllFloatsSketch s2 = KllFloatsSketch.newHeapInstance(k);
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final float x = (float)rand.nextGaussian();
     s1.update(x + .05F);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("MedRes KLL Floats");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertFalse(reject);
 }

 @Test
 public void highResolutionClassicDoubles() {
   final int k = 8192;
   final UpdateDoublesSketch s1 = DoublesSketch.builder().setK(k).build();
   final UpdateDoublesSketch s2 = DoublesSketch.builder().setK(k).build();
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("HiRes Classic Doubles");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertTrue(reject);
 }

 @Test
 public void highResolutionKllDoubles() {
   final int k = 8192;
   final KllDoublesSketch s1 = KllDoublesSketch.newHeapInstance(k);
   final KllDoublesSketch s2 = KllDoublesSketch.newHeapInstance(k);
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final double x = rand.nextGaussian();
     s1.update(x + .05);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("HiRes KLL Doubles");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertTrue(reject);
 }

 @Test
 public void highResolutionKllFloats() {
   final int k = 8192;
   final KllFloatsSketch s1 = KllFloatsSketch.newHeapInstance(k);
   final KllFloatsSketch s2 = KllFloatsSketch.newHeapInstance(k);
   final double tgtPvalue = .05;

   final Random rand = new Random(1);

   final int n =  (3 * k) - 1;
   for (int i = 0; i < n; ++i) {
     final float x = (float)rand.nextGaussian();
     s1.update(x + .05F);
     s2.update(x);
   }

   double D = KolmogorovSmirnov.computeKSDelta(s1, s2);
   double thresh = KolmogorovSmirnov.computeKSThreshold(s1, s2, tgtPvalue);
   final boolean reject = KolmogorovSmirnov.kolmogorovSmirnovTest(s1, s2, tgtPvalue);
   println("HiRes KLL Floats");
   println("pVal = " + tgtPvalue + "\nK = " + k + "\nD = " + D + "\nTh = " + thresh
       + "\nNull Hypoth Rejected = " + reject + LS);
   assertTrue(reject);
 }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
