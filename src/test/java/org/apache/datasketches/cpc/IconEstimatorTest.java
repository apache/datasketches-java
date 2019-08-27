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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.cpc.IconEstimator.getIconEstimate;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class IconEstimatorTest {

  //SLOW EXACT VERSION, Eventually move to characterization

  /**
   * Important note: do not change anything in the following function.
   * It has been carefully designed and tested for numerical accuracy.
   * In particular, the use of log1p and expm1 is critically important.
   * @param kf the value of k as a double
   * @param nf the value of n as a double
   * @param col the given column
   * @return the quantity qnj
   */
  static double qnj(final double kf, final double nf, final int col) {
    final double tmp1 = -1.0 / (kf * (Math.pow(2.0, col)));
    final double tmp2 = Math.log1p(tmp1);
    return (-1.0 * (Math.expm1(nf * tmp2)));
  }

  static double exactCofN(final double kf, final double nf) {
    double total = 0.0;
    for (int col = 128; col >= 1; col--) {
      total += qnj(kf, nf, col);
    }
    return kf * total;
  }

  static final double iconInversionTolerance = 1.0e-15;

  static double exactIconEstimatorBinarySearch(final double kf, final double targetC,
      final double nLoIn, final double nHiIn) {
    int depth = 0;
    double nLo = nLoIn;
    double nHi = nHiIn;

    while (true) { // manual tail recursion optimization
      if (depth > 100) {
        throw new SketchesStateException("Excessive recursion in binary search\n");
      }
      assert nHi > nLo;
      final double nMid = nLo + (0.5 * (nHi - nLo));
      assert ((nMid > nLo) && (nMid < nHi));
      if (((nHi - nLo) / nMid) < iconInversionTolerance) {
        return (nMid);
      }
      final double midC = exactCofN(kf, nMid);
      if (midC == targetC) {
        return nMid;
      }
      if (midC < targetC) {
        nLo = nMid;
        depth++;
        continue;
      }
      if (midC  > targetC) {
        nHi = nMid;
        depth++;
        continue;
      }
      throw new SketchesStateException("bad value in binary search\n");
    }
  }

  static double exactIconEstimatorBracketHi(final double kf, final double targetC, final double nLo) {
    int depth = 0;
    double curN = 2.0 * nLo;
    double curC = exactCofN(kf, curN);
    while (curC <= targetC) {
      if (depth > 100) {
        throw new SketchesStateException("Excessive looping in exactIconEstimatorBracketHi\n");
      }
      depth++;
      curN *= 2.0;
      curC = exactCofN(kf, curN);
    }
    assert (curC > targetC);
    return (curN);
  }

  static double exactIconEstimator(final int lgK, final long c) {
    final double targetC = c;
    if ((c == 0L) || (c == 1L)) {
      return targetC;
    }
    final double kf = (1L << lgK);
    final double nLo = targetC;
    assert exactCofN(kf, nLo) < targetC; // bracket lo
    final double nHi = exactIconEstimatorBracketHi(kf, targetC, nLo); // bracket hi
    return exactIconEstimatorBinarySearch(kf, targetC, nLo, nHi);
  }

  //@Test //used for characterization
  public static void testIconEstimator() {
    final int lgK = 12;
    final int k = 1 << lgK;
    long c = 1;
    while (c < (k * 64)) { // was K * 15
      final double exact  = exactIconEstimator(lgK, c);
      final double approx = getIconEstimate(lgK, c);
      final double relDiff = (approx - exact) / exact;
      printf("%d\t%.19g\t%.19g\t%.19g\n", c, relDiff, exact, approx);
      final long a = c + 1;
      final long b = (1001 * c) / 1000;
      c = ((a > b) ? a : b);
    }
  }

  @Test //used for unit test
  public static void quickIconEstimatorTest() {
    for (int lgK = 4; lgK <= 26; lgK++) {
      final long k = 1L << lgK;
      long[] cArr = {2, 5 * k, 6 * k, 60L * k};
      assertEquals(getIconEstimate(lgK, 0L), 0.0, 0.0);
      assertEquals(getIconEstimate(lgK, 1L), 1.0, 0.0);
      for (long c : cArr) {
        final double exact  = exactIconEstimator(lgK, c);
        final double approx = getIconEstimate(lgK, c);
        final double relDiff = Math.abs((approx - exact) / exact);
        printf("%d\t %d\t %.19g\t %.19g\t %.19g\n", lgK, c, relDiff, exact, approx);
        assertTrue(relDiff < Math.max(2E-6, 1.0/(80 * k)));
      }
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * Print with format
   * @param format the given format
   * @param args the args
   */
  static void printf(String format, Object ... args) {
    String out = String.format(format, args);
    print(out);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + "\n");
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s); //disable here
  }
}
