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

package org.apache.datasketches.tuple.aninteger;

import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.round;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Union;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class EngagementTest {

  @Test
  public void computeEngagementHistogram() {
    int lgK = 12;
    int K = 1 << lgK; // = 4096
    int days = 30;
    IntegerSummary.Mode sumMode = IntegerSummary.Mode.Sum;
    int v = 0;
    int daysPerMonth = 30;
    IntegerSketch[] skArr = new IntegerSketch[30];
    for (int i = 0; i < 30; i++) {
      skArr[i] = new IntegerSketch(lgK, IntegerSummary.Mode.AlwaysOne);
    }
    for (int i = 0; i <= days; i++) { //31 generating indices
      int numIds = numIDs(daysPerMonth, i);
      int numDays = numDays(daysPerMonth, i);
      int myV = v++;
      for (int d = 0; d < numDays; d++) {
        for (int id = 0; id < numIds; id++) {
          skArr[d].update(myV + id, 1);
        }
      }
      v += numIds;
    }

    int numVisits = unionOps(K, sumMode, skArr);
    assertEquals(numVisits, 897);
  }

  @Test
  public void simpleCheckAlwaysOneIntegerSketch() {
    int lgK = 12;
    int K = 1 << lgK; // = 4096
    IntegerSummary.Mode a1Mode = IntegerSummary.Mode.AlwaysOne;

    IntegerSketch a1Sk1 = new IntegerSketch(lgK, a1Mode);
    IntegerSketch a1Sk2 = new IntegerSketch(lgK, a1Mode);

    int m = 2 * K;
    for (int key = 0; key < m; key++) {
      a1Sk1.update(key, 1);
      a1Sk2.update(key + (m/2), 1); //overlap by 1/2 = 1.5m = 12288.
    }
    int numVisits = unionOps(K, a1Mode, a1Sk1, a1Sk2);
    assertEquals(numVisits, K);
  }

  private static int unionOps(int K, IntegerSummary.Mode mode, IntegerSketch ... sketches) {
    IntegerSummarySetOperations setOps = new IntegerSummarySetOperations(mode, mode);
    Union<IntegerSummary> union = new Union<>(K, setOps);
    int len = sketches.length;

    for (IntegerSketch isk : sketches) {
      union.update(isk);
    }
    CompactSketch<IntegerSummary> result = union.getResult();
    SketchIterator<IntegerSummary> itr = result.iterator();

    int[] freqArr = new int[len +1];

    while (itr.next()) {
      int value = itr.getSummary().getValue();
      freqArr[value]++;
    }
    println("Engagement Histogram:");
    printf("%12s,%12s\n","Days Visited", "Visitors");
    int sumVisitors = 0;
    int sumVisits = 0;
    for (int i = 0; i < freqArr.length; i++) {
      int visits = freqArr[i];
      if (visits == 0) { continue; }
      sumVisitors += visits;
      sumVisits += (visits * i);
      printf("%12d,%12d\n", i, visits);
    }
    println("Total Visitors: " + sumVisitors);
    println("Total Visits  : " + sumVisits);
    return sumVisits;
  }

  @Test
  public void checkPwrLaw() {
    int dpm = 30;
    for (int i = 0; i <= dpm; i++) {
      int numIds = numIDs(dpm, i);
      int numDays = numDays(dpm, i);
      printf("%6d%6d%6d\n", i, numIds, numDays);
    }
  }

  private static int numIDs(int daysPerMonth, int index) {
    double d = daysPerMonth;
    double i = index;
    return (int)(round(exp((i * log(d)) / d)));
  }

  private static int numDays(int daysPerMonth, int index) {
    double d = daysPerMonth;
    double i = index;
    return (int)(round(exp(((d - i) * log(d)) / d)));

  }

  /**
   * @param o object to print
   */
  static void println(Object o) {
    //System.out.println(o.toString()); //Disable
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  static void printf(String fmt, Object ... args) {
    //System.out.printf(fmt, args); //Disable
  }
}
