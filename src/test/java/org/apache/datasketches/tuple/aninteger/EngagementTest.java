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
import static org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode.AlwaysOne;
import static org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode.Sum;

import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Union;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class EngagementTest {
  public static final int numStdDev = 2;

  @Test
  public void computeEngagementHistogram() {
    final int lgK = 8; //Using a larger sketch >= 9 will produce exact results for this little example
    final int K = 1 << lgK;
    final int days = 30;
    int v = 0;
    final IntegerSketch[] skArr = new IntegerSketch[days];
    for (int i = 0; i < days; i++) {
      skArr[i] = new IntegerSketch(lgK, AlwaysOne);
    }
    for (int i = 0; i <= days; i++) { //31 generating indices for symmetry
      final int numIds = numIDs(days, i);
      final int numDays = numDays(days, i);
      final int myV = v++;
      for (int d = 0; d < numDays; d++) {
        for (int id = 0; id < numIds; id++) {
          skArr[d].update(myV + id, 1);
        }
      }
      v += numIds;
    }
    unionOps(K, Sum, skArr);
  }

  private static int numIDs(final int totalDays, final int index) {
    final double d = totalDays;
    final double i = index;
    return (int)round(exp(i * log(d) / d));
  }

  private static int numDays(final int totalDays, final int index) {
    final double d = totalDays;
    final double i = index;
    return (int)round(exp((d - i) * log(d) / d));
  }

  private static void unionOps(final int K, final IntegerSummary.Mode mode, final IntegerSketch ... sketches) {
    final IntegerSummarySetOperations setOps = new IntegerSummarySetOperations(mode, mode);
    final Union<IntegerSummary> union = new Union<>(K, setOps);
    final int len = sketches.length;

    for (final IntegerSketch isk : sketches) {
      union.union(isk);
    }
    final CompactSketch<IntegerSummary> result = union.getResult();
    final SketchIterator<IntegerSummary> itr = result.iterator();

    final int[] numDaysArr = new int[len + 1]; //zero index is ignored

    while (itr.next()) {
      //For each unique visitor from the result sketch, get the # days visited
      final int numDaysVisited = itr.getSummary().getValue();
      //increment the number of visitors that visited numDays
      numDaysArr[numDaysVisited]++; //values range from 1 to 30
    }

    println("\nEngagement Histogram:");
    println("Number of Unique Visitors by Number of Days Visited");
    printf("%12s%12s%12s%12s\n","Days Visited", "Estimate", "LB", "UB");
    int sumVisits = 0;
    final double theta = result.getTheta();
    for (int i = 0; i < numDaysArr.length; i++) {
      final int visitorsAtDaysVisited = numDaysArr[i];
      if (visitorsAtDaysVisited == 0) { continue; }
      sumVisits += visitorsAtDaysVisited * i;

      final double estVisitorsAtDaysVisited = visitorsAtDaysVisited / theta;
      final double lbVisitorsAtDaysVisited = result.getLowerBound(numStdDev, visitorsAtDaysVisited);
      final double ubVisitorsAtDaysVisited = result.getUpperBound(numStdDev, visitorsAtDaysVisited);

      printf("%12d%12.0f%12.0f%12.0f\n",
          i, estVisitorsAtDaysVisited, lbVisitorsAtDaysVisited, ubVisitorsAtDaysVisited);
    }

    //The estimate and bounds of the total number of visitors comes directly from the sketch.
    final double visitors = result.getEstimate();
    final double lbVisitors = result.getLowerBound(numStdDev);
    final double ubVisitors = result.getUpperBound(numStdDev);
    printf("\n%12s%12s%12s%12s\n","Totals", "Estimate", "LB", "UB");
    printf("%12s%12.0f%12.0f%12.0f\n", "Visitors", visitors, lbVisitors, ubVisitors);

    //The total number of visits, however, is a scaled metric and takes advantage of the fact that
    //the retained entries in the sketch is a uniform random sample of all unique visitors, and
    //the the rest of the unique users will likely behave in the same way.
    final double estVisits = sumVisits / theta;
    final double lbVisits = estVisits * lbVisitors / visitors;
    final double ubVisits = estVisits * ubVisitors / visitors;
    printf("%12s%12.0f%12.0f%12.0f\n\n", "Visits", estVisits, lbVisits, ubVisits);
  }

  /**
   * @param o object to print
   */
  private static void println(final Object o) {
    printf("%s\n", o.toString());
  }

  /**
   * @param fmt format
   * @param args arguments
   */
  private static void printf(final String fmt, final Object ... args) {
    //System.out.printf(fmt, args); //Enable/Disable printing here
  }
}
