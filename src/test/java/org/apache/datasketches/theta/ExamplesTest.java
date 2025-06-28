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

package org.apache.datasketches.theta;

import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ExamplesTest {

  @Test
  public void simpleCountingSketch() {
    final int k = 4096;
    final int u = 1000000;

    final UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i = 0; i < u; i++) {
      sketch.update(i);
    }

    println(sketch.toString());
  }
  /*
### HeapQuickSelectSketch SUMMARY:
   Nominal Entries (k)     : 4096
   Estimate                : 1002714.745231455
   Upper Bound, 95% conf   : 1027777.3354974985
   Lower Bound, 95% conf   : 978261.4472857157
   p                       : 1.0
   Theta (double)          : 0.00654223948655085
   Theta (long)            : 60341508738660257
   Theta (long, hex        : 00d66048519437a1
   EstMode?                : true
   Empty?                  : false
   Resize Factor           : 8
   Array Size Entries      : 8192
   Retained Entries        : 6560
   Update Seed             : 9001
   Seed Hash               : ffff93cc
### END SKETCH SUMMARY
  */

  @Test
  public void theta2dot0Examples() {
    //Load source sketches
    final UpdateSketchBuilder bldr = UpdateSketch.builder();
    final UpdateSketch skA = bldr.build();
    final UpdateSketch skB = bldr.build();
    for (int i = 1; i <= 1000; i++) {
      skA.update(i);
      skB.update(i + 250);
    }

    //Union Stateless:
    Union union = SetOperation.builder().buildUnion();
    CompactSketch csk = union.union(skA, skB);
    assert csk.getEstimate() == 1250;

    //Union Stateful:
    union = SetOperation.builder().buildUnion();
    union.union(skA); //first call
    union.union(skB); //2nd through nth calls
    //...
    csk = union.getResult();
    assert csk.getEstimate() == 1250;

    //Intersection Stateless:
    Intersection inter = SetOperation.builder().buildIntersection();
    csk = inter.intersect(skA, skB);
    assert csk.getEstimate() == 750;

    //Intersection Stateful:
    inter = SetOperation.builder().buildIntersection();
    inter.intersect(skA); //first call
    inter.intersect(skB); //2nd through nth calls
    //...
    csk = inter.getResult();
    assert csk.getEstimate() == 750;

    //AnotB Stateless:
    AnotB diff = SetOperation.builder().buildANotB();
    csk = diff.aNotB(skA, skB);
    assert csk.getEstimate() == 250;

    //AnotB Stateful:
    diff = SetOperation.builder().buildANotB();
    diff.setA(skA); //first call
    diff.notB(skB); //2nd through nth calls
    //...
    csk = diff.getResult(true);
    assert csk.getEstimate() == 250;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //enable/disable here
  }

}
