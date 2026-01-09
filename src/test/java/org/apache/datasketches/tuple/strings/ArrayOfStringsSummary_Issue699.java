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

package org.apache.datasketches.tuple.strings;

import static org.apache.datasketches.common.Util.LS;

import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.TupleUnion;
import org.testng.annotations.Test;

public class ArrayOfStringsSummary_Issue699 {
  UpdatableThetaSketch thetaSk = UpdatableThetaSketch.builder().build();
  ArrayOfStringsTupleSketch tupleSk = new ArrayOfStringsTupleSketch();
  TupleUnion<ArrayOfStringsSummary> union = new TupleUnion<>(new ArrayOfStringsSummarySetOperations());

  @Test
  void go() {
    thetaSk.update("a");
    thetaSk.update("b");
    thetaSk.update("c");
    
    tupleSk.update("a", new String[] {"x", "y"});
    tupleSk.update("b", new String[] {"z"});
    tupleSk.update("e", new String[] {"x", "z"});
    
    println("Print Tuple Summary before union");
    printSummaries(tupleSk.iterator());
    
    union.union(tupleSk);
    union.union(thetaSk, new ArrayOfStringsSummary()); //enable this or the next
    //union.union(thetaSk, new ArrayOfStringsSummary(new String[] {"u", "v"})); //optional association
    
    println("Print Tuple Summary after union");
    printSummaries(union.getResult().iterator());
  }
  
  @Test
  void checkCopy() {
    ArrayOfStringsSummary aoss = new ArrayOfStringsSummary();
    aoss.copy(); //if null this will throw
  }
  
  @Test
  void checkToByteArray() {
    ArrayOfStringsSummary aoss = new ArrayOfStringsSummary();
    byte[] bytes = aoss.toByteArray();
    println("byte[].length = " + bytes.length);
  }
  
  
  static void printSummaries(TupleSketchIterator<ArrayOfStringsSummary> it) {
    while (it.next()) {
      String[] strArr = it.getSummary().getValue();
      if (strArr.length == 0) { print("-"); } //illustrates an empty string array
      for (String s : strArr) {
        print(s + ", ");
      }
      println("");
    }
    println("");
  }
  
  private static void println(Object o) { 
    print(o + LS);
  }
  
  private static void print(Object o) {
    //System.out.print(o.toString());
  }
}
