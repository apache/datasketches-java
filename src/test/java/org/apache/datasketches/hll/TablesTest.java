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

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.CouponMapping.xArr;
import static org.apache.datasketches.hll.CouponMapping.yArr;
import static org.apache.datasketches.hll.CubicInterpolation.usingXAndYTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import org.apache.datasketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 *
 */
@SuppressWarnings("javadoc")
public class TablesTest {

  @Test
  public void checkInterpolationExceptions() {
    try {
      usingXAndYTables(xArr, yArr, -1);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
    try {
      usingXAndYTables(xArr, yArr, 11000000.0);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkCornerCases() {
    int len = xArr.length;
    double x = xArr[len - 1];
    double y = usingXAndYTables(xArr, yArr, x);
    double yExp = yArr[len - 1];
    assertEquals(y, yExp, 0.0);
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
