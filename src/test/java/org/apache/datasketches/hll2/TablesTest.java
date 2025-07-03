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

package org.apache.datasketches.hll2;

import static org.apache.datasketches.hll2.CouponMapping.xArr;
import static org.apache.datasketches.hll2.CouponMapping.yArr;
import static org.apache.datasketches.hll2.CubicInterpolation.usingXAndYTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 *
 */
public class TablesTest {

  @Test
  public void checkInterpolationExceptions() {
    try {
      usingXAndYTables(xArr, yArr, -1);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
    try {
      usingXAndYTables(xArr, yArr, 11000000.0);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkCornerCases() {
    final int len = xArr.length;
    final double x = xArr[len - 1];
    final double y = usingXAndYTables(xArr, yArr, x);
    final double yExp = yArr[len - 1];
    assertEquals(y, yExp, 0.0);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
