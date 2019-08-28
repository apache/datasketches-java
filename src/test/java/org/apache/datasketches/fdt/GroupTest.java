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

package org.apache.datasketches.fdt;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class GroupTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkToString() { //check visually
    Group gp = new Group();
    gp.init("AAAAAAAA,BBBBBBBBBB", 100_000_000, 1E8, 1.2E8, 8E7, 0.1, 0.01);
    assertEquals(gp.getPrimaryKey(), "AAAAAAAA,BBBBBBBBBB");
    assertEquals(gp.getCount(), 100_000_000);
    assertEquals(gp.getEstimate(), 1E8);
    assertEquals(gp.getUpperBound(), 1.2E8);
    assertEquals(gp.getLowerBound(), 8E7);
    assertEquals(gp.getFraction(), 0.1);
    assertEquals(gp.getRse(), 0.01);

    println(gp.getHeader());
    println(gp.toString());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    //System.out.print(s);  //disable here
  }

}
