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

package org.apache.datasketches;

import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.Family.stringToFamily;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class FamilyTest {

  @Test
  public void checkFamilyEnum() {
    final Family[] families = Family.values();
    final int numFam = families.length;

    for (int i = 0; i < numFam; i++) {
      final Family f = families[i];
      final int fid = f.getID();
      f.checkFamilyID(fid);

      final Family f2 = idToFamily(fid);
      assertTrue(f.equals(f2));
      assertEquals(f.getFamilyName(), f2.getFamilyName());
      final int id2 = f2.getID();
      assertEquals(fid, id2);
    }
    checkStringToFamily("Alpha");
    checkStringToFamily("QuickSelect");
    checkStringToFamily("Union");
    checkStringToFamily("Intersection");
    checkStringToFamily("AnotB");
    checkStringToFamily("HLL");
    checkStringToFamily("Quantiles");
  }

  private static void checkStringToFamily(final String inStr) {
    final String fName = stringToFamily(inStr).toString();
    assertEquals(fName, inStr.toUpperCase());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamilyName() {
    stringToFamily("Test");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamilyID() {
    final Family famAlpha = Family.ALPHA;
    final Family famQS = Family.QUICKSELECT;
    famAlpha.checkFamilyID(famQS.getID());
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
