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

package org.apache.datasketches.tuple;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.SetOperationCornerCases.CornerCase;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class MiscTest {

  @Test
  public void checkUpdatableSketchBuilderReset() {
    final DoubleSummary.Mode mode = Mode.Sum;
    final UpdatableSketchBuilder<Double, DoubleSummary> bldr =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode));
    bldr.reset();
    final UpdatableSketch<Double,DoubleSummary> sk = bldr.build();
    assertTrue(sk.isEmpty());
  }

  @Test
  public void checkStringToByteArray() {
    Util.stringToByteArray("");
  }

  @Test
  public void checkDoubleToLongArray() {
    final long[] v = Util.doubleToLongArray(-0.0);
    assertEquals(v[0], 0);
  }

  //@Test
  public void checkById() {
    final int[] ids = {0,1,2, 5, 6 };
    final int len = ids.length;
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        final int id = ids[i] << 3 | ids[j];
        final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
        final String interResStr = cCase.getIntersectAction().getActionDescription();
        final String anotbResStr = cCase.getAnotbAction().getActionDescription();
        println(Integer.toOctalString(id) + "\t" + cCase + "\t" + cCase.getCaseDescription()
         + "\t" + interResStr + "\t" + anotbResStr);
      }
    }
  }

  @Test
  public void checkCopyCtor() {
    final DoubleSummary.Mode mode = Mode.Sum;
    final UpdatableSketchBuilder<Double, DoubleSummary> bldr =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode));
    bldr.reset();
    final UpdatableSketch<Double,DoubleSummary> sk = bldr.build();
    sk.update(1.0, 1.0);
    assertEquals(sk.getRetainedEntries(), 1);
    final  UpdatableSketch<Double,DoubleSummary> sk2 = sk.copy();
    assertEquals(sk2.getRetainedEntries(), 1);
  }


  /**
   *
   * @param o object to print
   */
  private static void println(final Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
