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

package org.apache.datasketches.req;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.Criteria;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqCompactorTest {
  final ReqSketchTest reqSketchTest = new ReqSketchTest();

  @Test
  public void checkNearestEven() {
    assertEquals(ReqCompactor.nearestEven(-0.9), 0);
  }

  @Test
  public void checkGetters() {
    boolean up = true;
    boolean hra = true;
    Criteria criterion = Criteria.LE;
    ReqSketch sk = reqSketchTest.loadSketch( 20,   1, 120,  up,  hra,  criterion, 0);
    ReqCompactor c = sk.getCompactors().get(0);

    c.getCoin();
    int numCompacts = c.getNumCompactions();
    assertEquals(numCompacts, 1);
    assertEquals(c.getNumSections(), 3);
    assertEquals(c.getSectionSize(), 20);
    assertEquals(c.getState(), numCompacts);
  }

  @Test
  public void checkSerDe() {
    checkSerDeImpl(12, false);
    checkSerDeImpl(12, true);
  }

  private static void checkSerDeImpl(int k, boolean hra) {
    ReqCompactor c1 = new ReqCompactor((byte)0, hra, k, null);
    FloatBuffer buf = c1.getBuffer();
    int exact = 2 * 3 * k;
    for (int i = 1; i <= exact; i++) {
      buf.append(i);
    }
    double sectionSizeDbl = c1.getSectionSizeDbl();
    int sectionSize = c1.getSectionSize();
    int numSections = c1.getNumSections();
    int numCompactions = c1.getNumCompactions();
    int state = c1.getState();
    int lgWeight = c1.getLgWeight();
    boolean coin = c1.getCoin();
    boolean c1hra = c1.isHighRankAccuracy();
    FloatBuffer buf1 = c1.getBuffer();

    byte[] c1ser = c1.toByteArray();
    Buffer buff = Memory.wrap(c1ser).asBuffer();
    ReqCompactor c2 = ReqCompactor.heapify(buff);

    assertEquals(c2.getSectionSizeDbl(), sectionSizeDbl);
    assertEquals(c2.getSectionSize(), sectionSize);
    assertEquals(c2.getNumSections(), numSections);
    assertEquals(c2.getNumCompactions(), numCompactions);
    assertEquals(c2.getState(), state);
    assertEquals(c2.getLgWeight(), lgWeight);
    assertEquals(c2.getCoin(), coin);
    assertEquals(c2.isHighRankAccuracy(), c1hra);
    FloatBuffer buf2 = c2.getBuffer();
    assertTrue(buf1.isEqualTo(buf2));
  }
}
