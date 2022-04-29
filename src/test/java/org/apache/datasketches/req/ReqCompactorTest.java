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

import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.req.ReqSerDe.Compactor;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ReqCompactorTest {
  final ReqSketchTest reqSketchTest = new ReqSketchTest();

  @Test
  public void checkNearestEven() {
    assertEquals(ReqCompactor.nearestEven(-0.9f), 0);
  }

  @Test
  public void checkGetters() {
    final boolean up = true;
    final boolean hra = true;
    final boolean ltEq = true;
    final ReqSketch sk = reqSketchTest.loadSketch( 20,   1, 120,  up,  hra,  ltEq, 0);
    final ReqCompactor c = sk.getCompactors().get(0);

    c.getCoin();
    final long state = c.getState();
    assertEquals(state, 1L);
    assertEquals(c.getNumSections(), 3);
    assertEquals(c.getSectionSize(), 20);
  }

  @Test
  public void checkSerDe() {
    checkSerDeImpl(12, false);
    checkSerDeImpl(12, true);
  }

  private static void checkSerDeImpl(final int k, final boolean hra) {
    final ReqCompactor c1 = new ReqCompactor((byte)0, hra, k, null);
    final int nomCap = 2 * 3 * k;
    final int cap = 2 * nomCap;
    final int delta = nomCap;
    final FloatBuffer buf = c1.getBuffer();

    for (int i = 1; i <= nomCap; i++) {
      buf.append(i); //compactor doesn't have a direct update() method
    }
    final float minV = 1;
    final float maxV = nomCap;
    final float sectionSizeFlt = c1.getSectionSizeFlt();
    final int sectionSize = c1.getSectionSize();
    final int numSections = c1.getNumSections();
    final long state = c1.getState();
    final int lgWeight = c1.getLgWeight();
    final boolean c1hra = c1.isHighRankAccuracy();
    final boolean sorted = buf.isSorted();
    final byte[] c1ser = c1.toByteArray();
    //now deserialize
    final Buffer buff = Memory.wrap(c1ser).asBuffer();
    final Compactor compactor = ReqSerDe.extractCompactor(buff, sorted, c1hra);
    final ReqCompactor c2 = compactor.reqCompactor;
    assertEquals(compactor.minVal, minV);
    assertEquals(compactor.maxVal, maxV);
    assertEquals(compactor.count, nomCap);
    assertEquals(c2.getSectionSizeFlt(), sectionSizeFlt);
    assertEquals(c2.getSectionSize(), sectionSize);
    assertEquals(c2.getNumSections(), numSections);
    assertEquals(c2.getState(), state);
    assertEquals(c2.getLgWeight(), lgWeight);
    assertEquals(c2.isHighRankAccuracy(), c1hra);
    final FloatBuffer buf2 = c2.getBuffer();
    assertEquals(buf2.getCapacity(), cap);
    assertEquals(buf2.getDelta(), delta);
    assertTrue(buf.isEqualTo(buf2));
  }
}
