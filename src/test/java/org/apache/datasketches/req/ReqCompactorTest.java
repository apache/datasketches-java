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
}
