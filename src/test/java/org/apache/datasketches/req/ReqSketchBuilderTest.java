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

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ReqSketchBuilderTest {

  @Test
  public void checkBldr() {
    ReqSketchBuilder bldr = new ReqSketchBuilder();
    ReqDebugImpl rdi = new ReqDebugImpl(2);
    bldr.setK(50).setHighRankAccuracy(true).setLessThanOrEqual(false)
      .setCompatible(true).setReqDebug(rdi);
    assertEquals(bldr.getK(), 50);
    assertEquals(bldr.getHighRankAccuracy(), true);
    assertEquals(bldr.getLessThanOrEqual(), false);
    assertEquals(bldr.getCompatible(), true);
    assertTrue(bldr.getReqDebug() != null);
    println(bldr.toString());
    bldr.setReqDebug(null);
    println(bldr.toString());

  }

  @SuppressWarnings("unused")
  static void println(Object o) {
    //System.out.println(o.toString());
  }
}
