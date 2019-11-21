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

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class MiscTests {
  long v = 0;

  //@Test
  public void checkSrcGTTgt() {
    int unionLgK = 5;
    int delLgK = 1;
    int srcLgK = unionLgK + delLgK;
    TgtHllType srcType = TgtHllType.HLL_8;

    Union union = buildUnion(unionLgK, 1 << unionLgK);
    HllSketch sk = build(srcLgK, srcType, 1 << srcLgK);
    union.update(sk);

  }




  private Union buildUnion(int lgMaxK, int n) {
    Union u = new Union(lgMaxK);
    for (int i = 0; i < n; i++) { u.update(i + v); }
    v += n;
    return u;
  }

  private HllSketch build(int lgK, TgtHllType tgtHllType, int n) {
    HllSketch sk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) { sk.update(i + v); }
    v += n;
    return sk;
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
