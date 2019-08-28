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

package org.apache.datasketches.cpc;

/**
 * Note: except for brief transitional moments, these sketches always obey the following strict
 * mapping between the flavor of a sketch and the number of coupons that it has collected.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
enum Flavor {
  EMPTY,   //    0  == C <    1
  SPARSE,  //    1  <= C <   3K/32
  HYBRID,  // 3K/32 <= C <   K/2
  PINNED,  //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
  SLIDING; // 27K/8 <= C

  private static Flavor[] fmtArr = Flavor.class.getEnumConstants();

  /**
   * Returns the Flavor given its enum ordinal
   * @param ordinal the given enum ordinal
   * @return the Flavor given its enum ordinal
   */
  static Flavor ordinalToFlavor(final int ordinal) {
    return fmtArr[ordinal];
  }

}
