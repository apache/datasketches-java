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

/**
 * These criteria are used by all the quantiles algorithms in the library.
 * <ul>
 * <li><b>Definition of Non Inclusive <i>getRank(q)</i> search:</b><br>
 * Given quantile <i>q</i>, return the rank, <i>r</i>, of the largest quantile that is strictly less than <i>q</i>.
 * </li>
 *
 * <li><b>Definition of Inclusive <i>getRank(q)</i> search:</b><br>
 * Given quantile <i>q</i>, return the rank, <i>r</i>, of the largest quantile that is less than or equal to <i>q</i>.
 * </li>
 *
 * <li><b>Definition of Non Inclusive Search <i>getQuantile(r)</i> search:</b><br>
 * Given rank <i>r</i>, return the quantile of the smallest rank that is strictly greater than <i>r</i>.
 * </li>
 *
 * <li><b>Definition of Inclusive Search <i>getQuantile(r)</i> search:</b><br>
 * Given rank <i>r</i>, return the quantile of the smallest rank that is strictly greater than or equal to <i>r</i>.
 * </li>
 * </ul>

 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks Tutorial</a>
 *
 * @author Lee Rhodes
 */
public enum QuantileSearchCriteria {

  /**
   * See definition of Non Inclusive above.
   *
   * <p>For a non inclusive, strict getQuantile(rank) type search, If the given rank is equal to 1.0,
   * there is no quantile that satisfies this criterion,
   * the getQuantile(rank) method will return a NaN.</p>
   */
  NON_INCLUSIVE_STRICT,

  /**
   * See definition of Non Inclusive above.
   *
   * <p>For a non inclusive, getQuantile(rank) type search, If the given rank is is equal to 1.0,
   * there is no quantile that satisfies this criterion, however,
   * the method will return the largest quantile value retained by the sketch as a convenience.</p>
   *
   * <p>This is not strictly mathematically correct, but very convenient as it most often what we expect.</p>
   */
  NON_INCLUSIVE,

  /**
   * See definition of Inclusive above.
   */
  INCLUSIVE
}

