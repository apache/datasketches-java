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
 * These search criteria are used by the KLL, REQ and Classic Quantiles sketches in the DataSketches library.
 *
 * @see <a href="https://datasketches.apache.org/docs/Quantiles/SketchingQuantilesAndRanksTutorial.html">
 * Sketching Quantiles and Ranks Tutorial</a>
 *
 * @author Lee Rhodes
 */
public enum QuantileSearchCriteria {

  /**
   * <b>Definition of INCLUSIVE <i>getQuantile(r)</i> search:</b><br>
   * Given rank <i>r</i>, return the quantile of the smallest rank that is
   * strictly greater than or equal to <i>r</i>.
   *
   * <p><b>Definition of INCLUSIVE <i>getRank(q)</i> search:</b><br>
   * Given quantile <i>q</i>, return the rank, <i>r</i>, of the largest quantile that is
   * less than or equal to <i>q</i>.</p>
   */
  INCLUSIVE,

  /**
   * <b>Definition of EXCLUSIVE <i>getQuantile(r)</i> search:</b><br>
   * Given rank <i>r</i>, return the quantile of the smallest rank that is
   * strictly greater than <i>r</i>.
   *
   * <p>However, if the given rank is is equal to 1.0, or there is no quantile that satisfies this criterion
   * the method will return a <i>NaN</i> or <i>null</i>.</p>
   *
   * <p><b>Definition of EXCLUSIVE <i>getRank(q)</i> search:</b><br>
   * Given quantile <i>q</i>, return the rank, <i>r</i>, of the largest quantile that is
   * strictly less than <i>q</i>.</p>
   *
   * <p>If there is no quantile value that is strictly less than <i>q</i>,
   * the method will return a rank of zero.</p>
   *
   */
  EXCLUSIVE
}

