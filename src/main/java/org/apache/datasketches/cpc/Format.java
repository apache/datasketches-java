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
 * There are seven different preamble formats (8 combinations) that determine the layout of the
 * <i>HiField</i> variables after the first 8 bytes of the preamble.
 * Do not change the order.
 */
enum Format {
  EMPTY_MERGED,
  EMPTY_HIP,
  SPARSE_HYBRID_MERGED,
  SPARSE_HYBRID_HIP,
  PINNED_SLIDING_MERGED_NOSV,
  PINNED_SLIDING_HIP_NOSV,
  PINNED_SLIDING_MERGED,
  PINNED_SLIDING_HIP;

  private static Format[] fmtArr = Format.class.getEnumConstants();

  /**
   * Returns the Format given its enum ordinal
   * @param ordinal the given enum ordinal
   * @return the Format given its enum ordinal
   */
  static Format ordinalToFormat(final int ordinal) {
    return fmtArr[ordinal];
  }

} //end enum Format
