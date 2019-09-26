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

package org.apache.datasketches.tuple.adouble;

import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;

/**
 * Methods for defining how unions and intersections of two objects of type DoubleSummary
 * are performed. These methods are not called directly by a user.
 */
public final class DoubleSummarySetOperations implements SummarySetOperations<DoubleSummary> {

  private final Mode summaryMode_;

  //TODO see IntegerSummarySetOperations for better model

  /**
   * Creates an instance with default mode.
   */
  @Deprecated
  public DoubleSummarySetOperations() {
    summaryMode_ = DoubleSummary.Mode.Sum;
  }

  /**
   * Creates an instance given a DoubleSummary update mode.
   * @param summaryMode DoubleSummary update mode.
   */
  public DoubleSummarySetOperations(final Mode summaryMode) {
    summaryMode_ = summaryMode;
  }

  @Override
  public DoubleSummary union(final DoubleSummary a, final DoubleSummary b) {
    final DoubleSummary result = new DoubleSummary(summaryMode_);
    result.update(a.getValue());
    result.update(b.getValue());
    return result;
  }


  /**
   * Intersection is not well defined or even meaningful between numeric values.
   * Nevertheless, this can be defined to be just a different type of aggregation.
   * In this case it is defined to be the same as union. It can be overridden to
   * be a more meaningful operation.
   */
  @Override
  public DoubleSummary intersection(final DoubleSummary a, final DoubleSummary b) {
    return union(a, b);
  }
}
