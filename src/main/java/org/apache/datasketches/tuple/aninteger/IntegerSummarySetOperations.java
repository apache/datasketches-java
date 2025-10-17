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

package org.apache.datasketches.tuple.aninteger;

import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode;

/**
 * Methods for defining how unions and intersections of two objects of type IntegerSummary
 * are performed.
 *
 * @author Lee Rhodes
 */
public class IntegerSummarySetOperations implements SummarySetOperations<IntegerSummary> {

  private final Mode unionSummaryMode_;

  /**
   * TupleIntersection is not well defined or even meaningful between numeric values.
   * Nevertheless, this can be defined to be a different type of aggregation for intersecting hashes.
   */
  private final Mode intersectionSummaryMode_;

  /**
   * Creates a new instance with two modes
   * @param unionSummaryMode for unions
   * @param intersectionSummaryMode for intersections
   */
  public IntegerSummarySetOperations(final Mode unionSummaryMode, final Mode intersectionSummaryMode) {
    unionSummaryMode_ = unionSummaryMode;
    intersectionSummaryMode_ = intersectionSummaryMode;
  }

  @Override
  public IntegerSummary union(final IntegerSummary a, final IntegerSummary b) {
    final IntegerSummary result = new IntegerSummary(unionSummaryMode_);
    result.update(a.getValue());
    result.update(b.getValue());
    return result;
  }

  @Override
  public IntegerSummary intersection(final IntegerSummary a, final IntegerSummary b) {
    final IntegerSummary result = new IntegerSummary(intersectionSummaryMode_);
    result.update(a.getValue());
    result.update(b.getValue());
    return result;
  }

}
