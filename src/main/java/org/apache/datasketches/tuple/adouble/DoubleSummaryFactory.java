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

import org.apache.datasketches.tuple.SummaryFactory;

/**
 * Factory for DoubleSummary.
 *
 * @author Lee Rhodes
 */
public final class DoubleSummaryFactory implements SummaryFactory<DoubleSummary> {

  private final DoubleSummary.Mode summaryMode_;

  /**
   * Creates an instance of DoubleSummaryFactory with a given mode
   * @param summaryMode summary mode
   */
  public DoubleSummaryFactory(final DoubleSummary.Mode summaryMode) {
    summaryMode_ = summaryMode;
  }

  @Override
  public DoubleSummary newSummary() {
    return new DoubleSummary(summaryMode_);
  }

}
