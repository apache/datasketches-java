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

package org.apache.datasketches.sampling;

/**
 * A simple object o capture the results of a subset sum query on a sampling sketch.
 *
 * @author Jon Malkin
 */
public class SampleSubsetSummary {
  private double lowerBound;
  private double estimate;
  private double upperBound;
  private double totalSketchWeight;

  SampleSubsetSummary(final double lowerBound,
                      final double estimate,
                      final double upperBound,
                      final double totalSketchWeight) {
    this.lowerBound        = lowerBound;
    this.estimate          = estimate;
    this.upperBound        = upperBound;
    this.totalSketchWeight = totalSketchWeight;
  }

  /**
   * @return the Lower Bound
   */
  public double getLowerBound() {
    return lowerBound;
  }

  /**
   * @return the total sketch weight
   */
  public double getTotalSketchWeight() {
    return totalSketchWeight;
  }

  /**
   * @return the Upper Bound
   */
  public double getUpperBound() {
    return upperBound;
  }

  /**
   * @return the unique count estimate
   */
  public double getEstimate() {
    return estimate;
  }
}
