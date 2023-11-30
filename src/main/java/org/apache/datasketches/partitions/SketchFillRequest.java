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

package org.apache.datasketches.partitions;

import org.apache.datasketches.quantilescommon.PartitioningFeature;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;

/**
 * This is a callback request to the data source to fill a quantiles sketch,
 * which is returned to the caller.
 *
 * @author Lee Rhodes
 */
public interface SketchFillRequest<T, S extends QuantilesGenericAPI<T> & PartitioningFeature<T>> {

  /**
   * This is a callback request to the data source to fill a quantiles sketch
   * with a range of data between upper and lower bounds. Which of these bounds are to be included is determined by
   * the <i>BoundsRule</i>.
   *
   * <p>This range of data may or may not be subsequently further partitioned.</p>
   * @param lowerQuantile the lowest quantile of a range
   * @param upperQuantile the highest quantile of a range
   * @param boundsRule determines which quantile bounds to include
   * @return a quantiles sketch filled from the given upper and lower bounds.
   */
  public S getRange(final T lowerQuantile, final T upperQuantile, final BoundsRule boundsRule);

}
