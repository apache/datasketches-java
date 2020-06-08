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

package org.apache.datasketches.tuple;

/**
 * This is to provide methods of producing unions and intersections of two Summary objects.
 * @param <S> type of Summary
 */
public interface SummarySetOperations<S extends Summary> {

  /**
   * This is called by the union operator when both sketches have the same hash value.
   *
   * <p><b>Caution:</b> Do not modify the input Summary objects. Also do not return them directly,
   * unless they are immutable (most Summary objects are not). For mutable Summary objects, it is
   * important to create a new Summary object with the correct contents to be returned. Do not
   * return null summaries.
   *
   * @param a Summary from sketch A
   * @param b Summary from sketch B
   * @return union of Summary A and Summary B
   */
  public S union(S a, S b);

  /**
   * This is called by the intersection operator when both sketches have the same hash value.
   *
   * <p><b>Caution:</b> Do not modify the input Summary objects. Also do not return them directly,
   * unless they are immutable (most Summary objects are not). For mutable Summary objects, it is
   * important to create a new Summary object with the correct contents to be returned. Do not
   * return null summaries.
   *
   * @param a Summary from sketch A
   * @param b Summary from sketch B
   * @return intersection of Summary A and Summary B
   */
  public S intersection(S a, S b);

}
