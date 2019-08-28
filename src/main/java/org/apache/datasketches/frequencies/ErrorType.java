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

package org.apache.datasketches.frequencies;

/**
 * Specifies one of two types of error regions of the statistical classification Confusion Matrix
 * that can be excluded from a returned sample of Frequent Items.
 */
public enum ErrorType {

  /**
   * No <i>Type I</i> error samples will be included in the sample set,
   * which means all <i>Truly Negative</i> samples will be excluded from the sample set.
   * However, there may be <i>Type II</i> error samples (<i>False Negatives</i>)
   * that should have been included that were not.
   * This is a subset of the NO_FALSE_NEGATIVES ErrorType.
   */
  NO_FALSE_POSITIVES,

  /**
   * No <i>Type II</i> error samples will be excluded from the sample set,
   * which means all <i>Truly Positive</i> samples will be included in the sample set.
   * However, there may be <i>Type I</i> error samples (<i>False Positives</i>)
   * that were included that should not have been.
   */
  NO_FALSE_NEGATIVES
}
