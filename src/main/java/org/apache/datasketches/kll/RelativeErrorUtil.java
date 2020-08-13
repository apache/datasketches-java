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

package org.apache.datasketches.kll;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc"})
public class RelativeErrorUtil {
  final static double SECTION_SIZE_SCALAR = 0.5;
  final static double NEVER_SIZE_SCALAR = 0.5;
  final static int INIT_NUMBER_OF_SECTIONS = 2;
  final static int SMALLEST_MEANINGFUL_SECTION_SIZE = 4;
  final static double DEFAULT_EPS = 0.01;
  //the sketch gives rather bad results for eps > 0.1
  final static double EPS_UPPER_BOUND = 0.1;

  public enum Schedule { DETERMINISTIC, RANDOMIZED, RANDOMIZED_LINAR }
}
