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

package org.apache.datasketches.common;

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeSuite;

public class A_BeforeSuite {

  /**
   * For counting cross-language test cases.
   */
  private final AtomicInteger count = new AtomicInteger(0);

  public void incCount() {
    count.incrementAndGet(); // getAndIncrement()
  }

  public int getCount() {
    return count.get();
  }

  public void resetCount() {
    count.set(0);
  }

  @BeforeSuite(alwaysRun = true)
  public void printTestEnvironment() {
    System.out.println("====================================================");
    System.out.println("TEST JDK: " + System.getProperty("java.version"));
    System.out.println("TEST JDK HOME: " + System.getProperty("java.home"));
    System.out.println("=====================================================");
    resetCount();
  }

}
