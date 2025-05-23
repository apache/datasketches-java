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

package org.apache.datasketches.theta2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.datasketches.common.SuppressFBWarnings;

/**
 * Pool of threads to serve <i>all</i> propagation tasks in the system.
 *
 * @author Eshcar Hillel
 */
final class ConcurrentPropagationService {

  static int NUM_POOL_THREADS = 3; // Default: 3 threads
  private static volatile ConcurrentPropagationService instance = null; // Singleton
  private static ExecutorService[] propagationExecutorService = null;

  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "Fix later")
  private ConcurrentPropagationService() {
    propagationExecutorService = new ExecutorService[NUM_POOL_THREADS];
  }

  //Factory: Get the singleton
  @SuppressFBWarnings(value = "SSD_DO_NOT_USE_INSTANCE_LOCK_ON_SHARED_STATIC_DATA", justification = "Fix later")
  private static ConcurrentPropagationService getInstance() {
    if (instance == null) {
      synchronized (ConcurrentPropagationService.class) {
        if (instance == null) {
          instance = new ConcurrentPropagationService(); //SpotBugs: SSD_DO_NOT_USE_INSTANCE_LOCK_ON_SHARED_STATIC_DATA
        }
      }
    }
    return instance;
  }

  public static ExecutorService getExecutorService(final long id) {
    return getInstance().initExecutorService((int) id % NUM_POOL_THREADS);
  }

  @SuppressWarnings("static-access")
  public static ExecutorService resetExecutorService(final long id) {
    return getInstance().propagationExecutorService[(int) id % NUM_POOL_THREADS] = null;
  }

  @SuppressWarnings("static-method")
  private ExecutorService initExecutorService(final int i) {
    if (propagationExecutorService[i] == null) {
      propagationExecutorService[i] = Executors.newSingleThreadExecutor();
    }
    return propagationExecutorService[i];
  }
}
