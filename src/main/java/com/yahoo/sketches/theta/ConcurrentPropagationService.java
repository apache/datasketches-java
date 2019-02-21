package com.yahoo.sketches.theta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Pool of threads to serve <i>all</i> propagation tasks in the system.
 *
 * @author eshcar
 */
final class ConcurrentPropagationService {

  static int NUM_POOL_THREADS = 3; // Default: 3 threads
  private static volatile ConcurrentPropagationService instance = null; // Singleton
  private static ExecutorService[] propagationExecutorService = null;


  private ConcurrentPropagationService() {
    propagationExecutorService = new ExecutorService[NUM_POOL_THREADS];
  }

  //Factory: Get the singleton
  private static ConcurrentPropagationService getInstance() {
    if (instance == null) {
      synchronized (ConcurrentPropagationService.class) {
        if (instance == null) {
          instance = new ConcurrentPropagationService();
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
