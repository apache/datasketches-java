package com.yahoo.sketches.theta;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Pool of threads to serve *all* propagation tasks in the system.
 *
 * @author eshcar
 */
class ConcurrentPropagationService {

  // Singleton
  private static volatile ConcurrentPropagationService instance = null;
  private static ExecutorService[] propagationExecutorService = null;
  //
  static int NUM_POOL_THREADS = 3; // Default: 3 threads

  public static ExecutorService getExecutorService(long id) {
    return getInstance().initExecutorService((int)id%NUM_POOL_THREADS);
  }

  public static ExecutorService resetExecutorService(long id) {
    return getInstance().propagationExecutorService[(int)id%NUM_POOL_THREADS] = null;
  }

  //make the constructor private so that this class cannot be
  //instantiated externally
  private ConcurrentPropagationService(){
    propagationExecutorService = new ExecutorService[NUM_POOL_THREADS];
  }

  //Get the only object available
  private static ConcurrentPropagationService getInstance(){
    if(instance == null) {
      synchronized (ConcurrentPropagationService.class) {
        if (instance == null) {
          instance = new ConcurrentPropagationService();
        }
      }
    }
    return instance;
  }

  private ExecutorService initExecutorService(int i) {
    if(propagationExecutorService[i] == null) {
      propagationExecutorService[i] = Executors.newSingleThreadExecutor();
    }
    return propagationExecutorService[i];
  }
}
