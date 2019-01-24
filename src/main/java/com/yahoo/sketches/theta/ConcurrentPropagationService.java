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
  private static ConcurrentPropagationService instance = null;
  private static ExecutorService propagationExecutorService = null;
  //
  static int NUM_POOL_THREADS = 3; // Default: 3 threads

  public static void execute(Runnable job) {
    getInstance().getExecutorService().execute(job);
  }

  //make the constructor private so that this class cannot be
  //instantiated externaly
  private ConcurrentPropagationService(){
    propagationExecutorService = Executors.newWorkStealingPool(NUM_POOL_THREADS);
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

  private ExecutorService getExecutorService() {
    return propagationExecutorService;
  }
}
