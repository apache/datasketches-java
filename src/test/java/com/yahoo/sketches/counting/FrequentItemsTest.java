package com.yahoo.sketches.counting;

import org.testng.annotations.Test;
import org.testng.Assert;
import java.lang.Math;

/**
 * Tests FrequentItems class
 * 
 * @author edo
 * 
 */
public class FrequentItemsTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void construct() {
    int size = 100;
    FrequentItems frequentItems = new FrequentItems(size);
    Assert.assertNotNull(frequentItems);
    // Should throw exception
    frequentItems = new FrequentItems(-134);
  }

  @Test
  public void updateOneTime() {
    int size = 100;
    FrequentItems frequentItems = new FrequentItems(size);
    frequentItems.update(13L);
    Assert.assertEquals(frequentItems.nnz(), 1);
  }
  
  @Test
  public void sizeDoesNotGrow() {
    int maxSize = 100;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (long key=0L; key<10000L; key++){
      frequentItems.update(key);
      Assert.assertTrue(frequentItems.nnz() <= maxSize);
    }
  }
  
  @Test
  public void estimatesAreCorectBeofreDeletePhase() {
    int maxSize = 100;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (long key=0L; key<99L; key++){
      frequentItems.update(key);
      Assert.assertTrue(frequentItems.get(key) == 1);
      Assert.assertTrue(frequentItems.getMaxError() == 0);
    }
  }

  /**
   * @param prob the probability of success for the geometric distribution. 
   * @return a random number generated from the geometric distribution.
   */
  static private long randomGeometricDist(double prob){
    assert(prob > 0.0 && prob < 1.0);
    return (long) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }
  
  @Test
  public void testRandomGeometricDist() {
    long maxKey = 0L;
    double prob = .1;
    for (int i=0; i<100; i++){
      long key = randomGeometricDist(prob) ;
      if (key > maxKey) maxKey = key;
      // If you succeed with probability p the probability 
      // of failing 20/p times is smaller than 1/2^20.
      Assert.assertTrue(maxKey < 20.0/prob);
    }
  }
   
  @Test
  public void realCountsInBounds() {
    int n = 4213;
    int maxSize = 50;
    long key;
    double prob = .04; 
    FrequentItems frequentItems = new FrequentItems(maxSize);
    PositiveCountersMap realCounts = new PositiveCountersMap();
    for (int i=0; i<n; i++){   
      key = randomGeometricDist(prob);
      frequentItems.update(key);
      realCounts.increment(key);
      long realCount = realCounts.get(key);
      long lowerBound = frequentItems.get(key);
      long upperBound = frequentItems.get(key) + frequentItems.getMaxError();
      Assert.assertTrue(upperBound >=  realCount && realCount >= lowerBound);   
    }
  }
  
  @Test
  public void errorWithinLimits() {
    int n = 100;
    int maxSize = 20;
    long key;
    double prob = .1;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (int i=0; i<n; i++){
      key = randomGeometricDist(prob);
      frequentItems.update(key);
      long lowerBound = frequentItems.get(key);
      long upperBound = frequentItems.get(key) + frequentItems.getMaxError();
      Assert.assertTrue(upperBound - lowerBound <= i/maxSize);  
    }
  } 
    
  @Test
  public void realCountsInBoundsAfterUnion() {
    int n = 1000;
    int maxSize1 = 100;
    int maxSize2 = 400;
    double prob1 = .01;
    double prob2 = .005;
   
    PositiveCountersMap realCounts = new PositiveCountersMap();
    FrequentItems frequentItems1 = new FrequentItems(maxSize1);
    FrequentItems frequentItems2 = new FrequentItems(maxSize2);
    for (int i=0; i<n; i++){
      long key1 = randomGeometricDist(prob1);
      long key2 = randomGeometricDist(prob2);
      
      frequentItems1.update(key1);
      frequentItems2.update(key2);
      
      // Updating the real counters
      realCounts.increment(key1);
      realCounts.increment(key2);
    }
    FrequentItems frequentItems = frequentItems1.union(frequentItems2);

    for ( long key : realCounts.keys()){
      
      long realCount = realCounts.get(key);
      long lowerBound = frequentItems.get(key);
      long upperBound = frequentItems.get(key) + frequentItems.getMaxError();
      Assert.assertTrue(upperBound >=  realCount && realCount >= lowerBound);
    }
  }
  
  @Test
  public void stressTestUpdateTime() {
    int n = 1000000;
    int maxSize = 1000;  
    FrequentItems frequentItems = new FrequentItems(maxSize);
    double prob = 1.0/n;
    final long startTime = System.currentTimeMillis();
    for (int i=0; i<n; i++){
      long key = randomGeometricDist(prob);
      frequentItems.update(key);
    }
    final long endTime = System.currentTimeMillis();
    double timePerUpdate = (double)(endTime-startTime)/(double)n;
    System.out.println("Amortized time per update: " + timePerUpdate);
    Assert.assertTrue(timePerUpdate < 10E-3);
  }

}
