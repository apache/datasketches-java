package com.yahoo.sketches.counting;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;


public class CuckooHashWithImplicitDeletionsTest {

  
  @Test
  public void oneUpdateWorks() {  
    CuckooHashWithImplicitDeletions cuckooHash = new CuckooHashWithImplicitDeletions(100);
    long key = 23951235L;
    cuckooHash.increment(key);
    Assert.assertEquals(cuckooHash.get(key), 1);
  }
  
  @Test
  public void oneUpdateAndOneDeleteWorks() {  
    CuckooHashWithImplicitDeletions cuckooHash = new CuckooHashWithImplicitDeletions(100);
    long key = 23951235L;
    cuckooHash.increment(key);
    cuckooHash.decrement();
    Assert.assertEquals(cuckooHash.get(key), 0);
  }

  @Test
  public void randomUpdatesCountsAreCorrect() {  
    int maxSize = 100;
    int nonZeroCounts = 10;
    CuckooHashWithImplicitDeletions cuckooHash = new CuckooHashWithImplicitDeletions(maxSize);
    Random random = new Random();
    long[] realCounts = new long[nonZeroCounts];
    for (int i=0; i<100; i++){
      int key = random.nextInt(nonZeroCounts);
      if (cuckooHash.increment(key)) realCounts[key]++;
    }
    for (int key=0; key<nonZeroCounts; key++){
      Assert.assertEquals(cuckooHash.get(key), realCounts[key]);
    }
  }
  
  @Test
  public void stressTestUpdateTime() {
    int n = 100000000;
    int maxSize = 1000000;  
    CuckooHashWithImplicitDeletions cuckooHash = new CuckooHashWithImplicitDeletions(maxSize);
    int error = 0;
    long key = 0;
    final long startTime = System.currentTimeMillis();
    for (int i=0; i<n; i++){
      key = (i < n/2) ? i%(maxSize/2) : i; 
      if (!cuckooHash.increment(key)) {
        cuckooHash.decrement();
        error++;
      }
    }
    final long endTime = System.currentTimeMillis();
    double timePerUpdate = (double)(endTime-startTime)/(double)n;
    System.out.println("Amortized time per update: " + timePerUpdate);
    System.out.println("Error: " + error);
    Assert.assertTrue(timePerUpdate < 10E-3);
  }
  
}
