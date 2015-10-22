package com.yahoo.sketches.counting;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.lang.Math;
import java.util.HashMap;

/**
 * @author edo
 *
 */

public class FrequentItemsTest {

	@Test
  public void construct() {
    int size = 100;
    FrequentItems frequentItems = new FrequentItems(size);
    AssertJUnit.assertNotNull(frequentItems);
  }
		
  @Test
  public void updateOneTime() {
    int size = 100;
    FrequentItems frequentItems = new FrequentItems(size);
    long[] key = new long[] {13};
    frequentItems.update(key);
    AssertJUnit.assertEquals(frequentItems.getSize(), 1);
  }
 
  @Test
  public void sizeDoesNotGrow() {
    int maxSize = 100;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (int key=0; key<10000; key++){
    	frequentItems.update(key);
      AssertJUnit.assertTrue(frequentItems.getSize() <= maxSize);
    }
  }
  
  @Test
  public void estimatesAreCorectBeofreDeletePhase() {
    int maxSize = 100;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (int key=0; key<99; key++){
    	frequentItems.update(key);
    	AssertJUnit.assertTrue(frequentItems.getEstimate(key) == 1);
    	AssertJUnit.assertTrue(frequentItems.getDeletePhases() == 0);
    }
  }
  
  //Draws a number from a skewed distribution (geometric)
  private int randomGeometricDist(double prob){
  	return (int) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }
  
  @Test
  public void testRandomGeometricDist() {
  	int maxKey = 0;
    double prob = .1;
    for (int i=0; i<100; i++){
    	// Draws a number from a skewed distribution (geometric)
    	int key = randomGeometricDist(prob) ;
    	if (key > maxKey) maxKey = key;
    	//System.out.println(key);
    	Assert.assertTrue((double)maxKey < 20.0/prob);
    }
  }
   
  @Test
  public void realCountsInBounds() {
    int n = 4213;
    int maxSize = 50;
    int key;
    double prob = .04;
    HashMap<Integer,Integer> realCounts = new HashMap<Integer,Integer>();
   
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (int i=0; i<n; i++){ 	
    	key = randomGeometricDist(prob);
    	
    	// Updating the real counters
    	if (! realCounts.containsKey(key)) realCounts.put(key,0); 
    	realCounts.put(key, realCounts.get(key)+1);
    	
    	frequentItems.update(key);
    	
    	AssertJUnit.assertTrue(realCounts.get(key) >= frequentItems.getEstimate(key));
    	AssertJUnit.assertTrue(realCounts.get(key) <= frequentItems.getUpperBound(key)); 	
    }
  }
  
  @Test
  public void errorWithinLimits() {
    int n = 8713;
    int maxSize = 50;
    int key;
    double p = .04;
    FrequentItems frequentItems = new FrequentItems(maxSize);
    for (int i=0; i<n; i++){
    	// Draws a number from a skewed distribution (geometric)
    	key = (int) -(Math.log(Math.random()) / p);
    	frequentItems.update(key);
    	AssertJUnit.assertTrue(frequentItems.getUpperBound(key) - frequentItems.getEstimate(key) <= i/maxSize);	
    }
  } 
    
  @Test
  public void testUnionErrorWithinBounds() {
    int n = 1000;
    int maxSize1 = 100;
    int maxSize2 = 400;
    double prob1 = .01;
    double prob2 = .005;
   
    HashMap<Integer,Integer> realCounts = new HashMap<Integer,Integer>();
    FrequentItems frequentItems1 = new FrequentItems(maxSize1);
    FrequentItems frequentItems2 = new FrequentItems(maxSize2);
    for (int i=0; i<n; i++){
    	int key1 =randomGeometricDist(prob1);
    	frequentItems1.update(key1);
    	int key2 =randomGeometricDist(prob2);
    	frequentItems1.update(key2);
    	
    	// Updating the real counters
    	if (! realCounts.containsKey(key1)) realCounts.put(key1,0); 
    	realCounts.put(key1, realCounts.get(key1)+1);
    	// Updating the real counters
    	if (! realCounts.containsKey(key2)) realCounts.put(key2,0); 
    	realCounts.put(key2, realCounts.get(key2)+1);
    }
    FrequentItems frequentItems = frequentItems1.union(frequentItems2);

    for ( Integer key : realCounts.keySet()){
    	AssertJUnit.assertTrue(realCounts.get(key) >= frequentItems.getEstimate(key));
    	AssertJUnit.assertTrue(realCounts.get(key) <= frequentItems.getUpperBound(key));
    }
  	 	
  }
  
  
}
