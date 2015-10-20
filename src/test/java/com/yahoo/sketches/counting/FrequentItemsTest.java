package com.yahoo.sketches.counting;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FrequentItemsTest {
	
  @Test
  public void construct() {
    int size = 100;
    FrequentItems f = new FrequentItems(size);
    Assert.assertNotNull(f);
  }
		
}
