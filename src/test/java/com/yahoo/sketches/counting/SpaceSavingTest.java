package com.yahoo.sketches.counting;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SpaceSavingTest {

  public static void main(String[] args){
	  	int size = 3;
	    SpaceSaving f = new SpaceSaving(size);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(1, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(1, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(2, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(3, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(3, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(2, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    f.update(4, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    System.out.println(f.get(4));
	    f.update(5, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    System.out.println(f.get(4));
	    System.out.println(f.get(5));
	    f.update(4, 1);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    System.out.println(f.get(4));
	    System.out.println(f.get(5));
	    f.update(1, 2);
	    System.out.println(f.get(1));
	    System.out.println(f.get(2));
	    System.out.println(f.get(3));
	    System.out.println(f.get(4));
	    System.out.println(f.get(5));
	    
  }
  @Test
  public void construct() {
    int size = 3;
    SpaceSaving f = new SpaceSaving(size);
    Assert.assertNotNull(f);
    f.update(1, 1);
    f.update(1, 1);
    f.update(2, 1);
    f.update(3, 1);
    f.update(3, 1);
    f.update(2, 1);
    f.update(4, 1);
    Boolean test = false;
    if (f.get(4) == 3){
    	test=true;
    }
    System.out.println(f.get(4));
    Assert.assertTrue(test);
    
  }
		
}
