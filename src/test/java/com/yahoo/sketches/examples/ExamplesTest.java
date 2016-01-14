/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.examples;

import org.testng.annotations.Test;

import com.yahoo.sketches.BinomialBounds;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;

public class ExamplesTest {
  
  @Test
  public void setOpsExample() {
    println("Set Operations Example:");
    int k = 4096;
    UpdateSketch skA = Sketches.updateSketchBuilder().build(k);
    UpdateSketch skB = Sketches.updateSketchBuilder().build(k);
    UpdateSketch skC = Sketches.updateSketchBuilder().build(k);
    
    for (int i=1;  i<=10; i++) { skA.update(i); }
    for (int i=1;  i<=20; i++) { skB.update(i); }
    for (int i=6;  i<=15; i++) { skC.update(i); } //overlapping set
    
    Union union = Sketches.setOperationBuilder().buildUnion(k);
    union.update(skA);
    union.update(skB);
    // ... continue to iterate on the input sketches to union

    CompactSketch unionSk = union.getResult();   //the result union sketch
    println("A U B      : "+unionSk.getEstimate());   //the estimate of the union

    //Intersection is similar
    
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.update(unionSk);
    inter.update(skC);
    // ... continue to iterate on the input sketches to intersect

    CompactSketch interSk = inter.getResult();  //the result intersection sketch 
    println("(A U B) ^ C: "+interSk.getEstimate());  //the estimate of the intersection

    //The AnotB operation is a little different as it is stateless:

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    aNotB.update(skA, skC);

    CompactSketch not = aNotB.getResult();
    println("A \\ C      : "+not.getEstimate()); //the estimate of the AnotB operation
  }
  
  @Test
  public void boundsExample() {
    println("BinomialBounds Example:");
    int k = 500;
    double theta = 0.001;
    int stdDev = 2;
    double ub = BinomialBounds.getUpperBound(k, theta, stdDev, false);
    double est = k/theta;
    double lb = BinomialBounds.getLowerBound(k, theta, stdDev, false);
    println("K="+k+", Theta="+theta+", SD="+stdDev);
    println("UB:  "+ub);
    println("Est: "+est);
    println("LB:  "+lb);
    println("");
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
  public static void main(String[] args) {
    ExamplesTest ext = new ExamplesTest();
    ext.setOpsExample();
    ext.boundsExample();

  }
}
