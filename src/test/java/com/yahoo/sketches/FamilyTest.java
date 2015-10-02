/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static com.yahoo.sketches.Family.*;
import static org.testng.Assert.*;

import org.testng.annotations.Test;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author Lee Rhodes
 */
public class FamilyTest {
  
  @Test
  public void checkFamilyEnum() {
    println("ID to Family:");
    for (int i=1; i<=6; i++) {
      Family fam = idToFamily(i);
      println(fam.toString());
    }
    println("\nString to Family:");
    checkStringToFamily("Alpha");
    checkStringToFamily("QuickSelect");
    checkStringToFamily("Union");
    checkStringToFamily("Intersection");
    checkStringToFamily("AnotB");
    
    println("\nObject to Family:");
    Sketch sk1 = UpdateSketch.builder().setFamily(ALPHA).build(512);
    println(objectToFamily(sk1).toString());
  }
  
  private static void checkStringToFamily(String inStr) {
    String fName = stringToFamily(inStr).toString();
    assertEquals(fName, inStr);
  }
  
  @Test
  public void checkFamily() {
    UpdateSketch sk = UpdateSketch.builder().build();
    println(sk.getClass().getSimpleName());
    println(Family.objectToFamily(sk).toString());
    
    for (Family f : Family.values()) {
      String fstr = f.toString();
      println("Name: "+fstr + ": ID: "+f.getID());
    }
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadFamilyName() {
    stringToFamily("Test");
  }
  
  @SuppressWarnings("unused")
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadObject() {
    objectToFamily("Test");
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadFamilyID() {
    Family famAlpha = Family.ALPHA;
    Family famQS = Family.QUICKSELECT;
    famAlpha.checkFamilyID(famQS.getID());
  }
  
  @Test
  public void checkValidSketchID() {
    assertFalse(isValidSketchID(0));
    assertTrue(isValidSketchID(ALPHA.getID()));
    assertTrue(isValidSketchID(QUICKSELECT.getID()));
    assertTrue(isValidSketchID(COMPACT.getID()));
    assertFalse(isValidSketchID(UNION.getID()));
    assertFalse(isValidSketchID(INTERSECTION.getID()));
    assertFalse(isValidSketchID(A_NOT_B.getID()));
    assertFalse(isValidSketchID(HLL.getID()));
  }
  
  @Test
  public void checkValidSetOpID() {
    assertFalse(isValidSetOpID(0));
    assertFalse(isValidSetOpID(ALPHA.getID()));
    assertFalse(isValidSetOpID(QUICKSELECT.getID()));
    assertFalse(isValidSetOpID(COMPACT.getID()));
    assertTrue(isValidSetOpID(UNION.getID()));
    assertTrue(isValidSetOpID(INTERSECTION.getID()));
    assertTrue(isValidSetOpID(A_NOT_B.getID()));
    assertFalse(isValidSetOpID(HLL.getID()));
  }
  
  @Test
  public void printlnTest() {
    println(this.getClass().getSimpleName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}