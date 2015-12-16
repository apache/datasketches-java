/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.demo;

import com.yahoo.sketches.demo.DemoImpl;


/**
 * <p>This demo computes a stream of values and feeds them first to
 * an exact sort-based method of computing the number of unique values
 * in the stream and then feeds a similar stream to two different types of
 * sketches from the library.
 * 
 * <p>This demo becomes most significant in the case where the number of uniques in the 
 * stream exceeds what the computer can hold in memory.
 * 
 * <p>This demo utilizes the Unix/Linux/OS-X sort and wc commands for the brute force compuation.
 * So this needs to be run on a linux or mac machine. A windows machine with a suitable unix
 * library installed might also work, but it has not been tested.
 */
public class ExactVsSketchDemo {
  
  /**
   * Runs the demo.
   * 
   * @param args 
   * <ul><li>arg[0]: The stream length and can be expressed as a positive double value.</li>
   * <li>arg[1] The fraction of the stream length that will be unique, the remainder will be 
   * duplicates.</li>
   * </ul>
   */
  public static void main(String[] args) {
    int argsLen = args.length;
    long streamLen = (long)1E6;   //The default stream length
    double uFrac = 1.0;          //The default fraction that are unique
    if (argsLen == 1) {
      streamLen = (long)(Double.parseDouble(args[0]));
    } else if (argsLen == 2) {
      uFrac = Double.parseDouble(args[1]);
    }
    
    DemoImpl demo = new DemoImpl(streamLen, uFrac);
    
    demo.runDemo();
  }
  
}