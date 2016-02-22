/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.demo;

import static java.lang.Math.sqrt;
import static com.yahoo.sketches.hash.MurmurHash3.hash;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * A simple demo that compares brute force counting of uniques vs. using sketches.
 * 
 * <p>This demo computes a stream of values and feeds them first to
 * an exact sort-based method of computing the number of unique values
 * in the stream and then feeds a similar stream to two different types of
 * sketches from the library.
 * 
 * <p>This demo becomes most significant in the case where the number of uniques in the 
 * stream exceeds what the computer can hold in memory. 
 * 
 * <p>This demo utilizes the Unix sort and wc commands for the brute force compuation.
 * So this needs to be run on a linux or mac machine. A windows machine with a similar unix
 * library installed should also work, but it has not been tested.
 */
public class DemoImpl {
  //Static constants
  private static final String LS = System.getProperty("line.separator");
  private static final byte LS_BYTE = LS.getBytes()[0];
  private static Random rand = new Random();
  private static StandardOpenOption C = StandardOpenOption.CREATE;
  private static StandardOpenOption W = StandardOpenOption.WRITE;
  private static StandardOpenOption TE = StandardOpenOption.TRUNCATE_EXISTING;
  
  //Stream Configuration
  private int byteBufCap_ = 1000000; //ByteBuffer capacity
  private long n_ = (long)1E8; //stream length
  private final int threshold_; //equivalent uniquesFraction on integer scale
  
  //Sketch configuration
  private int lgK_ = 14; //16K
  
  //Internal sketch values
  private int maxMemSkBytes_;
  private double rse2_;  //RSE for 95% confidence
  private UpdateSketch tSketch_ = null;
  private HllSketch hllSketch_ = null;
  
  //Other internal values
  private Path path = Paths.get("tmp/test.txt");
  private long[] vArr_ = new long[1]; //reuse this array
  private long fileBytes_ = 0;
  private long u_ = 0;    //unique count;
  
  /**
   * Constuct the demo.
   * @param streamLen  The total stream length.
   * @param uniquesFraction the fraction of streamLen values less than 1.0, that will be unique. 
   * The actual # of uniques will vary around this value, because it is computed statistically.
   */
  public DemoImpl(long streamLen, double uniquesFraction) {
    if (uniquesFraction == 1.0) {
      this.threshold_ = Integer.MAX_VALUE;
    }
    else {
      this.threshold_ = (int)(Integer.MAX_VALUE * uniquesFraction);
    }
    n_ = streamLen;
    lgK_ = 14; //Log-base 2 of the configured sketch size = 16K
    File dir = new File("tmp");
    if (!dir.exists()) {
      try {
        dir.mkdir();
      } catch(SecurityException e) {
        throw new SecurityException(e);
      }
    }
  }
  
  /**
   * Run the demo
   */
  public void runDemo() {
    println("# COMPUTE DISTINCT COUNT EXACTLY:");
    long exactTimeMS;
    
    exactTimeMS = buildFile();
    //exactTimeMS = buildFileAndSketch(); //used instead only for testing
    
    println("## SORT & REMOVE DUPLICATES");
    String sortCmd = "sort -u -o tmp/sorted.txt tmp/test.txt";
    exactTimeMS += runUnixCmd("sort", sortCmd);
    
    println("\n## LINE COUNT");
    String wcCmd = "wc -l tmp/sorted.txt";
    exactTimeMS += runUnixCmd("wc", wcCmd);
    
    println("Total Exact "+getMinSec(exactTimeMS) +LS+LS);
    
    println("# COMPUTE DISTINCT COUNT USING SKETCHES");
    configureThetaSketch();
    long sketchTimeMS = buildSketch();
    double factor = exactTimeMS*1.0/sketchTimeMS;
    println("Speedup Factor "+String.format("%.1f", factor) + LS);
    
    configureHLLSketch();
    sketchTimeMS = buildSketch();
    factor = exactTimeMS*1.0/sketchTimeMS;
    println("Speedup Factor "+String.format("%.1f", factor));
    
  }
  
  /**
   * @return total test time in milliseconds
   */
  private long buildFile() {
    println("## BUILD FILE:");
    ByteBuffer byteBuf = ByteBuffer.allocate(byteBufCap_);
    u_ = 0;
    fileBytes_ = 0;
    long testStartTime_mS = System.currentTimeMillis();
    try (SeekableByteChannel sbc = Files.newByteChannel(path, C, W, TE)) {
      for (long i=0; i<n_; i++) {
        long v = nextValue();
        String s = Long.toHexString(v);
        if (byteBuf.remaining() < 25) {
          byteBuf.flip();
          fileBytes_ += sbc.write(byteBuf);
          byteBuf.clear();
        }
        byteBuf.put(s.getBytes()).put(LS_BYTE);
      }
      if (byteBuf.position() > 0) { //write remainder
        byteBuf.flip();
        fileBytes_ += sbc.write(byteBuf);
        byteBuf.clear();
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    long testTime_mS = System.currentTimeMillis() - testStartTime_mS;
    //Print common results
    printCommon(testTime_mS, n_, u_);
    //Print file results
    println("File Size Bytes: "+String.format("%,d", fileBytes_) + LS);
    return testTime_mS;
  }
  
  /**
   * @return total test time in milliseconds
   */
  private static long runUnixCmd(String name, String cmd) {
    StringBuilder sbOut = new StringBuilder();
    StringBuilder sbErr = new StringBuilder();
    String out = null;
    String err = null;
    Process p = null;
    String[] envp = {"LC_ALL=C"}; //https://bugs.launchpad.net/ubuntu/+source/coreutils/+bug/846628
    long testStartTime_mS = System.currentTimeMillis();
    try {
      // run the Unix cmd using the Runtime exec method:
      p = Runtime.getRuntime().exec(cmd, envp);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

      // read the output from the command
      boolean outFlag = true;
      while ((out = stdInput.readLine()) != null) {
        if (outFlag) {
          sbOut.append("Output from "+name+" command:").append(LS);
          outFlag = false;
        }
        sbOut.append(out).append(LS);
      }

      // read any errors from the attempted command
      boolean errFlag = true;
      while ((err = stdError.readLine()) != null) {
        if (errFlag) {
          sbErr.append("\nError from "+name+" command:").append(LS);
          errFlag = false;
        }
        sbErr.append(err).append(LS);
      }
    }
    catch (IOException e) {
      System.out.println("Exception: ");
      e.printStackTrace();
      System.exit( -1);
    }
    if ((p != null) && (p.isAlive())) {
      p.destroy();
    }
    long testTime_mS = System.currentTimeMillis() - testStartTime_mS;
    println("Unix cmd: "+cmd);
    println(getMinSec(testTime_mS));
    if (sbOut.length() > 0) { println(sbOut.toString()); }
    if (sbErr.length() > 0) { println(sbErr.toString()); }
    return testTime_mS;
  }
  
  /**
   * @return total test time in milliseconds
   */
  private long buildSketch() {
    u_ = 0; //unique counter for accuracy computation
    long testStartTime_mS = System.currentTimeMillis();
    
    if (tSketch_ != null) { //Theta Sketch
      for (long i = 0; i < n_; i++) {
        long v = nextValue();
        tSketch_.update(v);
      }
    } 
    else { //HLL Sketch
      for (long i = 0; i < n_; i++) {
        long v = nextValue();
        hllSketch_.update(v);
      }
    }
    long testTime_mS = System.currentTimeMillis() - testStartTime_mS;
    
    //Print sketch name
    String sk = (tSketch_ != null)? "THETA" : "HLL";
    println("## USING "+sk+" SKETCH");
    //Print common results
    printCommon(testTime_mS, n_, u_);
    
    //Print sketch results
    printSketchResults(u_, maxMemSkBytes_, rse2_);
    return testTime_mS;
  }
  
  /**
   * Used in testing
   * @return total test time in milliseconds
   */
  @SuppressWarnings("unused")
  private long buildFileAndSketch() {
    println("## BUILD FILE AND SKETCH:");
    ByteBuffer byteBuf = ByteBuffer.allocate(byteBufCap_);
    u_ = 0;
    fileBytes_ = 0;
    long testStartTime_mS = System.currentTimeMillis();
    try (SeekableByteChannel sbc = Files.newByteChannel(path, C, W, TE)) {
      if (tSketch_ != null) {
        long v = nextValue();
        tSketch_.update(v);
        
        //build file
        String s = Long.toHexString(v);
        if (byteBuf.remaining() < 25) {
          byteBuf.flip();
          fileBytes_ += sbc.write(byteBuf);
          byteBuf.clear();
        }
        byteBuf.put(s.getBytes()).put(LS_BYTE);
      }
      else { //HLL Sketch
        long v = nextValue();
        hllSketch_.update(v);
        
        //build file
        String s = Long.toHexString(v);
        if (byteBuf.remaining() < 25) {
          byteBuf.flip();
          fileBytes_ += sbc.write(byteBuf);
          byteBuf.clear();
        }
        byteBuf.put(s.getBytes()).put(LS_BYTE);
      }

      if (byteBuf.position() > 0) {
        byteBuf.flip();
        fileBytes_ += sbc.write(byteBuf);
        byteBuf.clear();
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    long testTime_mS = System.currentTimeMillis() - testStartTime_mS;
    
    //Print common results
    printCommon(testTime_mS, n_, u_);
    //Print file results
    println("File Size Bytes: "+String.format("%,d", fileBytes_));
    
    //Print sketch results
    printSketchResults(u_, maxMemSkBytes_, rse2_);
    return testTime_mS;
  }
  
  /**
   * @return next hashed long value
   */
  private long nextValue() { 
    if (((rand.nextInt() >>> 1) < threshold_) || (u_ == 0)) {
      u_++;
    }
    vArr_[0] = u_;
    return hash(vArr_, 0L)[0];
  }
  
//  private long nextValue() {  //Faster version, always 100% uniques
//    vArr_[0] = ++u_;
//    return hash(vArr_, 0L)[0];
//  }
  
  private final void configureThetaSketch() {
    int k = 1 << lgK_; //14 
    hllSketch_ = null;
    maxMemSkBytes_ = k *16; //includs full hash table
    rse2_ = 2.0/sqrt(k);    //Error for 95% confidence
    tSketch_ = Sketches.updateSketchBuilder().
        setResizeFactor(ResizeFactor.X1).
        setFamily(Family.ALPHA).build(k );
  }
  
  private final void configureHLLSketch() {
    int k = 1 << lgK_; //14 
    boolean compressed = true;
    boolean hipEstimator = true;
    boolean denseMode = true;
    tSketch_ = null;
    maxMemSkBytes_ = (compressed)? k/2 : k;
    rse2_ = 2.0 * ((hipEstimator)? 0.836/sqrt(k) : 1.04/sqrt(k)); //for 95% confidence
    hllSketch_ = HllSketch.builder().setLogBuckets(lgK_).
        setHipEstimator(hipEstimator).
        setDenseMode(denseMode).
        setCompressedDense(compressed).
        build();
  }
  
  private static void printCommon(long testTime, long n, long u) {
    println(getMinSec(testTime));
    println("Total Values: "+String.format("%,d",n));
    int nSecRate = (int) (testTime *1000000.0/n);
    println("Build Rate: "+ String.format("%d nSec/Value", nSecRate));
    println("Exact Uniques: "+String.format("%,d", u));
  }
  
  private void printSketchResults(long u, int maxMemSkBytes, double rse2) {
    double rounded = Math.round((tSketch_ != null)? tSketch_.getEstimate() : hllSketch_.getEstimate());
    println("Sketch Estimate of Uniques: "+ String.format("%,d", (long)rounded));
    double err = (u == 0)? 0 : (rounded/u - 1.0);
    println("Sketch Relative Error: "+String.format("%.3f%%, +/- %.3f%%", err*100, rse2*100));
    println("Max Sketch Size Bytes: "+ String.format("%,d", maxMemSkBytes));
  }
  
  private static String getMinSec(long mSec) {
    int totSec = (int)(mSec/1000.0);
    int min = totSec/60;
    int sec = totSec%60;
    int ms  = (int)(mSec - totSec * 1000);
    String t = String.format("Time Min:Sec.mSec = %d:%02d.%03d", min, sec, ms);
    return t;
  }
  
  private static void println(String s) { System.out.println(s); }
  
}
