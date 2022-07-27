<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

[![Build Status](https://travis-ci.org/apache/datasketches-java.svg?branch=master)](https://travis-ci.org/apache/datasketches-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/datasketches-java/context:java)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/apache/datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/datasketches-java/alerts/)
[![Coverage Status](https://coveralls.io/repos/github/apache/datasketches-java/badge.svg)](https://coveralls.io/github/apache/datasketches-java)

=================

# Apache<sup>&reg;</sup> DataSketches&trade; Core Java Library Component
This is the core Java component of the DataSketches library.  It contains all of the sketching algorithms and can be accessed directly from user applications. 

This component is also a dependency of other components of the library that create adaptors for target systems, such as the [Apache Pig adaptor](https://github.com/apache/datasketches-pig) and the [Apache Hive adaptor](https://github.com/apache/datasketches-hive).

Note that we have a parallel core component for C++ and Python implementations of the same sketch algorithms, 
[datasketches-cpp](https://github.com/apache/datasketches-cpp).

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---

## Maven Build Instructions
__NOTE:__ This component accesses resource files for testing. As a result, the directory elements of the full absolute path of the target installation directory must qualify as Java identifiers. In other words, the directory elements must not have any space characters (or non-Java identifier characters) in any of the path elements. This is required by the Oracle Java Specification in order to ensure location-independent access to resources: [See Oracle Location-Independent Access to Resources](https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html)

### A JDK8 with Hotspot through JDK13 with Hotspot is required to compile
This component depends on the [datasketches-memory](https://github.com/apache/datasketches-memory) component, 
and, as a result, must be compiled with one of the above JDKs. 
If your application only relies on the APIs of this component no special JVM arguments are required.
However, if your application also directly relies on the APIs of the *datasketches-memory* component, 
you may need additional JVM arguments.
Please refer to the [datasketches-memory README](https://github.com/apache/datasketches-memory/blob/master/README.md) for details.

If your application uses Maven, you can also use the *pom.xml* of this component as an example of how to automatically
configure the JVM arguments for compilation and testing based on the version of the JDK.

### Recommended Build Tool
This DataSketches component is structured as a Maven project and Maven is the recommended Build Tool.

There are two types of tests: normal unit tests and tests run by the strict profile.  

To run normal unit tests:

    $ mvn clean test

To run the strict profile tests (only supported in Java 8):

    $ mvn clean test -P strict

To install jars built from the downloaded source:

    $ mvn clean install -DskipTests=true

This will create the following jars:

* datasketches-java-X.Y.Z.jar The compiled main class files.
* datasketches-java-X.Y.Z-tests.jar The compiled test class files.
* datasketches-java-X.Y.Z-sources.jar The main source files.
* datasketches-java-X.Y.Z-test-sources.jar The test source files
* datasketches-java-X.Y.Z-javadoc.jar  The compressed Javadocs.

### Dependencies

#### Run-time
There is one run-time dependency: 

* org.apache.datasketches : datasketches-memory

#### Testing
See the pom.xml file for test dependencies.

## Special Build / Test Instructions for Eclipse

Building and running tests using JDK 8 should not be a problem. 

However, with JDK 9+, and Eclipse versions up to and including 4.22.0 (2021-12), Eclipse fails to translate the required JPMS JVM arguments specified in the POM compiler or surefire plugins into the *.classpath* file, causing illegal reflection access errors 
[eclipse-m2e/m2e-core Bug 543631](https://github.com/eclipse-m2e/m2e-core/issues/129).

There are two ways to fix this:

#### Method 1: Manually update *.classpath* file:
Open the *.classpath* file in a text editor and find the following *classpathentry* element (this assumes JDK11, change to suit):

```
	<classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER/org.eclipse.jdt.internal.debug.ui.launcher.StandardVMType/JavaSE-11">
		<attributes>
			<attribute name="module" value="true"/>
			<attribute name="maven.pomderived" value="true"/>
		</attributes>
	</classpathentry>
```
Then edit it as follows:

```
	<classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER/org.eclipse.jdt.internal.debug.ui.launcher.StandardVMType/JavaSE-11">
		<attributes>
			<attribute name="module" value="true"/>
			<attribute name="add-exports" value="java.base/jdk.internal.misc=ALL-UNNAMED:java.base/jdk.internal.ref=ALL-UNNAMED"/>
			<attribute name="add-opens" value="java.base/java.nio=ALL-UNNAMED:java.base/sun.nio.ch=ALL-UNNAMED"/>
			<attribute name="maven.pomderived" value="true"/>
		</attributes>
	</classpathentry>
```
Finally, *refresh*.

#### Method 2: Manually update *Module Dependencies*

In Eclipse, open the project *Properties / Java Build Path / Module Dependencies ...*

* Select *java.base*
* Select *Configured details*
* Select *Expose Package...*
    * Enter *Package* = java.nio
    * Enter *Target module* = ALL-UNNAMED
    * Select button: *opens*
    * Hit *OK*
* Select *Expose Package...*
    * Enter *Package* = jdk.internal.misc
    * Enter *Target module* = ALL-UNNAMED
    * Select button: *exports*
    * Hit *OK*
* Select *Expose Package...*
    * Enter *Package* = jdk.internal.ref
    * Enter *Target module* = ALL-UNNAMED
    * Select button: *exports*
    * Hit *OK*
* Select *Expose Package...*
    * Enter *Package* = sun.nio.ch
    * Enter *Target module* = ALL-UNNAMED
    * Select button: *opens*
    * Hit *OK*

**NOTE:** If you execute *Maven/Update Project...* from Eclipse with the option *Update project configuration from pom.xml* checked, all of the above will be erased, and you will have to redo it.

