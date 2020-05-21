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

[![Build Status](https://travis-ci.org/apache/incubator-datasketches-java.svg?branch=master)](https://travis-ci.org/apache/incubator-datasketches-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.datasketches/datasketches-java)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/incubator-datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/incubator-datasketches-java/context:java)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/apache/incubator-datasketches-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/incubator-datasketches-java/alerts/)
[![Coverage Status](https://coveralls.io/repos/github/apache/incubator-datasketches-java/badge.svg?branch=master&service=github)](https://coveralls.io/github/apache/incubator-datasketches-java?branch=master)

=================

# DataSketches Core Java Library Component
This is the core Java component of the DataSketches library.  It contains all of the sketching algorithms and can be accessed directly from user applications. 

This component is also a dependency of other components of the library that create adaptors for target systems, such as Hadoop Pig and Hadoop Hive.

Note that we have a parallel core component for C++ and Python implementations of the same sketch algorithms, 
[incubator-datasketches-cpp](https://github.com/apache/incubator-datasketches-cpp).

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

---

## Build Instructions
__NOTE:__ This component accesses resource files for testing. As a result, the directory elements of the full absolute path of the target installation directory must qualify as Java identifiers. In other words, the directory elements must not have any space characters (or non-Java identifier characters) in any of the path elements. This is required by the Oracle Java Specification in order to ensure location-independent access to resources: [See Oracle Location-Independent Access to Resources](https://docs.oracle.com/javase/8/docs/technotes/guides/lang/resources.html)

### JDK8 is required to compile
This DataSketches component is pure Java and you must compile using JDK 8.

### Recommended Build Tool
This DataSketches component is structured as a Maven project and Maven is the recommended Build Tool.

There are two types of tests: normal unit tests and tests run by the strict profile.  

To run normal unit tests:

    $ mvn clean test

To run the strict profile tests:

    $ mvn clean test -P strict

To install jars built from the downloaded source:

    $ mvn clean install -DskipTests=true

This will create the following jars:

* datasketches-java-X.Y.Z-incubating.jar The compiled main class files.
* datasketches-java-X.Y.Z-incubating-tests.jar The compiled test class files.
* datasketches-java-X.Y.Z-incubating-sources.jar The main source files.
* datasketches-java-X.Y.Z-incubating-test-sources.jar The test source files
* datasketches-java-X.Y.Z-incubating-javadoc.jar  The compressed Javadocs.

### Dependencies

#### Run-time
There is one run-time dependency: 

* org.apache.datasketches : datasketches-memory

#### Testing
See the pom.xml file for test dependencies.

----

Disclaimer: Apache DataSketches is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
