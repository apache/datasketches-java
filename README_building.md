

## Building sketches-core
Use Apache Maven 3.0 to build this project

### Requires JDK8
DataSketches sketches-core leverages new API methods introduced with JDK8. 
JDK7 is no longer supported.


### Basic Build
    mvn clean install

### Build including source and javadoc jars
    mvn -P release-profile clean install

#### Mac install
To install you will need GnuPG which is not configured by default on Macs.

    brew install gnupg
    gpg --gen-key

Then, follow instructions to create keys.

To install Homebrew
    
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

### Release build, incrementing the version number and publishing to maven central repository

#### Prepare a release
First, "prepare" a release. Preparing a release ensures you have no checked out file and no SNAPSHOT dependencies.
It changes the version in all pom.xml files to the release version. It then runs all the tests. If everything passes,
the release version is committed to the SCM system and tagged. The version number is then updated in all the pom.xml 
files again, this time to the next development version. Finally, the changes are commited again to the SCM repository.

    mvn release:prepare

1. ensure you have **no** uncommitted files in your workspace. Ideally, use a fresh workspace for the release process.
2. Answer the prompts with appropriate values for the tag, version, and new development version
    - release version should be something like 1.0.1
    - development version should be something like 1.0.2-SNAPSHOT
    - tag should be something like sketches-core-1.0.1
3. If the prepare has an error (the build will fail if any tests fail), run 'mvn release:clean' to clean up the 
generated files and reset the workspace. If the prepare needs to be rolled back (undone in the SCM system), run 'mvn release:rollback'.

See https://maven.apache.org/maven-release/maven-release-plugin/examples/prepare-release.html for more info.

#### Perform a release:
Next, "perform" a release. Performing a release checks out the tagged release from the SCM system into a new directory,
builds it, and then runs the configured deploy goals to publish the release to a maven repository (Maven central). Performing
a release automatically enables the `release-profile` in maven, causing source and javadoc jars to be automatically created 
and published.

    mvn release:perform

You can add the '-DdryRun=true' option to test the release process without actually generating a release. dryRun is 
supported on both the prepare and perform steps.

See https://maven.apache.org/maven-release/maven-release-plugin/examples/perform-release.html for more info.

#### Rollback a release
If a release has been prepared but not performed yet, the release can be rolled back. Rolling back a release resets
the pom.xml version numbers and tags in the SCM system, effectively nullifying the release.

    mvn release:rollback

A release cannot be rolled back if it has already been cleaned up (the release.properties file and other metadata removed).

#### Clean a release
If a release fails to prepare or crashs, it can leave various files behind. These can be cleaned up with a release clean.

    mvn release:clean

