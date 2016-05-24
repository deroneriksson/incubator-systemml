---
layout: global
title: SystemML Release Checklist
description: Description of the SystemML release process and validation.
displayTitle: SystemML Release Checklist
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

* This will become a table of contents (this text will be scraped).
{:toc}


Release Candidate Version: **0.10.0-incubating-rc1**

# Release Candidate Checklist

<style type="text/css">
.graybold {background: #CCC;font-weight: bold;}
.centerbold {text-align: center;font-weight: bold;}
.bold {font-weight: bold;}
.nowrap {white-space:nowrap;}
.pass {text-align: center;background: #bfffba;font-weight: bold;}
.fail {text-align: center;background: #ff7b7b;font-weight: bold;}
.fix {text-align: center;background: #fcf883;font-weight: bold;}
</style>

<table>
  <thead>
    <tr>
      <th colspan="2">Item</th><th>Status</th><th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td colspan="2" class="nowrap"><a href="#all-artifacts-and-checksums-present">All Artifacts and Checksums Present</a></td>
      <td class="fix">Fix</td>
      <td>Not all artifacts have md5 checksums. See <a href="https://dist.apache.org/repos/dist/dev/incubator/systemml/0.10.0-incubating-rc1/">here</a>. Also, the <a href="https://repo1.maven.org/maven2/org/apache/systemml/systemml/0.9.0-incubating/">previous release</a> has sha1 files in addition to asc and md5 files.</td>
    </tr>
    <tr>
      <td rowspan="3"><a href="#release-candidate-build">Release Candidate Build</a></td>
      <td width="1%">Windows</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>OS X</td>
      <td class="pass">Pass</td>
      <td></td>
    </tr>
    <tr>
      <td>Linux</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td rowspan="3"><a href="#test-suite-passes">Test Suite Passes</a></td>
      <td width="1%">Windows</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>OS X</td>
      <td class="pass">Pass</td>
      <td>(DE will re-verify)</td>
    </tr>
    <tr>
      <td>Linux</td>
      <td class="pass">Pass</td>
      <td>All daily tests from May 18 to May 24 have passed. See <a href="https://sparktc.ibmcloud.com/jenkins/job/SystemML-DailyTest/">here</a>.</td>
    </tr>
    <tr>
      <td colspan="2"><a href="#all-binaries-execute">All Binaries Execute</a></td>
      <td class="pass">Pass</td>
      <td>Verified on OS X. (DE will re-verify)</td>
    </tr>
    <tr>
      <td colspan="2"><a href="#check-license-and-notice-files">Check LICENSE and NOTICE Files</a></td>
      <td class="pass">Pass</td>
      <td>(DE will re-verify)</td>
    </tr>
    <tr>
      <td colspan="2"><a href="#src-artifact-builds-and-tests-pass">Src Artifact Builds and Tests Pass</a></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td rowspan="3"><a href="#single-node-standalone">Single-Node Standalone</a></td>
      <td width="1%">Windows</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>OS X</td>
      <td class="pass">Pass</td>
      <td></td>
    </tr>
    <tr>
      <td>Linux</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td colspan="2"><a href="#single-node-spark">Single-Node Spark</a></td>
      <td class="pass">Pass</td>
      <td></td>
    </tr>
    <tr>
      <td colspan="2"><a href="#single-node-hadoop">Single-Node Hadoop</a></td>
      <td class="pass">Pass</td>
      <td></td>
    </tr>
    <tr>
      <td rowspan="2"><a href="#notebooks">Notebooks</a></td>
      <td width="1%">Jupyter</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>Zeppelin</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td rowspan="2"><a href="#performance-suite">Performance Suite</a></td>
      <td width="1%">Spark</td>
      <td class="pass">Pass</td>
      <td>Performance testsuite run on Spark 1.6.1 for data sizes {80MB, 800MB, 8GB, 80GB}, sparse/dense, intercept 0/1/2, and the algorithm classes binomial (Mlogreg, L2SVM, MSVM), multinomial (Mlogreg, MSVM, Naive Bayes), regression (LinregCG, LinregDS, GLM poisson-log, GLM gamma-log, GLM binomal-probit), clustering (Kmeans), and statistics (Univariate, Bivariate). The good news is that there are no compiler/runtime issues and performance is as expected.</td>
    </tr>
    <tr>
      <td>Hadoop</td>
      <td></td>
      <td></td>
    </tr>
    
  </tbody>
</table>

<table>
<thead>
<tr><th colspan="4" class="centerbold">Status Options</th></tr>
</thead>
<tbody>
<tr>
<td width="25%" class="pass">Pass</td>
<td width="25%" class="fix">Fix</td>
<td width="25%" class="fail">Fail</td>
<td width="25%">&nbsp;</td>
</tr>
</tbody>
</table>

## All Artifacts and Checksums Present

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that each expected artifact is present at [https://dist.apache.org/repos/dist/dev/incubator/systemml/](https://dist.apache.org/repos/dist/dev/incubator/systemml/) and that each artifact has accompanying
checksums (such as .asc and .md5).


## Release Candidate Build

<a href="#release-candidate-checklist">Up to Checklist</a>

The release candidate should build on Windows, OS X, and Linux. To do this cleanly,
the following procedure can be performed.

Clone the Apache SystemML GitHub repository
to an empty location. Next, check out the release tag. Following
this, build the distributions using Maven. This should be performed
with an empty local Maven repository.

Here is an example:

	$ git clone https://github.com/apache/incubator-systemml.git
	$ cd incubator-systemml
	$ git tag -l
	$ git checkout tags/0.10.0-incubating-rc1 -b 0.10.0-incubating-rc1
	$ mvn -Dmaven.repo.local=$HOME/.m2/temp-repo clean package -P distribution


## Test Suite Passes

<a href="#release-candidate-checklist">Up to Checklist</a>

The entire test suite should pass with no errors on Windows, OS X, and Linux.
The test suite can be run using:

	$ mvn clean verify


## All Binaries Execute

<a href="#release-candidate-checklist">Up to Checklist</a>

Validate that all of the binary artifacts can execute, including those artifacts packaged
in other artifacts (in the tar.gz and zip artifacts). Here is an example of doing a basic
sanity check on OS X.

	# build distribution artifacts
	mvn clean package -P distribution
	
	cd target
	
	# verify main jar works
	java -cp ./lib/*:systemml-0.10.0-incubating.jar org.apache.sysml.api.DMLScript -s "print('hello world');"
	
	# verify SystemML.jar works
	java -cp ./lib/*:SystemML.jar org.apache.sysml.api.DMLScript -s "print('hello world');"
	
	# verify standalone jar works
	java -jar systemml-0.10.0-incubating-standalone.jar -s "print('hello world');"
	
	# verify src works
	tar -xvzf systemml-0.10.0-incubating-src.tar.gz
	cd systemml-0.10.0-incubating-src
	mvn clean package -P distribution
	cd target/
	java -cp ./lib/*:systemml-0.10.0-incubating.jar org.apache.sysml.api.DMLScript -s "print('hello world');"
	java -cp ./lib/*:SystemML.jar org.apache.sysml.api.DMLScript -s "print('hello world');"
	java -jar systemml-0.10.0-incubating-standalone.jar -s "print('hello world');"
	cd ..
	cd ..
	
	# verify in-memory jar works
	echo "import org.apache.sysml.api.jmlc.*;public class JMLCEx {public static void main(String[] args) throws Exception {Connection conn = new Connection();PreparedScript script = conn.prepareScript(\"print('hello world');\", new String[]{}, new String[]{}, false);script.executeScript();}}" > JMLCEx.java
	javac -cp systemml-0.10.0-incubating-inmemory.jar JMLCEx.java
	java -cp .:systemml-0.10.0-incubating-inmemory.jar JMLCEx
	
	# verify standalone tar.gz works
	tar -xvzf systemml-0.10.0-incubating-standalone.tar.gz
	cd systemml-0.10.0-incubating-standalone
	echo "print('hello world');" > hello.dml
	./runStandaloneSystemML.sh hello.dml
	cd ..
	
	# verify distrib tar.gz works
	tar -xvzf systemml-0.10.0-incubating.tar.gz
	cd systemml-0.10.0-incubating
	java -cp ../lib/*:SystemML.jar org.apache.sysml.api.DMLScript -s "print('hello world');"
	
	# verify spark batch mode
	export SPARK_HOME=/Users/deroneriksson/spark-1.5.1-bin-hadoop2.6
	$SPARK_HOME/bin/spark-submit SystemML.jar -s "print('hello world');" -exec hybrid_spark
	
	# verify hadoop batch mode
	hadoop jar SystemML.jar -s "print('hello world');"


## Check LICENSE and NOTICE Files

<a href="#release-candidate-checklist">Up to Checklist</a>

Each artifact *must* contain LICENSE and NOTICE files. These files must reflect the
contents of the artifacts. If the project dependencies (ie, libraries) have changed
since the last release, the LICENSE and NOTICE files must be updated to reflect these
changes.

Each artifact *should* contain a DISCLAIMER file.

For more information, see:

1. <http://incubator.apache.org/guides/releasemanagement.html>
2. <http://www.apache.org/dev/licensing-howto.html>


## Src Artifact Builds and Tests Pass

<a href="#release-candidate-checklist">Up to Checklist</a>

The project should be built using the `src` (tar.gz and zip) artifacts.
In addition, the test suite should be run using an `src` artifact and
all tests should pass.


## Single-Node Standalone

<a href="#release-candidate-checklist">Up to Checklist</a>

The standalone tar.gz and zip artifacts contain `runStandaloneSystemML.sh` and `runStandaloneSystemML.bat`
files. Verify that one or more algorithms can be run on a single node using these
standalone distributions.

Here is an example based on the [Quick Start Guide](http://apache.github.io/incubator-systemml/quick-start-guide.html)
demonstrating the execution of an algorithm (on OS X).

	$ tar -xvzf systemml-0.10.0-incubating-standalone.tar.gz
	$ cd systemml-0.10.0-incubating-standalone
	$ wget -P data/ http://archive.ics.uci.edu/ml/machine-learning-databases/haberman/haberman.data
	$ echo '{"rows": 306, "cols": 4, "format": "csv"}' > data/haberman.data.mtd
	$ echo '1,1,1,2' > data/types.csv
	$ echo '{"rows": 1, "cols": 4, "format": "csv"}' > data/types.csv.mtd
	$ ./runStandaloneSystemML.sh scripts/algorithms/Univar-Stats.dml -nvargs X=data/haberman.data TYPES=data/types.csv STATS=data/univarOut.mtx CONSOLE_OUTPUT=TRUE


## Single-Node Spark

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that SystemML runs algorithms on Spark locally.

Here is an example of running the `Univar-Stats.dml` algorithm on random generated data.

	$ tar -xvzf systemml-0.10.0-incubating.tar.gz
	$ cd systemml-0.10.0-incubating
	$ export SPARK_HOME=/Users/deroneriksson/spark-1.5.1-bin-hadoop2.6
	$ $SPARK_HOME/bin/spark-submit SystemML.jar -f scripts/datagen/genRandData4Univariate.dml -exec hybrid_spark -args 1000000 100 10 1 2 3 4 uni.mtx
	$ echo '1' > uni-types.csv
	$ echo '{"rows": 1, "cols": 1, "format": "csv"}' > uni-types.csv.mtd
	$ $SPARK_HOME/bin/spark-submit SystemML.jar -f scripts/algorithms/Univar-Stats.dml -exec hybrid_spark -nvargs X=uni.mtx TYPES=uni-types.csv STATS=uni-stats.txt CONSOLE_OUTPUT=TRUE


## Single-Node Hadoop

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that SystemML runs algorithms on Hadoop locally.

Based on the "Single-Node Spark" setup above, the `Univar-Stats.dml` algorithm could be run as follows:

	$ hadoop jar SystemML.jar -f scripts/algorithms/Univar-Stats.dml -nvargs X=uni.mtx TYPES=uni-types.csv STATS=uni-stats.txt CONSOLE_OUTPUT=TRUE


## Notebooks

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that SystemML can be executed from Jupyter and Zeppelin notebooks.
For examples, see the [Spark MLContext Programming Guide](http://apache.github.io/incubator-systemml/spark-mlcontext-programming-guide.html).


## Performance Suite

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that the performance suite located at scripts/perftest/ executes on Spark and Hadoop. Testing should
include 80MB, 800MB, 8GB, and 80GB data sizes.




