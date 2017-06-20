/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.test.integration.mlcontext;

import static org.apache.sysml.api.mlcontext.ScriptFactory.dmlFromFile;

import java.io.File;

import org.apache.spark.sql.SparkSession;
import org.apache.sysml.api.mlcontext.MLContext;
import org.apache.sysml.api.mlcontext.Matrix;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.utils.TestUtils;
import org.apache.sysml.utils.ExecutionMode;
import org.apache.sysml.utils.GlobalState;
import org.junit.After;
import org.junit.Test;


public class MLContextMultipleScriptsTest extends AutomatedTestBase 
{
	private final static String TEST_DIR = "org/apache/sysml/api/mlcontext";
	private final static String TEST_NAME = "MLContextMultiScript";

	private final static int rows = 1123;
	private final static int cols = 789;
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_DIR, TEST_NAME);
		getAndLoadTestConfiguration(TEST_NAME);
	}

	@Test
	public void testMLContextMultipleScriptsCP() {
		runMLContextTestMultipleScript(ExecutionMode.SINGLE_NODE, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsHybrid() {
		runMLContextTestMultipleScript(ExecutionMode.HYBRID_SPARK, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsSpark() {
		runMLContextTestMultipleScript(ExecutionMode.SPARK, false);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadCP() {
		runMLContextTestMultipleScript(ExecutionMode.SINGLE_NODE, true);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadHybrid() {
		runMLContextTestMultipleScript(ExecutionMode.HYBRID_SPARK, true);
	}
	
	@Test
	public void testMLContextMultipleScriptsWithReadSpark() {
		runMLContextTestMultipleScript(ExecutionMode.SPARK, true);
	}

	/**
	 * 
	 * @param platform
	 */
	private void runMLContextTestMultipleScript(ExecutionMode platform, boolean wRead) 
	{
		ExecutionMode oldplatform = GlobalState.rtplatform;
		GlobalState.rtplatform = platform;
		
		//create mlcontext
		SparkSession spark = createSystemMLSparkSession("MLContextMultipleScriptsTest", "local");
		MLContext ml = new MLContext(spark);
		ml.setExplain(true);

		String dml1 = baseDirectory + File.separator + "MultiScript1.dml";
		String dml2 = baseDirectory + File.separator + (wRead?"MultiScript2b.dml":"MultiScript2.dml");
		String dml3 = baseDirectory + File.separator + (wRead?"MultiScript3b.dml":"MultiScript3.dml");
		
		try
		{
			//run script 1
			Script script1 = dmlFromFile(dml1).in("$rows", rows).in("$cols", cols).out("X");
			Matrix X = ml.execute(script1).getMatrix("X");
			
			Script script2 = dmlFromFile(dml2).in("X", X).out("Y");
			Matrix Y = ml.execute(script2).getMatrix("Y");
			
			Script script3 = dmlFromFile(dml3).in("X", X).in("Y",Y).out("z");
			String z = ml.execute(script3).getString("z");
			
			System.out.println(z);
		}
		finally {
			GlobalState.rtplatform = oldplatform;
			
			// stop underlying spark context to allow single jvm tests (otherwise the
			// next test that tries to create a SparkContext would fail)
			spark.stop();
			// clear status mlcontext and spark exec context
			ml.close();
		}
	}

	@After
	public void tearDown() {
		super.tearDown();
	}
}
