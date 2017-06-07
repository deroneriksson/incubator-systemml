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

package org.apache.sysml.test.integration.functions.transform;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.RuntimePlatform.ExecutionMode;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.runtime.io.ReaderBinaryBlock;
import org.apache.sysml.runtime.io.ReaderTextCSV;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.test.integration.TestConfiguration;
import org.apache.sysml.test.utils.TestUtils;

public class TransformTest extends AutomatedTestBase 
{
	
	private final static String TEST_NAME1 = "Transform";
	private final static String TEST_NAME2 = "Apply";
	private final static String TEST_DIR = "functions/transform/";
	private final static String TEST_CLASS_DIR = TEST_DIR + TransformTest.class.getSimpleName() + "/";
	
	private final static String HOMES_DATASET 	= "homes/homes.csv";
	private final static String HOMES_SPEC 		= "homes/homes.tfspec.json";
	private final static String HOMES_IDSPEC 	= "homes/homes.tfidspec.json";
	private final static String HOMES_TFDATA 	= "homes/homes.transformed.csv";
	
	private final static String HOMES_OMIT_DATASET 	= "homes/homes.csv";
	private final static String HOMES_OMIT_SPEC 	= "homes/homesOmit.tfspec.json";
	private final static String HOMES_OMIT_IDSPEC 	= "homes/homesOmit.tfidspec.json";
	private final static String HOMES_OMIT_TFDATA 	= "homes/homesOmit.transformed.csv";
	
	// Homes data set in two parts
	private final static String HOMES2_DATASET 	= "homes2/homes.csv";
	private final static String HOMES2_SPEC 	= "homes2/homes.tfspec.json";
	private final static String HOMES2_IDSPEC 	= "homes2/homes.tfidspec.json";
	private final static String HOMES2_TFDATA 	= "homes/homes.transformed.csv"; // same as HOMES_TFDATA
	
	private final static String IRIS_DATASET 	= "iris/iris.csv";
	private final static String IRIS_SPEC 		= "iris/iris.tfspec.json";
	private final static String IRIS_IDSPEC 	= "iris/iris.tfidspec.json";
	private final static String IRIS_TFDATA 	= "iris/iris.transformed.csv";
	
	@Override
	public void setUp() 
	{
		addTestConfiguration(TEST_NAME1, 
			new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] { "y" }) );
	}
	
	// ---- Iris CSV ----
	
	@Test
	public void testIrisHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "iris", false);
	}
	
	@Test
	public void testIrisSingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "iris", false);
	}
	
	@Test
	public void testIrisSPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "iris", false);
	}
	
	@Test
	public void testIrisHadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "iris", false);
	}

	@Test
	public void testIrisSparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "iris", false);
	}

	// ---- Iris BinaryBlock ----
	
	@Test
	public void testIrisHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "iris", false);
	}
	
	@Test
	public void testIrisSingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "iris", false);
	}
	
	@Test
	public void testIrisSPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "iris", false);
	}
	
	@Test
	public void testIrisHadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "iris", false);
	}
	
	@Test
	public void testIrisSparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "iris", false);
	}
	
	// ---- Homes CSV ----
	
	@Test
	public void testHomesHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homes", false);
	}
	
	@Test
	public void testHomesSingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homes", false);
	}
	
	@Test
	public void testHomesHadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homes", false);
	}

	@Test
	public void testHomesSPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homes", false);
	}

	@Test
	public void testHomesSparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homes", false);
	}

	// ---- Homes BinaryBlock ----
	
	@Test
	public void testHomesHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homes", false);
	}
	
	@Test
	public void testHomesSingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homes", false);
	}
	
	@Test
	public void testHomesHadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homes", false);
	}
	
	@Test
	public void testHomesSPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homes", false);
	}
	
	@Test
	public void testHomesSparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homes", false);
	}
	
	// ---- OmitHomes CSV ----
	
	@Test
	public void testOmitHomesHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesSingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesHadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homesomit", false);
	}

	@Test
	public void testOmitHomesSPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homesomit", false);
	}

	@Test
	public void testOmitHomesSparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homesomit", false);
	}

	// ---- OmitHomes BinaryBlock ----
	
	@Test
	public void testOmitHomesHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesSingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesHadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesSPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homesomit", false);
	}
	
	@Test
	public void testOmitHomesSparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homesomit", false);
	}
	
	// ---- Homes2 CSV ----
	
	@Test
	public void testHomes2HybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homes2", false);
	}
	
	@Test
	public void testHomes2SingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homes2", false);
	}
	
	@Test
	public void testHomes2HadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homes2", false);
	}

	@Test
	public void testHomes2SPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homes2", false);
	}

	@Test
	public void testHomes2SparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homes2", false);
	}

	// ---- Homes2 BinaryBlock ----
	
	@Test
	public void testHomes2HybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homes2", false);
	}
	
	@Test
	public void testHomes2SingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homes2", false);
	}
	
	@Test
	public void testHomes2HadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homes2", false);
	}
	
	@Test
	public void testHomes2SPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homes2", false);
	}
	
	@Test
	public void testHomes2SparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homes2", false);
	}
	
	// ---- Iris ID CSV ----
	
	@Test
	public void testIrisHybridIDCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "iris", true);
	}
	
	@Test
	public void testIrisSingleNodeIDCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "iris", true);
	}
	
	@Test
	public void testIrisSPHybridIDCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "iris", true);
	}
	
	@Test
	public void testIrisHadoopIDCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "iris", true);
	}

	@Test
	public void testIrisSparkIDCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "iris", true);
	}

	// ---- Iris ID BinaryBlock ----
	
	@Test
	public void testIrisHybridIDBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "iris", true);
	}
	
	@Test
	public void testIrisSingleNodeIDBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "iris", true);
	}
	
	@Test
	public void testIrisSPHybridIDBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "iris", true);
	}
	
	@Test
	public void testIrisHadoopIDBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "iris", true);
	}
	
	@Test
	public void testIrisSparkIDBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "iris", true);
	}
	
	// ---- Homes ID CSV ----
	
	@Test
	public void testHomesHybridIDCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homes", true);
	}
	
	@Test
	public void testHomesSingleNodeIDCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homes", true);
	}
	
	@Test
	public void testHomesSPHybridIDCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homes", true);
	}
	
	@Test
	public void testHomesHadoopIDCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homes", true);
	}

	@Test
	public void testHomesSparkIDCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homes", true);
	}

	// ---- Homes ID BinaryBlock ----
	
	@Test
	public void testHomesHybridIDBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homes", true);
	}
	
	@Test
	public void testHomesSingleNodeIDBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homes", true);
	}
	
	@Test
	public void testHomesSPHybridIDBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homes", true);
	}
	
	@Test
	public void testHomesHadoopIDBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homes", true);
	}
	
	@Test
	public void testHomesSparkIDBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homes", true);
	}
	
	// ---- OmitHomes CSV ----
	
	@Test
	public void testOmitHomesIDHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDSingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDHadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homesomit", true);
	}

	@Test
	public void testOmitHomesIDSPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homesomit", true);
	}

	@Test
	public void testOmitHomesIDSparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homesomit", true);
	}

	// ---- OmitHomes BinaryBlock ----
	
	@Test
	public void testOmitHomesIDHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDSingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDHadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDSPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homesomit", true);
	}
	
	@Test
	public void testOmitHomesIDSparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homes2", true);
	}
	
	// ---- Homes2 CSV ----
	
	@Test
	public void testHomes2IDHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID, "csv", "homes2", true);
	}
	
	@Test
	public void testHomes2IDSingleNodeCSV() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "csv", "homes2", true);
	}
	
	@Test
	public void testHomes2IDHadoopCSV() 
	{
		runTransformTest(ExecutionMode.HADOOP, "csv", "homes2", true);
	}

	@Test
	public void testHomes2IDSPHybridCSV() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "csv", "homes2", true);
	}

	@Test
	public void testHomes2IDSparkCSV() 
	{
		runTransformTest(ExecutionMode.SPARK, "csv", "homes2", true);
	}

	// ---- Homes2 BinaryBlock ----
	
	@Test
	public void testHomes2IDHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID, "binary", "homes2", true);
	}
	
	@Test
	public void testHomes2IDSingleNodeBB() 
	{
		runTransformTest(ExecutionMode.SINGLE_NODE, "binary", "homes2", true);
	}
	
	@Test
	public void testHomes2IDHadoopBB() 
	{
		runTransformTest(ExecutionMode.HADOOP, "binary", "homes2", true);
	}
	
	@Test
	public void testHomes2IDSPHybridBB() 
	{
		runTransformTest(ExecutionMode.HYBRID_SPARK, "binary", "homes2", true);
	}
	
	@Test
	public void testHomes2IDSparkBB() 
	{
		runTransformTest(ExecutionMode.SPARK, "binary", "homes2", true);
	}
	
	// ------------------------------
	
	private void runTransformTest( ExecutionMode rt, String ofmt, String dataset, boolean byid )
	{
		String DATASET = null, SPEC=null, TFDATA=null;
		
		if(dataset.equals("homes"))
		{
			DATASET = HOMES_DATASET;
			SPEC = (byid ? HOMES_IDSPEC : HOMES_SPEC);
			TFDATA = HOMES_TFDATA;
		}
		else if(dataset.equals("homesomit"))
		{
			DATASET = HOMES_OMIT_DATASET;
			SPEC = (byid ? HOMES_OMIT_IDSPEC : HOMES_OMIT_SPEC);
			TFDATA = HOMES_OMIT_TFDATA;
		}

		else if(dataset.equals("homes2"))
		{
			DATASET = HOMES2_DATASET;
			SPEC = (byid ? HOMES2_IDSPEC : HOMES2_SPEC);
			TFDATA = HOMES2_TFDATA;
		}
		else if (dataset.equals("iris"))
		{
			DATASET = IRIS_DATASET;
			SPEC = (byid ? IRIS_IDSPEC : IRIS_SPEC);
			TFDATA = IRIS_TFDATA;
		}

		ExecutionMode rtold = rtplatform;
		rtplatform = rt;

		boolean sparkConfigOld = DMLScript.USE_LOCAL_SPARK_CONFIG;
		if( rtplatform == ExecutionMode.SPARK || rtplatform == ExecutionMode.HYBRID_SPARK)
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;

		try
		{
			getAndLoadTestConfiguration(TEST_NAME1);
			
			/* This is for running the junit test the new way, i.e., construct the arguments directly */
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME1 + ".dml";
			programArgs = new String[]{"-nvargs", 
				"DATA=" + HOME + "input/" + DATASET,
				"TFSPEC=" + HOME + "input/" + SPEC,
				"TFMTD=" + output("tfmtd"),
				"TFDATA=" + output("tfout"),
				"OFMT=" + ofmt };
	
			boolean exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
			
			fullDMLScriptName = HOME + TEST_NAME2 + ".dml";
			programArgs = new String[]{"-nvargs", 
				"DATA=" + HOME + "input/" + DATASET,
				"APPLYMTD=" + output("tfmtd"),  // generated above
				"TFMTD=" + output("test_tfmtd"),
				"TFDATA=" + output("test_tfout"),
				"OFMT=" + ofmt };
	
			exceptionExpected = false;
			runTest(true, exceptionExpected, null, -1); 
			
			try {
				ReaderTextCSV csvReader=  new ReaderTextCSV(new CSVFileFormatProperties(true, ",", true, 0, null));
				MatrixBlock exp = csvReader.readMatrixFromHDFS(HOME+"input/"+ TFDATA, -1, -1, -1, -1, -1);
				
				MatrixBlock out = null, out2=null;
				if(ofmt.equals("csv"))
				{
					ReaderTextCSV outReader=  new ReaderTextCSV(new CSVFileFormatProperties(false, ",", true, 0, null));
					out = outReader.readMatrixFromHDFS(output("tfout"), -1, -1, -1, -1, -1);
					out2 = outReader.readMatrixFromHDFS(output("test_tfout"), -1, -1, -1, -1, -1);
				}
				else
				{
					ReaderBinaryBlock bbReader = new ReaderBinaryBlock(false);
					out = bbReader.readMatrixFromHDFS(
							output("tfout"), exp.getNumRows(), exp.getNumColumns(), 
							ConfigurationManager.getBlocksize(), 
							ConfigurationManager.getBlocksize(),
							-1);
					out2 = bbReader.readMatrixFromHDFS(
							output("test_tfout"), exp.getNumRows(), exp.getNumColumns(), 
							ConfigurationManager.getBlocksize(), 
							ConfigurationManager.getBlocksize(),
							-1);
				}
				
				assertTrue("Incorrect output from data transform.", equals(out,exp,  1e-10));
				assertTrue("Incorrect output from apply transform.", equals(out2,exp,  1e-10));
					
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		finally
		{
			rtplatform = rtold;
			DMLScript.USE_LOCAL_SPARK_CONFIG = sparkConfigOld;
		}
	}
	
	public static boolean equals(MatrixBlock mb1, MatrixBlock mb2, double epsilon)
	{
		if(mb1.getNumRows() != mb2.getNumRows() || mb1.getNumColumns() != mb2.getNumColumns() || mb1.getNonZeros() != mb2.getNonZeros() )
			return false;
		
		// TODO: this implementation is to be optimized for different block representations
		for(int i=0; i < mb1.getNumRows(); i++) 
			for(int j=0; j < mb1.getNumColumns(); j++ )
				if(!TestUtils.compareCellValue(mb1.getValue(i, j), mb2.getValue(i,j), epsilon, false))
				{
					System.err.println("(i="+ (i+1) + ",j=" + (j+1) + ")  " + mb1.getValue(i, j) + " != " + mb2.getValue(i, j));
					return false;
				}
		
		return true;
	}
	
}