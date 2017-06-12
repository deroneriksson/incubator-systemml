package org.apache.sysml.api;

import org.apache.sysml.api.mlcontext.ScriptType;
import org.apache.sysml.hops.OptimizerUtils.OptimizationLevel;

public class RuntimePlatform {

	// TODO rename these modes to names that self-document and are more
	// consistent, like: DRIVER, SPARK, HADOOP, DRIVER_AND_SPARK,
	// DRIVER_AND_HADOOP
	public enum ExecutionMode {
		HADOOP, // execute all matrix operations in MR
		SINGLE_NODE, // execute all matrix operations in CP
		HYBRID, // execute matrix operations in CP or MR
		HYBRID_SPARK, // execute matrix operations in CP or Spark
		SPARK // execute matrix operations in Spark
	}

	public static ExecutionMode rtplatform = getDefaultExecutionMode();

	public static ExecutionMode getDefaultExecutionMode() {
		ExecutionMode executionMode = ExecutionMode.HYBRID;
		String sparkenv = System.getenv().get("SPARK_ENV_LOADED");
		if (sparkenv != null && sparkenv.equals("1"))
			executionMode = ExecutionMode.HYBRID_SPARK;
		return executionMode;
	}

	public static OptimizationLevel getDefaultOptimizationLevel() {
		return OptimizationLevel.O2_LOCAL_MEMORY_DEFAULT;
	}

	/**
	 * Global variable indicating the script type (DML or PYDML). Can be used
	 * for DML/PYDML-specific tasks, such as outputting booleans in the correct
	 * case (TRUE/FALSE for DML and True/False for PYDML).
	 */
	public static ScriptType scriptType = getDefaultScriptType();

	public static ScriptType getDefaultScriptType() {
		return ScriptType.DML;
	}
}