package org.apache.sysml.utils;

import org.apache.sysml.api.mlcontext.ScriptType;
import org.apache.sysml.hops.OptimizerUtils.OptimizationLevel;
import org.apache.sysml.runtime.controlprogram.parfor.util.IDHandler;

/**
 * Manages global state information and related functionality in SystemML.
 *
 */
public class GlobalState {

	/**
	 * If true, app master is active.
	 */
	public static boolean activeAM = false;

	/**
	 * If true, enable debug mode.
	 */
	public static boolean enableDebugMode = false;

	/**
	 * If true, force accelerator.
	 */
	public static boolean forceAccelerator = false;

	/**
	 * The execution mode in which to run SystemML.
	 */
	public static ExecutionMode rtplatform = getDefaultExecutionMode();

	/**
	 * Global variable indicating the script type (DML or PYDML). Can be used
	 * for DML/PYDML-specific tasks, such as outputting booleans in the correct
	 * case (TRUE/FALSE for DML and True/False for PYDML).
	 */
	public static ScriptType scriptType = getDefaultScriptType();

	/**
	 * If true, gather and output statistics.
	 */
	public static boolean statistics = false;

	/**
	 * Global constant to control whether or not to suppress printing to
	 * standard output.
	 */
	public static final boolean SUPPRESS_PRINT_TO_STDOUT = false;

	/**
	 * If true, use accelerator.
	 */
	public static boolean useAccelerator = false;

	/**
	 * Unique distributed identifier.
	 */
	public static String uuid = IDHandler.createDistributedUniqueID();

	/**
	 * If true, allow DMLProgram to be generated while not halting due to
	 * validation errors/warnings.
	 */
	public static boolean validatorIgnoreIssues = false;

	/**
	 * Obtain the default execution mode (either CP+Hadoop or CP+Spark).
	 * 
	 * @return the default execution mode
	 */
	public static ExecutionMode getDefaultExecutionMode() {
		ExecutionMode executionMode = ExecutionMode.HYBRID;
		String sparkenv = System.getenv().get("SPARK_ENV_LOADED");
		if (sparkenv != null && sparkenv.equals("1"))
			executionMode = ExecutionMode.HYBRID_SPARK;
		return executionMode;
	}

	/**
	 * Obtain the default optimization level.
	 * 
	 * @return the default optimization level
	 */
	public static OptimizationLevel getDefaultOptimizationLevel() {
		return OptimizationLevel.O2_LOCAL_MEMORY_DEFAULT;
	}

	/**
	 * Obtain the default script type.
	 * 
	 * @return the default script type
	 */
	public static ScriptType getDefaultScriptType() {
		return ScriptType.DML;
	}
}
