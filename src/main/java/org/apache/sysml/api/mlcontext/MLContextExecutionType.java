package org.apache.sysml.api.mlcontext;

/**
 * SystemML can be executed with Spark either on Spark or in Spark/Single Node mode.
 *
 */
public enum MLContextExecutionType {
	SPARK_ONLY, SPARK_OR_SINGLENODE
}
