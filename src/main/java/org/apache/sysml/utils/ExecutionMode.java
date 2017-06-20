package org.apache.sysml.utils;

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