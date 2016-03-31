package org.apache.sysml.api.mlcontext.matrix;

import org.apache.spark.rdd.RDD;

public class RDDMatrix<T> extends Matrix {

	protected RDD<T> rdd;

	public RDDMatrix(RDD<T> rdd) {
		this.rdd = rdd;
	}

	public RDDMatrix(RDD<T> rdd, MatrixProperties matrixProperties) {
		this(rdd);
		this.matrixProperties = matrixProperties;
	}

	public RDD<T> getRdd() {
		return rdd;
	}

	public void setRdd(RDD<T> rdd) {
		this.rdd = rdd;
	}
}
