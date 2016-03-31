package org.apache.sysml.api.mlcontext.matrix;

import org.apache.spark.api.java.JavaRDD;

public class JavaRDDMatrix<T> extends Matrix {

	protected JavaRDD<T> javaRDD;

	public JavaRDDMatrix(JavaRDD<T> javaRDD) {
		this.javaRDD = javaRDD;
	}

	public JavaRDDMatrix(JavaRDD<T> javaRDD, MatrixProperties matrixProperties) {
		this(javaRDD);
		this.matrixProperties = matrixProperties;
	}

	public JavaRDD<T> getJavaRDD() {
		return javaRDD;
	}

	public void setJavaRDD(JavaRDD<T> javaRDD) {
		this.javaRDD = javaRDD;
	}

}
