package org.apache.sysml.api.mlcontext.matrix;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class BinaryBlockRDDMatrix extends Matrix {

	protected IOFormat ioFormat;
	protected JavaPairRDD<MatrixIndexes, MatrixBlock> javaPairRdd;

	public IOFormat getIoFormat() {
		return ioFormat;
	}

	public void setIoFormat(IOFormat ioFormat) {
		this.ioFormat = ioFormat;
	}

	public JavaPairRDD<MatrixIndexes, MatrixBlock> getJavaPairRdd() {
		return javaPairRdd;
	}

	public void setJavaPairRdd(JavaPairRDD<MatrixIndexes, MatrixBlock> javaPairRdd) {
		this.javaPairRdd = javaPairRdd;
	}

}
