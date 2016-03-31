package org.apache.sysml.api.mlcontext.matrix;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.sysml.api.mlcontext.MLContextException;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class BinaryBlockMatrix extends Matrix {

	protected MatrixObject matrixObject;
	protected ExecutionContext executionContext;

	public BinaryBlockMatrix(MatrixObject matrixObject, ExecutionContext executionContext) {
		this.matrixObject = matrixObject;
		this.executionContext = executionContext;
		this.matrixCharacteristics = matrixObject.getMatrixCharacteristics();
	}

	public MatrixObject getMatrixObject() {
		return matrixObject;
	}

	public void setMatrixObject(MatrixObject matrixObject) {
		this.matrixObject = matrixObject;
	}

	public JavaRDD<String> stringRDD() {
		return stringRDD(IOFormat.IJV);
	}

	public JavaRDD<String> stringRDD(IOFormat ioFormat) {
		SparkExecutionContext sparkExecutionContext = (SparkExecutionContext) executionContext;

		if (ioFormat == IOFormat.IJV) {
			try {
				@SuppressWarnings("unchecked")
				JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockRDDMatrix = (JavaPairRDD<MatrixIndexes, MatrixBlock>) sparkExecutionContext
						.getRDDHandleForMatrixObject(matrixObject, InputInfo.BinaryBlockInputInfo);
				MatrixCharacteristics matrixCharacteristics = matrixObject.getMatrixCharacteristics();
				JavaRDD<String> stringRDD = RDDConverterUtilsExt.binaryBlockToStringRDD(binaryBlockRDDMatrix,
						matrixCharacteristics, "text");
				return stringRDD;
			} catch (DMLRuntimeException e) {
				throw new MLContextException("Exception while converting matrix to String RDD", e);
			} catch (DMLUnsupportedOperationException e) {
				throw new MLContextException("Exception while converting matrix to String RDD", e);
			}
		} else {
			throw new MLContextException("The output format:" + ioFormat + " is not currently supported.");
		}

	}

	public JavaRDDMatrix<String> javaRDDMatrix() {
		try {
			SparkExecutionContext sparkExecutionContext = (SparkExecutionContext) executionContext;
			@SuppressWarnings("unchecked")
			JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockRDDMatrix = (JavaPairRDD<MatrixIndexes, MatrixBlock>) sparkExecutionContext
					.getRDDHandleForMatrixObject(matrixObject, InputInfo.BinaryBlockInputInfo);
			MatrixCharacteristics matrixCharacteristics = matrixObject.getMatrixCharacteristics();
			JavaRDD<String> stringRDD = RDDConverterUtilsExt.binaryBlockToStringRDD(binaryBlockRDDMatrix,
					matrixCharacteristics, "text");
			JavaRDDMatrix<String> javaRDDMatrix = new JavaRDDMatrix<String>(stringRDD);
			javaRDDMatrix.setMatrixCharacteristics(matrixCharacteristics);
			return javaRDDMatrix;
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception converting MatrixObject to JavaRDDMatrix", e);
		} catch (DMLUnsupportedOperationException e) {
			throw new MLContextException("Exception converting MatrixObject to JavaRDDMatrix", e);
		}
	}

}
