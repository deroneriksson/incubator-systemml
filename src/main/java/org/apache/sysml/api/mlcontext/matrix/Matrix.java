package org.apache.sysml.api.mlcontext.matrix;

import org.apache.sysml.runtime.matrix.MatrixCharacteristics;

public abstract class Matrix {

	protected MatrixProperties matrixProperties = new MatrixProperties();
	protected MatrixCharacteristics matrixCharacteristics = new MatrixCharacteristics();
	
	public MatrixCharacteristics getMatrixCharacteristics() {
		return matrixCharacteristics;
	}

	public void setMatrixCharacteristics(MatrixCharacteristics matrixCharacteristics) {
		this.matrixCharacteristics = matrixCharacteristics;
	}

	public MatrixProperties getMatrixProperties() {
		return matrixProperties;
	}

	public void setMatrixProperties(MatrixProperties matrixProperties) {
		this.matrixProperties = matrixProperties;
	}

//	public Long getNumRows() {
//		return matrixProperties.getNumRows();
//	}
//
//	public Matrix setNumRows(Long numRows) {
//		matrixProperties.setNumRows(numRows);
//		return this;
//	}
//
//	public Long getNumColumns() {
//		return matrixProperties.getNumColumns();
//	}
//
//	public Matrix setNumColumns(Long numColumns) {
//		matrixProperties.setNumColumns(numColumns);
//		return this;
//	}
//
//	public Integer getNumRowsPerBlock() {
//		return matrixProperties.getNumRowsPerBlock();
//	}
//
//	public Matrix setNumRowsPerBlock(Integer numRowsPerBlock) {
//		matrixProperties.setNumRowsPerBlock(numRowsPerBlock);
//		return this;
//	}
//
//	public Integer getNumColumnsPerBlock() {
//		return matrixProperties.getNumColumnsPerBlock();
//	}
//
//	public Matrix setNumColumnsPerBlock(Integer numColumnsPerBlock) {
//		matrixProperties.setNumColumnsPerBlock(numColumnsPerBlock);
//		return this;
//	}
//
//	public Long getNumNonZeros() {
//		return matrixProperties.getNumNonZeros();
//	}
//
//	public Matrix setNumNonZeros(Long numNonZeros) {
//		matrixProperties.setNumNonZeros(numNonZeros);
//		return this;
//	}
//
//	public Boolean getHeader() {
//		return matrixProperties.getHeader();
//	}
//
//	public Matrix setHeader(Boolean header) {
//		matrixProperties.setHeader(header);
//		return this;
//	}
//
//	public String getDelimiter() {
//		return matrixProperties.getDelimiter();
//	}
//
//	public Matrix setDelimiter(String delimiter) {
//		matrixProperties.setDelimiter(delimiter);
//		return this;
//	}

}
