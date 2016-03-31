package org.apache.sysml.api.mlcontext.matrix;

public class MatrixProperties {

	protected Long numRows;
	protected Long numColumns;
	protected Integer numRowsPerBlock = 1;
	protected Integer numColumnsPerBlock = 1;
	protected Long numNonZeros;
	protected Boolean header;
	protected String delimiter = ",";
//	protected IOFormat ioFormat;

	/**
	 * Constructor.
	 */
	public MatrixProperties() {
	}

	public Long getNumRows() {
		return numRows;
	}

	public MatrixProperties setNumRows(Long numRows) {
		this.numRows = numRows;
		return this;
	}

	public Long getNumColumns() {
		return numColumns;
	}

	public MatrixProperties setNumColumns(Long numColumns) {
		this.numColumns = numColumns;
		return this;
	}

	public Integer getNumRowsPerBlock() {
		return numRowsPerBlock;
	}

	public MatrixProperties setNumRowsPerBlock(Integer numRowsPerBlock) {
		this.numRowsPerBlock = numRowsPerBlock;
		return this;
	}

	public Integer getNumColumnsPerBlock() {
		return numColumnsPerBlock;
	}

	public MatrixProperties setNumColumnsPerBlock(Integer numColumnsPerBlock) {
		this.numColumnsPerBlock = numColumnsPerBlock;
		return this;
	}

	public Long getNumNonZeros() {
		return numNonZeros;
	}

	public MatrixProperties setNumNonZeros(Long numNonZeros) {
		this.numNonZeros = numNonZeros;
		return this;
	}

	public Boolean getHeader() {
		return header;
	}

	public MatrixProperties setHeader(Boolean header) {
		this.header = header;
		return this;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public MatrixProperties setDelimiter(String delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	// public IOFormat getIoFormat() {
	// return ioFormat;
	// }
	//
	// public MatrixProperties setIoFormat(IOFormat ioFormat) {
	// this.ioFormat = ioFormat;
	// return this;
	// }

}
