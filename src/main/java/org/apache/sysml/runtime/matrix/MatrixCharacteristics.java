/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.sysml.runtime.matrix;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.sysml.lops.MMTSJ.MMTSJType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.instructions.mr.AggregateBinaryInstruction;
import org.apache.sysml.runtime.instructions.mr.AggregateInstruction;
import org.apache.sysml.runtime.instructions.mr.AggregateUnaryInstruction;
import org.apache.sysml.runtime.instructions.mr.AppendInstruction;
import org.apache.sysml.runtime.instructions.mr.BinaryInstruction;
import org.apache.sysml.runtime.instructions.mr.BinaryMInstruction;
import org.apache.sysml.runtime.instructions.mr.BinaryMRInstructionBase;
import org.apache.sysml.runtime.instructions.mr.CM_N_COVInstruction;
import org.apache.sysml.runtime.instructions.mr.CombineBinaryInstruction;
import org.apache.sysml.runtime.instructions.mr.CombineTernaryInstruction;
import org.apache.sysml.runtime.instructions.mr.CombineUnaryInstruction;
import org.apache.sysml.runtime.instructions.mr.CumulativeAggregateInstruction;
import org.apache.sysml.runtime.instructions.mr.DataGenMRInstruction;
import org.apache.sysml.runtime.instructions.mr.GroupedAggregateInstruction;
import org.apache.sysml.runtime.instructions.mr.GroupedAggregateMInstruction;
import org.apache.sysml.runtime.instructions.mr.MMTSJMRInstruction;
import org.apache.sysml.runtime.instructions.mr.MRInstruction;
import org.apache.sysml.runtime.instructions.mr.MapMultChainInstruction;
import org.apache.sysml.runtime.instructions.mr.MatrixReshapeMRInstruction;
import org.apache.sysml.runtime.instructions.mr.PMMJMRInstruction;
import org.apache.sysml.runtime.instructions.mr.ParameterizedBuiltinMRInstruction;
import org.apache.sysml.runtime.instructions.mr.QuaternaryInstruction;
import org.apache.sysml.runtime.instructions.mr.RandInstruction;
import org.apache.sysml.runtime.instructions.mr.RangeBasedReIndexInstruction;
import org.apache.sysml.runtime.instructions.mr.ReblockInstruction;
import org.apache.sysml.runtime.instructions.mr.RemoveEmptyMRInstruction;
import org.apache.sysml.runtime.instructions.mr.ReorgInstruction;
import org.apache.sysml.runtime.instructions.mr.ReplicateInstruction;
import org.apache.sysml.runtime.instructions.mr.ScalarInstruction;
import org.apache.sysml.runtime.instructions.mr.SeqInstruction;
import org.apache.sysml.runtime.instructions.mr.TernaryInstruction;
import org.apache.sysml.runtime.instructions.mr.UaggOuterChainInstruction;
import org.apache.sysml.runtime.instructions.mr.UnaryInstruction;
import org.apache.sysml.runtime.instructions.mr.UnaryMRInstructionBase;
import org.apache.sysml.runtime.instructions.mr.ZeroOutInstruction;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

/**
 * Matrix characteristics, such as the number of rows, the number of columns, the number of rows
 * per block, the number of columns per block, and the number of non-zero values in the matrix.
 *
 */
public class MatrixCharacteristics implements Serializable
{
	private static final long serialVersionUID = 8300479822915546000L;

	private long numRows = -1;
	private long numColumns = -1;
	private int numRowsPerBlock = 1;
	private int numColumnsPerBlock = 1;
	private long numNonZeros = -1;
	
	/**
	 * Constructor.
	 */
	public MatrixCharacteristics() {
	
	}
	
	/**
	 * Constructor to create a MatrixCharacteristics object based on the number of rows, the number of
	 * columns, the number of rows per block, and the number of columns per block in a matrix.
	 * 
	 * @param numRows             The number of rows in the matrix.
	 * @param numColumns          The number of columns in the matrix.
	 * @param numRowsPerBlock     The number of rows per block in the matrix.
	 * @param numColumnsPerBlock  The number of columns per block in the matrix.
	 */
	public MatrixCharacteristics(long numRows, long numColumns, int numRowsPerBlock, int numColumnsPerBlock)
	{
		set(numRows, numColumns, numRowsPerBlock, numColumnsPerBlock);
	}

	/**
	 * Constructor to create a MatrixCharacteristics object based on the number of rows, the number of
	 * columns, the number of rows per block, the number of columns per block, and the number of non-zero
	 * values in a matrix.
	 * 
	 * @param numRows             The number of rows in the matrix.
	 * @param numColumns          The number of columns in the matrix.
	 * @param numRowsPerBlock     The number of rows per block in the matrix.
	 * @param numColumnsPerBlock  The number of columns per block in the matrix.
	 * @param numNonZeros         The number of non-zero values in the matrix.
	 */
	public MatrixCharacteristics(long numRows, long numColumns, int numRowsPerBlock, int numColumnsPerBlock, long numNonZeros)
	{
		set(numRows, numColumns, numRowsPerBlock, numColumnsPerBlock, numNonZeros);
	}
	
	/**
	 * Constructor to create a MatrixCharacteristics object based on an existing MatrixCharacteristics object.
	 * 
	 * @param matrixCharacteristics Existing MatrixCharacteristics object.
	 */
	public MatrixCharacteristics(MatrixCharacteristics matrixCharacteristics)
	{
		set(matrixCharacteristics.numRows, matrixCharacteristics.numColumns, matrixCharacteristics.numRowsPerBlock,
				matrixCharacteristics.numColumnsPerBlock, matrixCharacteristics.numNonZeros);
	}

	/**
	 * Set the number of rows, the number of columns, the number of rows per block, and the number of
	 * columns per block in the matrix.
	 * 
	 * @param numRows             The number of rows in the matrix.
	 * @param numColumns          The number of columns in the matrix.
	 * @param numRowsPerBlock     The number of rows per block in the matrix.
	 * @param numColumnsPerBlock  The number of columns per block in the matrix.
	 */
	public void set(long numRows, long numColumns, int numRowsPerBlock, int numColumnsPerBlock) {
		this.numRows = numRows;
		this.numColumns = numColumns;
		this.numRowsPerBlock = numRowsPerBlock;
		this.numColumnsPerBlock = numColumnsPerBlock;
	}
	
	/**
	 * Set the number of rows, the number of columns, the number of rows per block, the number of
	 * columns per block, and the number of non-zero values in the matrix.
	 * 
	 * @param numRows             The number of rows in the matrix.
	 * @param numColumns          The number of columns in the matrix.
	 * @param numRowsPerBlock     The number of rows per block in the matrix.
	 * @param numColumnsPerBlock  The number of columns per block in the matrix.
	 * @param numNonZeros         The number of non-zero values in the matrix.
	 */
	public void set(long numRows, long numColumns, int numRowsPerBlock, int numColumnsPerBlock, long numNonZeros) {
		this.numRows = numRows;
		this.numColumns = numColumns;
		this.numRowsPerBlock = numRowsPerBlock;
		this.numColumnsPerBlock = numColumnsPerBlock;
		this.numNonZeros = numNonZeros;
	}
	
	/**
	 * Set this MatrixCharacteristics object's values based on another existing MatrixCharacteristics object.
	 * 
	 * @param matrixCharacteristics Existing MatrixCharacteristics object.
	 */
	public void set(MatrixCharacteristics matrixCharacteristics) {
		this.numRows = matrixCharacteristics.numRows;
		this.numColumns = matrixCharacteristics.numColumns;
		this.numRowsPerBlock = matrixCharacteristics.numRowsPerBlock;
		this.numColumnsPerBlock = matrixCharacteristics.numColumnsPerBlock;
		this.numNonZeros = matrixCharacteristics.numNonZeros;
	}
	
	/**
	 * Obtain the number of rows in the matrix.
	 * 
	 * @return The number of rows in the matrix.
	 */
	public long getRows(){
		return numRows;
	}

	/**
	 * Obtain the number of columns in the matrix.
	 * 
	 * @return The number of columns in the matrix.
	 */
	public long getCols(){
		return numColumns;
	}
	
	/**
	 * Obtain the number of rows per block in the matrix.
	 * 
	 * @return The number of rows per block in the matrix.
	 */
	public int getRowsPerBlock() {
		return numRowsPerBlock;
	}
	
	/**
	 * Set the number of rows per block in the matrix.
	 * 
	 * @param numRowsPerBlock The number of rows per block in the matrix.
	 */
	public void setRowsPerBlock( int numRowsPerBlock){
		this.numRowsPerBlock = numRowsPerBlock;
	} 
	
	/**
	 * Obtain the number of columns per block in the matrix.
	 * 
	 * @return The number of columns per block in the matrix.
	 */
	public int getColsPerBlock() {
		return numColumnsPerBlock;
	}
	
	/**
	 * Set the number of columns per block in the matrix.
	 * 
	 * @param numColumnsPerBlock The number of columns per block in the matrix.
	 */
	public void setColsPerBlock( int numColumnsPerBlock){
		this.numColumnsPerBlock = numColumnsPerBlock;
	} 
	
	/**
	 * Obtain the number of row blocks in the matrix.
	 * 
	 * @return The number of row blocks (rows divided by rows per block, rounded up).
	 */
	public long getNumRowBlocks(){
		return (long) Math.ceil((double)getRows() / getRowsPerBlock());
	}
	
	/**
	 * Obtain the number of column blocks in the matrix.
	 * 
	 * @return The number of column blocks (columns divided by columns per block, rounded up).
	 */
	public long getNumColBlocks(){
		return (long) Math.ceil((double)getCols() / getColsPerBlock());
	}
	
	public String toString()
	{
		return "["+numRows+" x "+numColumns+", nnz="+numNonZeros
		+", blocks ("+numRowsPerBlock+" x "+numColumnsPerBlock+")]";
	}
	
	/**
	 * Set the number of rows and the number of columns in the matrix.
	 * 
	 * @param numRows    The number of rows in the matrix.
	 * @param numColumns The number of columns in the matrix.
	 */
	public void setDimension(long numRows, long numColumns)
	{
		this.numRows = numRows;
		this.numColumns = numColumns;
	}
	
	/**
	 * Set the number of rows per block and the number of columns per block in the matrix.
	 * 
	 * @param numRowsPerBlock     The number of rows per block in the matrix.
	 * @param numColumnsPerBlock  The number of columns per block in the matrix.
	 */
	public void setBlockSize(int numRowsPerBlock, int numColumnsPerBlock)
	{
		this.numRowsPerBlock = numRowsPerBlock;
		this.numColumnsPerBlock = numColumnsPerBlock;
	}
	
	/**
	 * Set the number of non-zero values in the matrix.
	 * 
	 * @param numNonZeros    The number of non-zero values in the matrix.
	 */
	public void setNonZeros(long numNonZeros) {
		this.numNonZeros = numNonZeros;
	}
	
	/**
	 * Obtain the number of non-zero values in the matrix.
	 * 
	 * @return The number of non-zero values in the matrix.
	 */
	public long getNonZeros() {
		return numNonZeros;
	}
	
	/**
	 * Determine if the dimensions (the number of rows and the number of columns) of the matrix are known.
	 * 
	 * @return <code>true</code> if the number of rows and the number of columns are both greater than 0; <code>false</code> otherwise.
	 */
	public boolean dimsKnown() {
		return ( numRows > 0 && numColumns > 0 );
	}
	
	/**
	 * Determine if the dimensions of the matrix are known, also considering the number of non-zero values.
	 * 
	 * @param includeNumNonZeros Include the number of non-zeros.
	 * @return <code>true</code> if the number of rows and the number of columns are both greater than 0 and either the number of
	 * non-zero values is greater or equal to 0 or <code>includeNumNonZeros</code> is <code>false</code>; returns <code>false</code>
	 * otherwise.
	 */
	public boolean dimsKnown(boolean includeNumNonZeros) {
		return ( numRows > 0 && numColumns > 0 && (!includeNumNonZeros || numNonZeros>=0));
	}
	
	/**
	 * Determine if the number of rows in the matrix is known.
	 * 
	 * @return <code>true</code> if the number of rows is greater than 0; <code>false</code> otherwise.
	 */
	public boolean rowsKnown() {
		return ( numRows > 0 );
	}

	/**
	 * Determine if the number of columns in the matrix is known.
	 * 
	 * @return <code>true</code> if the number of columns is greater than 0; <code>false</code> otherwise.
	 */
	public boolean colsKnown() {
		return ( numColumns > 0 );
	}
	
	/**
	 * Determine if the number of non-zero values is known.
	 * 
	 * @return <code>true</code> if the number of non-zero values is greater or equal to 0; <code>false</code> otherwise.
	 */
	public boolean nnzKnown() {
		return ( numNonZeros >= 0 );
	}
	
	/**
	 * Determine if the matrix might have empty blocks.
	 * 
	 * @return <code>true</code> if the matrix might have empty blocks; <code>false</code> otherwise.
	 */
	public boolean mightHaveEmptyBlocks() 
	{
		long singleBlk =  Math.min(numRows, numRowsPerBlock) 
				        * Math.min(numColumns, numColumnsPerBlock);
		return !nnzKnown() || (numNonZeros < numRows*numColumns - singleBlk);
	}
	
	public static void reorg(MatrixCharacteristics dim, ReorgOperator op, 
			MatrixCharacteristics dimOut) throws DMLUnsupportedOperationException, DMLRuntimeException
	{
		op.fn.computeDimension(dim, dimOut);
	}
	
	public static void aggregateUnary(MatrixCharacteristics dim, AggregateUnaryOperator op, 
			MatrixCharacteristics dimOut) throws DMLUnsupportedOperationException, DMLRuntimeException
	{
		op.indexFn.computeDimension(dim, dimOut);
	}
	
	public static void aggregateBinary(MatrixCharacteristics dim1, MatrixCharacteristics dim2,
			AggregateBinaryOperator op, MatrixCharacteristics dimOut) 
	throws DMLUnsupportedOperationException
	{
		//set dimension
		dimOut.set(dim1.numRows, dim2.numColumns, dim1.numRowsPerBlock, dim2.numColumnsPerBlock);
	}
	
	public static void computeDimension(HashMap<Byte, MatrixCharacteristics> dims, MRInstruction ins) 
		throws DMLUnsupportedOperationException, DMLRuntimeException
	{
		MatrixCharacteristics dimOut=dims.get(ins.output);
		if(dimOut==null)
		{
			dimOut=new MatrixCharacteristics();
			dims.put(ins.output, dimOut);
		}
		
		if(ins instanceof ReorgInstruction)
		{
			ReorgInstruction realIns=(ReorgInstruction)ins;
			reorg(dims.get(realIns.input), (ReorgOperator)realIns.getOperator(), dimOut);
		}
		else if(ins instanceof AppendInstruction )
		{
			AppendInstruction realIns = (AppendInstruction)ins;
			MatrixCharacteristics in_dim1 = dims.get(realIns.input1);
			MatrixCharacteristics in_dim2 = dims.get(realIns.input2);
			if( realIns.isCBind() )
				dimOut.set(in_dim1.numRows, in_dim1.numColumns+in_dim2.numColumns, in_dim1.numRowsPerBlock, in_dim2.numColumnsPerBlock);
			else
				dimOut.set(in_dim1.numRows+in_dim2.numRows, in_dim1.numColumns, in_dim1.numRowsPerBlock, in_dim2.numColumnsPerBlock);
		}
		else if(ins instanceof CumulativeAggregateInstruction)
		{
			AggregateUnaryInstruction realIns=(AggregateUnaryInstruction)ins;
			MatrixCharacteristics in = dims.get(realIns.input);
			dimOut.set((long)Math.ceil( (double)in.getRows()/in.getRowsPerBlock()), in.getCols(), in.getRowsPerBlock(), in.getColsPerBlock());
		}
		else if(ins instanceof AggregateUnaryInstruction)
		{
			AggregateUnaryInstruction realIns=(AggregateUnaryInstruction)ins;
			aggregateUnary(dims.get(realIns.input), 
					(AggregateUnaryOperator)realIns.getOperator(), dimOut);
		}
		else if(ins instanceof AggregateBinaryInstruction)
		{
			AggregateBinaryInstruction realIns=(AggregateBinaryInstruction)ins;
			aggregateBinary(dims.get(realIns.input1), dims.get(realIns.input2),
					(AggregateBinaryOperator)realIns.getOperator(), dimOut);
		}
		else if(ins instanceof MapMultChainInstruction)
		{
			//output size independent of chain type
			MapMultChainInstruction realIns=(MapMultChainInstruction)ins;
			MatrixCharacteristics mc1 = dims.get(realIns.getInput1());
			MatrixCharacteristics mc2 = dims.get(realIns.getInput2());
			dimOut.set(mc1.numColumns, mc2.numColumns, mc1.numRowsPerBlock, mc1.numColumnsPerBlock);	
		}
		else if(ins instanceof QuaternaryInstruction)
		{
			QuaternaryInstruction realIns=(QuaternaryInstruction)ins;
			MatrixCharacteristics mc1 = dims.get(realIns.getInput1());
			MatrixCharacteristics mc2 = dims.get(realIns.getInput2());
			MatrixCharacteristics mc3 = dims.get(realIns.getInput3());
			realIns.computeMatrixCharacteristics(mc1, mc2, mc3, dimOut);
		}
		else if(ins instanceof ReblockInstruction)
		{
			ReblockInstruction realIns=(ReblockInstruction)ins;
			MatrixCharacteristics in_dim=dims.get(realIns.input);
			dimOut.set(in_dim.numRows, in_dim.numColumns, realIns.brlen, realIns.bclen, in_dim.numNonZeros);
		}
		else if( ins instanceof MatrixReshapeMRInstruction )
		{
			MatrixReshapeMRInstruction mrinst = (MatrixReshapeMRInstruction) ins;
			MatrixCharacteristics in_dim=dims.get(mrinst.input);
			dimOut.set(mrinst.getNumRows(),mrinst.getNumColunms(),in_dim.getRowsPerBlock(), in_dim.getColsPerBlock(), in_dim.getNonZeros());
		}
		else if(ins instanceof RandInstruction
				|| ins instanceof SeqInstruction) 
		{
			DataGenMRInstruction dataIns=(DataGenMRInstruction)ins;
			dimOut.set(dims.get(dataIns.getInput()));
		}
		else if( ins instanceof ReplicateInstruction )
		{
			ReplicateInstruction realIns=(ReplicateInstruction)ins;
			realIns.computeOutputDimension(dims.get(realIns.input), dimOut);
		}
		else if( ins instanceof ParameterizedBuiltinMRInstruction ) //before unary
		{
			ParameterizedBuiltinMRInstruction realIns = (ParameterizedBuiltinMRInstruction)ins;
			realIns.computeOutputCharacteristics(dims.get(realIns.input), dimOut);
		}
		else if(ins instanceof ScalarInstruction 
				|| ins instanceof AggregateInstruction
				||(ins instanceof UnaryInstruction && !(ins instanceof MMTSJMRInstruction))
				|| ins instanceof ZeroOutInstruction)
		{
			UnaryMRInstructionBase realIns=(UnaryMRInstructionBase)ins;
			dimOut.set(dims.get(realIns.input));
		}
		else if (ins instanceof MMTSJMRInstruction)
		{
			MMTSJMRInstruction mmtsj = (MMTSJMRInstruction)ins;
			MMTSJType tstype = mmtsj.getMMTSJType();
			MatrixCharacteristics mc = dims.get(mmtsj.input);
			dimOut.set( tstype.isLeft() ? mc.numColumns : mc.numRows,
					    tstype.isLeft() ? mc.numColumns : mc.numRows,
					     mc.numRowsPerBlock, mc.numColumnsPerBlock );
		}
		else if( ins instanceof PMMJMRInstruction )
		{
			PMMJMRInstruction pmmins = (PMMJMRInstruction) ins;
			MatrixCharacteristics mc = dims.get(pmmins.input2);
			dimOut.set( pmmins.getNumRows(),
					     mc.numColumns,
					     mc.numRowsPerBlock, mc.numColumnsPerBlock );
		}
		else if( ins instanceof RemoveEmptyMRInstruction )
		{
			RemoveEmptyMRInstruction realIns=(RemoveEmptyMRInstruction)ins;
			MatrixCharacteristics mc = dims.get(realIns.input1);
			if( realIns.isRemoveRows() )
				dimOut.set(realIns.getOutputLen(), mc.getCols(), mc.numRowsPerBlock, mc.numColumnsPerBlock);
			else
				dimOut.set(mc.getRows(), realIns.getOutputLen(), mc.numRowsPerBlock, mc.numColumnsPerBlock);
		}
		else if(ins instanceof UaggOuterChainInstruction) //needs to be checked before binary
		{
			UaggOuterChainInstruction realIns=(UaggOuterChainInstruction)ins;
			MatrixCharacteristics mc1 = dims.get(realIns.input1);
			MatrixCharacteristics mc2 = dims.get(realIns.input2);
			realIns.computeOutputCharacteristics(mc1, mc2, dimOut);
		}
		else if( ins instanceof GroupedAggregateMInstruction )
		{
			GroupedAggregateMInstruction realIns = (GroupedAggregateMInstruction) ins;
			MatrixCharacteristics mc1 = dims.get(realIns.input1);
			realIns.computeOutputCharacteristics(mc1, dimOut);
		}
		else if(ins instanceof BinaryInstruction || ins instanceof BinaryMInstruction || ins instanceof CombineBinaryInstruction )
		{
			BinaryMRInstructionBase realIns=(BinaryMRInstructionBase)ins;
			MatrixCharacteristics mc1 = dims.get(realIns.input1);
			MatrixCharacteristics mc2 = dims.get(realIns.input2);
			if(    mc1.getRows()>1 && mc1.getCols()==1 
				&& mc2.getRows()==1 && mc2.getCols()>1 ) //outer
			{
				dimOut.set(mc1.getRows(), mc2.getCols(), mc1.getRowsPerBlock(), mc2.getColsPerBlock());
			}
			else { //default case
				dimOut.set(mc1);	
			}
		}
		else if (ins instanceof CombineTernaryInstruction ) {
			TernaryInstruction realIns=(TernaryInstruction)ins;
			dimOut.set(dims.get(realIns.input1));
		}
		else if (ins instanceof CombineUnaryInstruction ) {
			dimOut.set( dims.get(((CombineUnaryInstruction) ins).input));
		}
		else if(ins instanceof CM_N_COVInstruction || ins instanceof GroupedAggregateInstruction )
		{
			dimOut.set(1, 1, 1, 1);
		}
		else if(ins instanceof RangeBasedReIndexInstruction)
		{
			RangeBasedReIndexInstruction realIns = (RangeBasedReIndexInstruction)ins;
			MatrixCharacteristics dimIn = dims.get(realIns.input);
			realIns.computeOutputCharacteristics(dimIn, dimOut);
		}
		else if (ins instanceof TernaryInstruction) {
			TernaryInstruction realIns = (TernaryInstruction)ins;
			MatrixCharacteristics in_dim=dims.get(realIns.input1);
			dimOut.set(realIns.getOutputDim1(), realIns.getOutputDim2(), in_dim.numRowsPerBlock, in_dim.numColumnsPerBlock);
		}
		else { 
			/*
			 * if ins is none of the above cases then we assume that dim_out dimensions are unknown
			 */
			dimOut.numRows = -1;
			dimOut.numColumns = -1;
			dimOut.numRowsPerBlock=1;
			dimOut.numColumnsPerBlock=1;
		}
	}

	@Override
	public boolean equals (Object anObject)
	{
		if (anObject instanceof MatrixCharacteristics)
		{
			MatrixCharacteristics mc = (MatrixCharacteristics) anObject;
			return ((numRows == mc.numRows) && 
					(numColumns == mc.numColumns) && 
					(numRowsPerBlock == mc.numRowsPerBlock) && 
					(numColumnsPerBlock == mc.numColumnsPerBlock) && 
					(numNonZeros == mc.numNonZeros)) ;
		}
		else
			return false;
	}
	
	@Override
	public int hashCode()
	{
		return super.hashCode();
	}
}
