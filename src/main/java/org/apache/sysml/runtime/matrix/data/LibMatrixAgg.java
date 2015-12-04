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

package org.apache.sysml.runtime.matrix.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.functionobjects.Builtin;
import org.apache.sysml.runtime.functionobjects.Builtin.BuiltinFunctionCode;
import org.apache.sysml.runtime.functionobjects.IndexFunction;
import org.apache.sysml.runtime.functionobjects.KahanFunction;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.functionobjects.KahanPlusSq;
import org.apache.sysml.runtime.functionobjects.Mean;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.ReduceAll;
import org.apache.sysml.runtime.functionobjects.ReduceCol;
import org.apache.sysml.runtime.functionobjects.ReduceDiag;
import org.apache.sysml.runtime.functionobjects.ReduceRow;
import org.apache.sysml.runtime.functionobjects.ValueFunction;
import org.apache.sysml.runtime.instructions.cp.KahanObject;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * MB:
 * Library for matrix aggregations including ak+, uak+ for all
 * combinations of dense and sparse representations, and corrections.
 * Those are performance-critical operations because they are used
 * on combiners/reducers of important operations like tsmm, mvmult,
 * indexing, but also basic sum/min/max/mean, row*, col*, etc. Specific
 * handling is especially required for all non sparse-safe operations
 * in order to prevent unnecessary worse asymptotic behavior.
 *
 * This library currently covers the following opcodes:
 * ak+, uak+, uark+, uack+, uamin, uarmin, uacmin, uamax, uarmax, uacmax,
 * ua*, uamean, uarmean, uacmean, uarimax, uaktrace.
 * cumk+, cummin, cummax, cum*, tak+
 * 
 * TODO next opcode extensions: a+, colindexmax
 */
public class LibMatrixAgg 
{
	//internal configuration parameters
	private static final boolean NAN_AWARENESS = false;
	private static final long PAR_NUMCELL_THRESHOLD = 1024*1024;   //Min 1M elements
	private static final long PAR_INTERMEDIATE_SIZE_THRESHOLD = 2*1024*1024; //Max 2MB
	
	////////////////////////////////
	// public matrix agg interface
	////////////////////////////////
	
	private enum AggType {
		KAHAN_SUM,
		KAHAN_SUM_SQ,
		CUM_KAHAN_SUM,
		CUM_MIN,
		CUM_MAX,
		CUM_PROD,
		MIN,
		MAX,
		MEAN,
		MAX_INDEX,
		MIN_INDEX,
		PROD,
		INVALID,
	}

	private LibMatrixAgg() {
		//prevent instantiation via private constructor
	}
	
	/**
	 * Core incremental matrix aggregate (ak+) as used in mapmult, tsmm, 
	 * cpmm, etc. Note that we try to keep the current 
	 * aggVal and aggCorr in dense format in order to allow efficient
	 * access according to the dense/sparse input. 
	 * 
	 * 
	 * @param in input matrix
	 * @param aggVal current aggregate values (in/out)
	 * @param aggCorr current aggregate correction (in/out)
	 * @throws DMLRuntimeException 
	 */
	public static void aggregateBinaryMatrix(MatrixBlock in, MatrixBlock aggVal, MatrixBlock aggCorr) 
		throws DMLRuntimeException
	{	
		//Timing time = new Timing(true);
		//boolean saggVal = aggVal.isInSparseFormat(), saggCorr = aggCorr.isInSparseFormat(); 
		//long naggVal = aggVal.getNonZeros(), naggCorr = aggCorr.getNonZeros();
		
		//core aggregation
		if(!in.sparse && !aggVal.sparse && !aggCorr.sparse)
			aggregateBinaryMatrixAllDense(in, aggVal, aggCorr);
		else if(in.sparse && !aggVal.sparse && !aggCorr.sparse)
			aggregateBinaryMatrixSparseDense(in, aggVal, aggCorr);
		else if(in.sparse ) //any aggVal, aggCorr
			aggregateBinaryMatrixSparseGeneric(in, aggVal, aggCorr);
		else //if( !in.sparse ) //any aggVal, aggCorr
			aggregateBinaryMatrixDenseGeneric(in, aggVal, aggCorr);
		
		//System.out.println("agg ("+in.rlen+","+in.clen+","+in.getNonZeros()+","+in.sparse+"), " +
		//		          "("+naggVal+","+saggVal+"), ("+naggCorr+","+saggCorr+") -> " +
		//		          "("+aggVal.getNonZeros()+","+aggVal.isInSparseFormat()+"), ("+aggCorr.getNonZeros()+","+aggCorr.isInSparseFormat()+") " +
		//		          "in "+time.stop()+"ms.");
	}
	
	/**
	 * Core incremental matrix aggregate (ak+) as used for uack+ and acrk+.
	 * Embedded correction values.
	 * 
	 * @param in
	 * @param aggVal
	 * @throws DMLRuntimeException
	 */
	public static void aggregateBinaryMatrix(MatrixBlock in, MatrixBlock aggVal, AggregateOperator aop) 
		throws DMLRuntimeException
	{	
		//sanity check matching dimensions 
		if( in.getNumRows()!=aggVal.getNumRows() || in.getNumColumns()!=aggVal.getNumColumns() )
			throw new DMLRuntimeException("Dimension mismatch on aggregate: "+in.getNumRows()+"x"+in.getNumColumns()+
					" vs "+aggVal.getNumRows()+"x"+aggVal.getNumColumns());
		
		//Timing time = new Timing(true);
		
		//core aggregation
		boolean lastRowCorr = (aop.correctionLocation == CorrectionLocationType.LASTROW);
		boolean lastColCorr = (aop.correctionLocation == CorrectionLocationType.LASTCOLUMN);
		if( !in.sparse && lastRowCorr )
			aggregateBinaryMatrixLastRowDenseGeneric(in, aggVal);
		else if( in.sparse && lastRowCorr )
			aggregateBinaryMatrixLastRowSparseGeneric(in, aggVal);
		else if( !in.sparse && lastColCorr )
			aggregateBinaryMatrixLastColDenseGeneric(in, aggVal);
		else //if( in.sparse && lastColCorr )
			aggregateBinaryMatrixLastColSparseGeneric(in, aggVal);
		
		//System.out.println("agg ("+in.rlen+","+in.clen+","+in.getNonZeros()+","+in.sparse+"), ("+naggVal+","+saggVal+") -> " +
		//                   "("+aggVal.getNonZeros()+","+aggVal.isInSparseFormat()+") in "+time.stop()+"ms.");
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param vFn
	 * @param ixFn
	 * @throws DMLRuntimeException
	 */
	public static void aggregateUnaryMatrix(MatrixBlock in, MatrixBlock out, AggregateUnaryOperator uaop) 
		throws DMLRuntimeException
	{
		//prepare meta data 
		AggType aggtype = getAggType(uaop);
		final int m = in.rlen;
		final int m2 = out.rlen;
		final int n2 = out.clen;
		
		//filter empty input blocks (incl special handling for sparse-unsafe operations)
		if( in.isEmptyBlock(false) ){
			aggregateUnaryMatrixEmpty(in, out, aggtype, uaop.indexFn);
			return;
		}	
		
		//Timing time = new Timing(true);
		
		//allocate output arrays (if required)
		out.reset(m2, n2, false); //always dense
		out.allocateDenseBlock();
		
		if( !in.sparse )
			aggregateUnaryMatrixDense(in, out, aggtype, uaop.aggOp.increOp.fn, uaop.indexFn, 0, m);
		else
			aggregateUnaryMatrixSparse(in, out, aggtype, uaop.aggOp.increOp.fn, uaop.indexFn, 0, m);
				
		//cleanup output and change representation (if necessary)
		out.recomputeNonZeros();
		out.examSparsity();
		
		//System.out.println("uagg ("+in.rlen+","+in.clen+","+in.sparse+") in "+time.stop()+"ms.");
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param uaop
	 * @param k
	 * @throws DMLRuntimeException
	 */
	public static void aggregateUnaryMatrix(MatrixBlock in, MatrixBlock out, AggregateUnaryOperator uaop, int k) 
		throws DMLRuntimeException
	{
		//fall back to sequential version if necessary
		if(    k <= 1 || (long)in.rlen*in.clen < PAR_NUMCELL_THRESHOLD || in.rlen <= k
			|| (!(uaop.indexFn instanceof ReduceCol) &&  out.clen*8*k > PAR_INTERMEDIATE_SIZE_THRESHOLD ) ) {
			aggregateUnaryMatrix(in, out, uaop);
			return;
		}
		
		//prepare meta data
		AggType aggtype = getAggType(uaop);
		final int m = in.rlen;
		final int m2 = out.rlen;
		final int n2 = out.clen;
		
		//filter empty input blocks (incl special handling for sparse-unsafe operations)
		if( in.isEmptyBlock(false) ){
			aggregateUnaryMatrixEmpty(in, out, aggtype, uaop.indexFn);
			return;
		}	
		
		//Timing time = new Timing(true);
		
		//allocate output arrays (if required)
		if( uaop.indexFn instanceof ReduceCol ) {
			out.reset(m2, n2, false); //always dense
			out.allocateDenseBlock();
		}
		
		//core multi-threaded unary aggregate computation
		//(currently: always parallelization over number of rows)
		try {
			ExecutorService pool = Executors.newFixedThreadPool( k );
			ArrayList<AggTask> tasks = new ArrayList<AggTask>();
			int blklen = (int)(Math.ceil((double)m/k));
			for( int i=0; i<k & i*blklen<m; i++ ) {
				tasks.add( (uaop.indexFn instanceof ReduceCol) ? 
						new RowAggTask(in, out, aggtype, uaop, i*blklen, Math.min((i+1)*blklen, m)) :
						new PartialAggTask(in, out, aggtype, uaop, i*blklen, Math.min((i+1)*blklen, m)) );
			}
			pool.invokeAll(tasks);	
			pool.shutdown();
			//aggregate partial results
			if( !(uaop.indexFn instanceof ReduceCol) ) {
				out.copy(((PartialAggTask)tasks.get(0)).getResult()); //for init
				for( int i=1; i<tasks.size(); i++ )
					aggregateFinalResult(uaop.aggOp, out, ((PartialAggTask)tasks.get(i)).getResult());
			}
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
				
		//cleanup output and change representation (if necessary)
		out.recomputeNonZeros();
		out.examSparsity();
		
		//System.out.println("uagg k="+k+" ("+in.rlen+","+in.clen+","+in.sparse+") in "+time.stop()+"ms.");
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param uop
	 * @throws DMLRuntimeException
	 */
	public static void aggregateUnaryMatrix(MatrixBlock in, MatrixBlock out, UnaryOperator uop) 
		throws DMLRuntimeException
	{
		//prepare meta data 
		AggType aggtype = getAggType(uop);
		final int m = in.rlen;
		final int m2 = out.rlen;
		final int n2 = out.clen;
		
		//filter empty input blocks (incl special handling for sparse-unsafe operations)
		if( in.isEmptyBlock(false) ){
			aggregateUnaryMatrixEmpty(in, out, aggtype, null);
			return;
		}	
		
		//allocate output arrays (if required)
		out.reset(m2, n2, false); //always dense
		out.allocateDenseBlock();
		
		//Timing time = new Timing(true);
		
		if( !in.sparse )
			aggregateUnaryMatrixDense(in, out, aggtype, uop.fn, null, 0, m);
		else
			aggregateUnaryMatrixSparse(in, out, aggtype, uop.fn, null, 0, m);
		
		//cleanup output and change representation (if necessary)
		out.recomputeNonZeros();
		out.examSparsity();
		
		//System.out.println("uop ("+in.rlen+","+in.clen+","+in.sparse+") in "+time.stop()+"ms.");
	}
	
	/**
	 * 
	 * @param in1
	 * @param in2
	 * @param in3
	 * @param ret
	 * @return
	 */
	public static double aggregateTernary(MatrixBlock in1, MatrixBlock in2, MatrixBlock in3)
	{
		//early abort if any block is empty
		if( in1.isEmptyBlock(false) || in2.isEmptyBlock(false) || in3!=null&&in3.isEmptyBlock(false) ) {
			return 0;
		}
				
		//Timing time = new Timing(true);
		
		double val = -1;
		if( !in1.sparse && !in2.sparse && (in3==null||!in3.sparse) ) //DENSE
			val = aggregateTernaryDense(in1, in2, in3, 0, in1.rlen);
		else //GENERAL CASE
			val = aggregateTernaryGeneric(in1, in2, in3, 0, in1.rlen);
		
		//System.out.println("tak+ ("+in1.rlen+","+in1.sparse+","+in2.sparse+","+in3.sparse+") in "+time.stop()+"ms.");
		
		return val;			
	}
	
	/**
	 * @throws DMLRuntimeException 
	 * 
	 */
	public static double aggregateTernary(MatrixBlock in1, MatrixBlock in2, MatrixBlock in3, int k) 
		throws DMLRuntimeException
	{		
		//fall back to sequential version if necessary
		if( k <= 1 || in1.rlen/3 < PAR_NUMCELL_THRESHOLD ) {
			return aggregateTernary(in1, in2, in3);
		}
		
		//early abort if any block is empty
		if( in1.isEmptyBlock(false) || in2.isEmptyBlock(false) || in3!=null&&in3.isEmptyBlock(false) ) {
			return 0;
		}
			
		//Timing time = new Timing(true);
		
		double val = -1;
		try {
			ExecutorService pool = Executors.newFixedThreadPool( k );
			ArrayList<AggTernaryTask> tasks = new ArrayList<AggTernaryTask>();
			int blklen = (int)(Math.ceil((double)in1.rlen/k));
			for( int i=0; i<k & i*blklen<in1.rlen; i++ )
				tasks.add( new AggTernaryTask(in1, in2, in3, i*blklen, Math.min((i+1)*blklen, in1.rlen)));
			pool.invokeAll(tasks);	
			pool.shutdown();
			//aggregate partial results
			KahanObject kbuff = new KahanObject(0, 0);
			KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
			for( AggTernaryTask task : tasks )
				kplus.execute2(kbuff, task.getResult());
			val = kbuff._sum;
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
		
		//System.out.println("tak+ k="+k+" ("+in1.rlen+","+in1.sparse+","+in2.sparse+","+in3.sparse+") in "+time.stop()+"ms.");
		
		return val;			
	}
	
	/**
	 * 
	 * @param op
	 * @return
	 */
	public static boolean isSupportedUnaryAggregateOperator( AggregateUnaryOperator op )
	{
		AggType type = getAggType( op );
		return (type != AggType.INVALID);
	}
	
	public static boolean isSupportedUnaryOperator( UnaryOperator op )
	{
		AggType type = getAggType( op );
		return (type != AggType.INVALID);
	}
	
	/**
	 * Recompute outputs (e.g., maxindex or minindex) according to block indexes from MR.
	 * TODO: this should not be part of block operations but of the MR instruction.
	 * 
	 * @param out
	 * @param op
	 * @param brlen
	 * @param bclen
	 * @param ix
	 */
	public static void recomputeIndexes( MatrixBlock out, AggregateUnaryOperator op, int brlen, int bclen, MatrixIndexes ix )
	{
		AggType type = getAggType(op);
		if( (type == AggType.MAX_INDEX || type == AggType.MIN_INDEX) && ix.getColumnIndex()!=1 ) //MAXINDEX or MININDEX
		{
			int m = out.rlen;
			double[] c = out.getDenseArray();
			for( int i=0, cix=0; i<m; i++, cix+=2 )
				c[cix] = UtilFunctions.cellIndexCalculation(ix.getColumnIndex(), bclen, (int)c[cix]-1);
		}
	}	
	
	/**
	 * 
	 * @param op
	 * @return
	 */
	private static AggType getAggType( AggregateUnaryOperator op )
	{
		ValueFunction vfn = op.aggOp.increOp.fn;
		IndexFunction ifn = op.indexFn;
		
		//(kahan) sum / sum squared / trace (for ReduceDiag)
		if( vfn instanceof KahanFunction
			&& (op.aggOp.correctionLocation == CorrectionLocationType.LASTCOLUMN || op.aggOp.correctionLocation == CorrectionLocationType.LASTROW)
			&& (ifn instanceof ReduceAll || ifn instanceof ReduceCol || ifn instanceof ReduceRow || ifn instanceof ReduceDiag) )
		{
			if (vfn instanceof KahanPlus)
				return AggType.KAHAN_SUM;
			else if (vfn instanceof KahanPlusSq)
				return AggType.KAHAN_SUM_SQ;
		}

		//mean
		if( vfn instanceof Mean 
			&& (op.aggOp.correctionLocation == CorrectionLocationType.LASTTWOCOLUMNS || op.aggOp.correctionLocation == CorrectionLocationType.LASTTWOROWS)
			&& (ifn instanceof ReduceAll || ifn instanceof ReduceCol || ifn instanceof ReduceRow) )
		{
			return AggType.MEAN;
		}
		
		//prod
		if( vfn instanceof Multiply && ifn instanceof ReduceAll )
		{
			return AggType.PROD;
		}
		
		//min / max
		if( vfn instanceof Builtin &&
		    (ifn instanceof ReduceAll || ifn instanceof ReduceCol || ifn instanceof ReduceRow) )
		{
			BuiltinFunctionCode bfcode = ((Builtin)vfn).bFunc;
			switch( bfcode ){
				case MAX: return AggType.MAX;
				case MIN: return AggType.MIN;
				case MAXINDEX: return AggType.MAX_INDEX;
				case MININDEX: return AggType.MIN_INDEX;
				default: //do nothing
			}
		}
		
		return AggType.INVALID;
	}
	
	/**
	 * 
	 * @param op
	 * @return
	 */
	private static AggType getAggType( UnaryOperator op )
	{
		ValueFunction vfn = op.fn;

		//cumsum/cumprod/cummin/cummax
		if( vfn instanceof Builtin ) {
			BuiltinFunctionCode bfunc = ((Builtin) vfn).bFunc;
			switch( bfunc )
			{
				case CUMSUM: 	return AggType.CUM_KAHAN_SUM;
				case CUMPROD:	return AggType.CUM_PROD;
				case CUMMIN:	return AggType.CUM_MIN;
				case CUMMAX: 	return AggType.CUM_MAX;
				default: 		return AggType.INVALID;
			}
		}
		
		return AggType.INVALID;
	}
	
	/**
	 * 
	 * @param aop
	 * @return
	 * @throws DMLRuntimeException 
	 * @throws DMLUnsupportedOperationException 
	 */
	private static void aggregateFinalResult( AggregateOperator aop, MatrixBlock out, MatrixBlock partout ) 
		throws DMLUnsupportedOperationException, DMLRuntimeException
	{
		AggregateOperator laop = aop;
		
		//special handling for mean where the final aggregate operator (kahan plus)
		//is not equals to the partial aggregate operator
		if( aop.increOp.fn instanceof Mean ) {
			laop = new AggregateOperator(0, KahanPlus.getKahanPlusFnObject(), aop.correctionExists, aop.correctionLocation);
		}

		//incremental aggregation of final results
		if( laop.correctionExists )
			out.incrementalAggregate(laop, partout);
		else
			out.binaryOperationsInPlace(laop.increOp, partout);
	}

	/**
	 * 
	 * @param in1
	 * @param in2
	 * @param in3
	 * @param rl
	 * @param ru
	 * @return
	 */
	private static double aggregateTernaryDense(MatrixBlock in1, MatrixBlock in2, MatrixBlock in3, int rl, int ru)
	{
		//compute block operations
		KahanObject kbuff = new KahanObject(0, 0);
		KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
		
		double[] a = in1.denseBlock;
		double[] b = in2.denseBlock;
		final int n = in1.clen;
		
		if( in3 != null ) //3 inputs
		{
			double[] c = in3.denseBlock;
			
			for( int i=rl, ix=rl*n; i<ru; i++ ) 
				for( int j=0; j<n; j++, ix++ ) {
					double val = a[ix] * b[ix] * c[ix];
					kplus.execute2( kbuff, val );
				}
		}
		else //2 inputs (third: literal 1)
		{
			for( int i=rl, ix=rl*n; i<ru; i++ ) 
				for( int j=0; j<n; j++, ix++ ) {
					double val = a[ix] * b[ix];
					kplus.execute2( kbuff, val );
				}
		}
		
		return kbuff._sum;
	}
	
	/**
	 * 
	 * @param in1
	 * @param in2
	 * @param in3
	 * @param rl
	 * @param ru
	 * @return
	 */
	private static double aggregateTernaryGeneric(MatrixBlock in1, MatrixBlock in2, MatrixBlock in3, int rl, int ru)
	{		
		//compute block operations
		KahanObject kbuff = new KahanObject(0, 0);
		KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
		final int n = in1.clen;
		
		if( in1.sparse )
		{
			SparseRow[] a = in1.sparseRows;
			
			for( int i=rl; i<ru; i++ )
				if( a[i]!=null && !a[i].isEmpty() ) {
					int alen = a[i].size();
					int[] aix = a[i].getIndexContainer();
					double[] avals = a[i].getValueContainer();
					for( int j=0; j<alen; j++ ) {
						double val1 = avals[j];
						double val2 = in2.quickGetValue(i, aix[j]);
						double val = val1 * val2;
						if( val != 0 && in3 != null )
							val *= in3.quickGetValue(i, aix[j]);
						kplus.execute2( kbuff, val );							
					}
				}	
		}
		else //generic case
		{
			for( int i=rl; i<ru; i++ )
				for( int j=0; j<n; j++ ){
					double val1 = in1.quickGetValue(i, j);
					double val2 = in2.quickGetValue(i, j);
					double val = val1 * val2;
					if( in3 != null )
						val *= in3.quickGetValue(i, j);
					kplus.execute2( kbuff, val );		
				}
		}
	
		return kbuff._sum;
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @param aggCorr
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixAllDense(MatrixBlock in, MatrixBlock aggVal, MatrixBlock aggCorr) 
			throws DMLRuntimeException
	{
		if( in.denseBlock==null || in.isEmptyBlock(false) )
			return;
		
		//allocate output arrays (if required)
		aggVal.allocateDenseBlock(); //should always stay in dense
		aggCorr.allocateDenseBlock(); //should always stay in dense
		
		double[] a = in.getDenseArray();
		double[] c = aggVal.getDenseArray();
		double[] cc = aggCorr.getDenseArray();
		
		KahanObject buffer1 = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		final int len = Math.min(a.length, in.rlen*in.clen);
		
		int nnzC = 0;
		int nnzCC = 0;
		
		for( int i=0; i<len; i++ )
		{
			buffer1._sum        = c[i];
			buffer1._correction = cc[i];
			akplus.execute2(buffer1, a[i]);
			c[i]  = buffer1._sum;
			cc[i] = buffer1._correction;
			nnzC += (buffer1._sum!=0)?1:0;
			nnzCC += (buffer1._correction!=0)?1:0;
		}
		
		aggVal.nonZeros = nnzC;
		aggCorr.nonZeros = nnzCC;
		//aggVal.examSparsity();
		//aggCorr.examSparsity(); 
	}

	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @param aggCorr
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixSparseDense(MatrixBlock in, MatrixBlock aggVal, MatrixBlock aggCorr) 
			throws DMLRuntimeException
	{
		if( in.sparseRows==null || in.isEmptyBlock(false) )
			return;
		
		//allocate output arrays (if required)
		aggVal.allocateDenseBlock(); //should always stay in dense
		aggCorr.allocateDenseBlock(); //should always stay in dense
		
		SparseRow[] a = in.getSparseRows();
		double[] c = aggVal.getDenseArray();
		double[] cc = aggCorr.getDenseArray();
		
		KahanObject buffer1 = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		final int m = in.rlen;
		final int n = in.clen;
		final int rlen = Math.min(a.length, m);
		
		for( int i=0, cix=0; i<rlen; i++, cix+=n )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				int[] aix = arow.getIndexContainer();
				double[] avals = arow.getValueContainer();
				
				for( int j=0; j<alen; j++ )
				{
					int ix = cix+aix[j];
					buffer1._sum        = c[ix];
					buffer1._correction = cc[ix];
					akplus.execute2(buffer1, avals[j]);
					c[ix]  = buffer1._sum;
					cc[ix] = buffer1._correction;
				}
			}
		}
		
		aggVal.recomputeNonZeros();
		aggCorr.recomputeNonZeros();
		//aggVal.examSparsity();
		//aggCorr.examSparsity(); 
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @param aggCorr
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixSparseGeneric(MatrixBlock in, MatrixBlock aggVal, MatrixBlock aggCorr) 
			throws DMLRuntimeException
	{
		if( in.sparseRows==null || in.isEmptyBlock(false) )
			return;
		
		SparseRow[] a = in.getSparseRows();
		
		KahanObject buffer1 = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		final int m = in.rlen;
		final int rlen = Math.min(a.length, m);
		
		for( int i=0; i<rlen; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				int[] aix = arow.getIndexContainer();
				double[] avals = arow.getValueContainer();
				
				for( int j=0; j<alen; j++ )
				{
					int jix = aix[j];
					buffer1._sum        = aggVal.quickGetValue(i, jix);
					buffer1._correction = aggCorr.quickGetValue(i, jix);
					akplus.execute2(buffer1, avals[j]);
					aggVal.quickSetValue(i, jix, buffer1._sum);
					aggCorr.quickSetValue(i, jix, buffer1._correction);
				}
			}
		}
		
		//note: nnz of aggVal/aggCorr maintained internally 
		aggVal.examSparsity();
		aggCorr.examSparsity(); 
	}
	
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @param aggCorr
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixDenseGeneric(MatrixBlock in, MatrixBlock aggVal, MatrixBlock aggCorr) 
		throws DMLRuntimeException
	{	
		if( in.denseBlock==null || in.isEmptyBlock(false) )
			return;
		
		final int m = in.rlen;
		final int n = in.clen;
		
		double[] a = in.getDenseArray();
		
		KahanObject buffer = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		//incl implicit nnz maintenance
		for(int i=0, ix=0; i<m; i++)
			for(int j=0; j<n; j++, ix++)
			{
				buffer._sum = aggVal.quickGetValue(i, j);
				buffer._correction = aggCorr.quickGetValue(i, j);
				akplus.execute(buffer, a[ix]);
				aggVal.quickSetValue(i, j, buffer._sum);
				aggCorr.quickSetValue(i, j, buffer._correction);
			}
		
		//note: nnz of aggVal/aggCorr maintained internally 
		aggVal.examSparsity();
		aggCorr.examSparsity();
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixLastRowDenseGeneric(MatrixBlock in, MatrixBlock aggVal) 
			throws DMLRuntimeException
	{
		if( in.denseBlock==null || in.isEmptyBlock(false) )
			return;
		
		final int m = in.rlen;
		final int n = in.clen;
		final int cix = (m-1)*n;
		
		double[] a = in.getDenseArray();
		
		KahanObject buffer = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		//incl implicit nnz maintenance
		for(int i=0, ix=0; i<m-1; i++)
			for(int j=0; j<n; j++, ix++)
			{
				buffer._sum = aggVal.quickGetValue(i, j);
				buffer._correction = aggVal.quickGetValue(m-1, j);
				akplus.execute(buffer, a[ix], a[cix+j]);
				aggVal.quickSetValue(i, j, buffer._sum);
				aggVal.quickSetValue(m-1, j, buffer._correction);
			}
		
		//note: nnz of aggVal maintained internally 
		aggVal.examSparsity();
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixLastRowSparseGeneric(MatrixBlock in, MatrixBlock aggVal) 
			throws DMLRuntimeException
	{
		//sparse-safe operation
		if( in.sparseRows==null || in.isEmptyBlock(false) )
			return;
		
		SparseRow[] a = in.getSparseRows();
		
		KahanObject buffer1 = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		final int m = in.rlen;
		final int rlen = Math.min(a.length, m);
		
		for( int i=0; i<rlen-1; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				int[] aix = arow.getIndexContainer();
				double[] avals = arow.getValueContainer();
				
				for( int j=0; j<alen; j++ )
				{
					int jix = aix[j];
					double corr = in.quickGetValue(m-1, jix);
					buffer1._sum        = aggVal.quickGetValue(i, jix);
					buffer1._correction = aggVal.quickGetValue(m-1, jix);
					akplus.execute(buffer1, avals[j], corr);
					aggVal.quickSetValue(i, jix, buffer1._sum);
					aggVal.quickSetValue(m-1, jix, buffer1._correction);
				}
			}
		}
		
		//note: nnz of aggVal/aggCorr maintained internally 
		aggVal.examSparsity(); 
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixLastColDenseGeneric(MatrixBlock in, MatrixBlock aggVal) 
			throws DMLRuntimeException
	{
		if( in.denseBlock==null || in.isEmptyBlock(false) )
			return;
		
		final int m = in.rlen;
		final int n = in.clen;
		
		double[] a = in.getDenseArray();
		
		KahanObject buffer = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		//incl implicit nnz maintenance
		for(int i=0, ix=0; i<m; i++, ix+=n)
			for(int j=0; j<n-1; j++)
			{
				buffer._sum = aggVal.quickGetValue(i, j);
				buffer._correction = aggVal.quickGetValue(i, n-1);
				akplus.execute(buffer, a[ix+j], a[ix+j+1]);
				aggVal.quickSetValue(i, j, buffer._sum);
				aggVal.quickSetValue(i, n-1, buffer._correction);
			}
		
		//note: nnz of aggVal maintained internally 
		aggVal.examSparsity();
	}
	
	/**
	 * 
	 * @param in
	 * @param aggVal
	 * @throws DMLRuntimeException
	 */
	private static void aggregateBinaryMatrixLastColSparseGeneric(MatrixBlock in, MatrixBlock aggVal) 
			throws DMLRuntimeException
	{
		//sparse-safe operation
		if( in.sparseRows==null || in.isEmptyBlock(false) )
			return;
		
		SparseRow[] a = in.getSparseRows();
		
		KahanObject buffer1 = new KahanObject(0, 0);
		KahanPlus akplus = KahanPlus.getKahanPlusFnObject();
		
		final int m = in.rlen;
		final int n = in.clen;
		final int rlen = Math.min(a.length, m);
		
		for( int i=0; i<rlen; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				int[] aix = arow.getIndexContainer();
				double[] avals = arow.getValueContainer();
				
				for( int j=0; j<alen && aix[j]<n-1; j++ )
				{
					int jix = aix[j];
					double corr = in.quickGetValue(i, n-1);
					buffer1._sum        = aggVal.quickGetValue(i, jix);
					buffer1._correction = aggVal.quickGetValue(i, n-1);
					akplus.execute(buffer1, avals[j], corr);
					aggVal.quickSetValue(i, jix, buffer1._sum);
					aggVal.quickSetValue(i, n-1, buffer1._correction);
				}
			}
		}
		
		//note: nnz of aggVal/aggCorr maintained internally 
		aggVal.examSparsity(); 
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param vFn
	 * @param ixFn
	 * @throws DMLRuntimeException
	 */
	private static void aggregateUnaryMatrixDense(MatrixBlock in, MatrixBlock out, AggType optype, ValueFunction vFn, IndexFunction ixFn, int rl, int ru) 
			throws DMLRuntimeException
	{
		final int m = in.rlen;
		final int n = in.clen;
		
		double[] a = in.getDenseArray();
		double[] c = out.getDenseArray();		
		
		switch( optype )
		{
			case KAHAN_SUM: //SUM/TRACE via k+, 
			{
				KahanObject kbuff = new KahanObject(0, 0);
				
				if( ixFn instanceof ReduceAll ) // SUM
					d_uakp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWSUM
					d_uarkp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLSUM
					d_uackp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceDiag ) //TRACE
					d_uakptrace(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				break;
			}
			case KAHAN_SUM_SQ: //SUM_SQ via k+,
			{
				KahanObject kbuff = new KahanObject(0, 0);

				if( ixFn instanceof ReduceAll ) //SUM_SQ
					d_uasqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWSUM_SQ
					d_uarsqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLSUM_SQ
					d_uacsqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				break;
			}
			case CUM_KAHAN_SUM: //CUMSUM
			{
				KahanObject kbuff = new KahanObject(0, 0);
				KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
				d_ucumkp(a, c, m, n, kbuff, kplus);
				break;
			}
			case CUM_PROD: //CUMPROD
			{
				d_ucumm(a, c, m, n);
				break;
			}
			case CUM_MIN:
			case CUM_MAX:
			{
				double init = Double.MAX_VALUE * ((optype==AggType.CUM_MAX)?-1:1);
				d_ucummxx(a, c, m, n, init, (Builtin)vFn);
				break;
			}
			case MIN: 
			case MAX: //MAX/MIN
			{
				double init = Double.MAX_VALUE * ((optype==AggType.MAX)?-1:1);
				
				if( ixFn instanceof ReduceAll ) // MIN/MAX
					d_uamxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWMIN/ROWMAX
					d_uarmxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLMIN/COLMAX
					d_uacmxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				
				break;
			}
			case MAX_INDEX:
			{
				double init = -Double.MAX_VALUE;
				
				if( ixFn instanceof ReduceCol ) //ROWINDEXMAX
					d_uarimxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				break;
			}
			case MIN_INDEX:
			{
				double init = Double.MAX_VALUE;
				
				if( ixFn instanceof ReduceCol ) //ROWINDEXMIN
					d_uarimin(a, c, m, n, init, (Builtin)vFn, rl, ru);
				break;
			}
			case MEAN: //MEAN
			{
				KahanObject kbuff = new KahanObject(0, 0);
				
				if( ixFn instanceof ReduceAll ) // MEAN
					d_uamean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWMEAN
					d_uarmean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLMEAN
					d_uacmean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				
				break;
			}
			case PROD: //PROD 
			{
				if( ixFn instanceof ReduceAll ) // PROD
					d_uam(a, c, m, n, rl, ru );
				break;
			}
			
			default:
				throw new DMLRuntimeException("Unsupported aggregation type: "+optype);
		}
	}
	
	/**
	 * 
	 * @param in
	 * @param out
	 * @param vFn
	 * @param ixFn
	 * @throws DMLRuntimeException
	 */
	private static void aggregateUnaryMatrixSparse(MatrixBlock in, MatrixBlock out, AggType optype, ValueFunction vFn, IndexFunction ixFn, int rl, int ru) 
			throws DMLRuntimeException
	{
		final int m = in.rlen;
		final int n = in.clen;
		
		SparseRow[] a = in.getSparseRows();
		double[] c = out.getDenseArray();
		
		switch( optype )
		{
			case KAHAN_SUM: //SUM via k+
			{
				KahanObject kbuff = new KahanObject(0, 0);
				
				if( ixFn instanceof ReduceAll ) // SUM
					s_uakp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWSUM
					s_uarkp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLSUM
					s_uackp(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				else if( ixFn instanceof ReduceDiag ) //TRACE
					s_uakptrace(a, c, m, n, kbuff, (KahanPlus)vFn, rl, ru);
				break;
			}
			case KAHAN_SUM_SQ: //SUM_SQ via k+
			{
				KahanObject kbuff = new KahanObject(0, 0);

				if( ixFn instanceof ReduceAll ) //SUM_SQ
					s_uasqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWSUM_SQ
					s_uarsqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLSUM_SQ
					s_uacsqkp(a, c, m, n, kbuff, (KahanPlusSq)vFn, rl, ru);
				break;
			}
			case CUM_KAHAN_SUM: //CUMSUM
			{
				KahanObject kbuff = new KahanObject(0, 0);
				KahanPlus kplus = KahanPlus.getKahanPlusFnObject();
				s_ucumkp(a, c, m, n, kbuff, kplus);
				break;
			}
			case CUM_PROD: //CUMPROD
			{
				s_ucumm(a, c, m, n);
				break;
			}
			case CUM_MIN:
			case CUM_MAX:
			{
				double init = Double.MAX_VALUE * ((optype==AggType.CUM_MAX)?-1:1);
				s_ucummxx(a, c, m, n, init, (Builtin)vFn);
				break;
			}
			case MIN:
			case MAX: //MAX/MIN
			{
				double init = Double.MAX_VALUE * ((optype==AggType.MAX)?-1:1);
				
				if( ixFn instanceof ReduceAll ) // MIN/MAX
					s_uamxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWMIN/ROWMAX
					s_uarmxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLMIN/COLMAX
					s_uacmxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				break;
			}
			case MAX_INDEX:
			{
				double init = -Double.MAX_VALUE;
				
				if( ixFn instanceof ReduceCol ) //ROWINDEXMAX
					s_uarimxx(a, c, m, n, init, (Builtin)vFn, rl, ru);
				break;
			}
			case MIN_INDEX:
			{
				double init = Double.MAX_VALUE;
				
				if( ixFn instanceof ReduceCol ) //ROWINDEXMAX
					s_uarimin(a, c, m, n, init, (Builtin)vFn, rl, ru);
				break;
			}
			case MEAN:
			{
				KahanObject kbuff = new KahanObject(0, 0);
				
				if( ixFn instanceof ReduceAll ) // MEAN
					s_uamean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				else if( ixFn instanceof ReduceCol ) //ROWMEAN
					s_uarmean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				else if( ixFn instanceof ReduceRow ) //COLMEAN
					s_uacmean(a, c, m, n, kbuff, (Mean)vFn, rl, ru);
				
				break;
			}
			case PROD: //PROD 
			{
				if( ixFn instanceof ReduceAll ) // PROD
					s_uam(a, c, m, n, rl, ru );
				break;
			}

			default:
				throw new DMLRuntimeException("Unsupported aggregation type: "+optype);
		}
	}

	/**
	 * 
	 * @param in
	 * @param out
	 * @param optype
	 * @param ixFn
	 * @throws DMLRuntimeException 
	 */
	private static void aggregateUnaryMatrixEmpty(MatrixBlock in, MatrixBlock out, AggType optype, IndexFunction ixFn) 
		throws DMLRuntimeException
	{
		//do nothing for pseudo sparse-safe operations
		if(optype==AggType.KAHAN_SUM || optype==AggType.KAHAN_SUM_SQ
				|| optype==AggType.MIN || optype==AggType.MAX || optype==AggType.PROD
				|| optype == AggType.CUM_KAHAN_SUM || optype == AggType.CUM_PROD
				|| optype == AggType.CUM_MIN || optype == AggType.CUM_MAX)
		{
			return;
		}
		
		//compute result based on meta data only
		switch( optype )
		{
			case MAX_INDEX:
			{
				if( ixFn instanceof ReduceCol ) { //ROWINDEXMAX
					for(int i=0; i<out.rlen; i++) {
						out.quickSetValue(i, 0, in.clen); //maxindex
					}
				}
				break;
			}
			case MIN_INDEX:
			{
				if( ixFn instanceof ReduceCol ) //ROWINDEXMIN
					for(int i=0; i<out.rlen; i++) {
						out.quickSetValue(i, 0, in.clen); //minindex
					}
				break;
			}
			case MEAN:
			{
				if( ixFn instanceof ReduceAll ) // MEAN
					out.quickSetValue(0, 1, in.rlen*in.clen); //count
				else if( ixFn instanceof ReduceCol ) //ROWMEAN
					for( int i=0; i<in.rlen; i++ ) //0-sum and 0-correction 
						out.quickSetValue(i, 1, in.clen); //count
				else if( ixFn instanceof ReduceRow ) //COLMEAN
					for( int j=0; j<in.clen; j++ ) //0-sum and 0-correction 
						out.quickSetValue(1, j, in.rlen); //count				
				break;
			}
			
			default:
				throw new DMLRuntimeException("Unsupported aggregation type: "+optype);
		}
	}
	
	
	////////////////////////////////////////////
	// core aggregation functions             //
	////////////////////////////////////////////

	/**
	 * SUM, opcode: uak+, dense input. 
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uakp( double[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru )
	{
		int len = Math.min((ru-rl)*n, a.length);
		sum( a, rl*n, len, kbuff, kplus );		
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;	
	}
	
	/**
	 * ROWSUM, opcode: uark+, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uarkp( double[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		for( int i=rl, aix=rl*n, cix=rl*2; i<ru; i++, aix+=n, cix+=2 )
		{
			kbuff.set(0, 0); //reset buffer
			sum( a, aix, n, kbuff, kplus );
			c[cix+0] = kbuff._sum;
			c[cix+1] = kbuff._correction;		
		}
	}
	
	/**
	 * COLSUM, opcode: uack+, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uackp( double[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		for( int i=rl, aix=rl*n; i<ru; i++, aix+=n )
			sumAgg( a, c, aix, 0, n, kbuff, kplus );
	}

	/**
	 * SUM_SQ, opcode: uasqk+, dense input.
	 *
	 * @param a Array of values to square & sum.
	 * @param c Output array to store sum and correction factor.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void d_uasqkp(double[] a, double[] c, int m, int n, KahanObject kbuff,
	                             KahanPlusSq kplusSq, int rl, int ru)
	{
		int len = Math.min((ru-rl)*n, a.length);
		sumSq(a, rl*n, len, kbuff, kplusSq);
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;
	}

	/**
	 * ROWSUM_SQ, opcode: uarsqk+, dense input.
	 *
	 * @param a Array of values to square & sum row-wise.
	 * @param c Output array to store sum and correction factor
	 *          for each row.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void d_uarsqkp(double[] a, double[] c, int m, int n, KahanObject kbuff,
	                              KahanPlusSq kplusSq, int rl, int ru)
	{
		for (int i=rl, aix=rl*n, cix=rl*2; i<ru; i++, aix+=n, cix+=2) {
			kbuff.set(0, 0); //reset buffer
			sumSq(a, aix, n, kbuff, kplusSq);
			c[cix+0] = kbuff._sum;
			c[cix+1] = kbuff._correction;
		}
	}

	/**
	 * COLSUM_SQ, opcode: uacsqk+, dense input.
	 *
	 * @param a Array of values to square & sum column-wise.
	 * @param c Output array to store sum and correction factor
	 *          for each column.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void d_uacsqkp(double[] a, double[] c, int m, int n, KahanObject kbuff,
	                              KahanPlusSq kplusSq, int rl, int ru)
	{
		for (int i=rl, aix=rl*n; i<ru; i++, aix+=n)
			sumSqAgg(a, c, aix, 0, n, kbuff, kplusSq);
	}

	/**
	 * CUMSUM, opcode: ucumk+, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_ucumkp( double[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus ) 
	{
		//init current row sum/correction arrays w/ neutral 0
		double[] csums = new double[ 2*n ]; 
		Arrays.fill(csums, 0); 
		
		//scan once and compute prefix sums
		for( int i=0, aix=0; i<m; i++, aix+=n ) {
			sumAgg( a, csums, aix, 0, n, kbuff, kplus );
			System.arraycopy(csums, 0, c, aix, n);	
		}
	}
	
	/**
	 * CUMPROD, opcode: ucum*, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_ucumm( double[] a, double[] c, int m, int n ) 
	{	
		//init current row product array w/ neutral 1
		double[] cprods = new double[ n ]; 
		Arrays.fill(cprods, 1);
		
		//scan once and compute prefix products
		for( int i=0, aix=0; i<m; i++, aix+=n ) {
			productAgg( a, cprods, aix, 0, n );
			System.arraycopy(cprods, 0, c, aix, n);
		}			
	}
	
	/**
	 * CUMMIN/CUMMAX, opcode: ucummin/ucummax, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param builtin
	 */
	private static void d_ucummxx( double[] a, double[] c, int m, int n, double init, Builtin builtin )
	{
		//init current row min/max array w/ extreme value 
		double[] cmxx = new double[ n ]; 
		Arrays.fill(cmxx, init);
				
		//scan once and compute prefix min/max
		for( int i=0, aix=0; i<m; i++, aix+=n ) {
			builtinAgg( a, cmxx, aix, n, builtin );
			System.arraycopy(cmxx, 0, c, aix, n);
		}
	}
	/**
	 * TRACE, opcode: uaktrace 
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uakptrace( double[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		//aggregate diag (via ix=n+1)
		for( int i=rl, aix=rl*n+rl; i<ru; i++, aix+=(n+1) )
			kplus.execute2(kbuff, a[ aix ]);			
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;	
	}
	
	/**
	 * MIN/MAX, opcode: uamin/uamax, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uamxx( double[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		int len = Math.min((ru-rl)*n, a.length);
		c[0] = builtin(a, rl*n, init, len, builtin);
	}
	
	/**
	 * ROWMIN/ROWMAX, opcode: uarmin/uarmax, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param builtin
	 */
	private static void d_uarmxx( double[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		for( int i=rl, aix=rl*n; i<ru; i++, aix+=n )
			c[i] = builtin(a, aix, init, n, builtin);
	}
	
	/**
	 * COLMIN/COLMAX, opcode: uacmin/uacmax, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param builtin
	 */
	private static void d_uacmxx( double[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		//init output (base for incremental agg)
		Arrays.fill(c, init);
		
		//execute builtin aggregate
		for( int i=rl, aix=rl*n; i<ru; i++, aix+=n )
			builtinAgg( a, c, aix, n, builtin );
	}

	/**
	 * ROWINDEXMAX, opcode: uarimax, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void d_uarimxx( double[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		for( int i=rl, aix=rl*n, cix=rl*2; i<ru; i++, aix+=n, cix+=2 )
		{
			int maxindex = indexmax(a, aix, init, n, builtin);
			c[cix+0] = (double)maxindex + 1;
			c[cix+1] = a[aix+maxindex]; //max value
		}
	}
	
	/**
	 * ROWINDEXMIN, opcode: uarimin, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void d_uarimin( double[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		for( int i=rl, aix=rl*n, cix=rl*2; i<ru; i++, aix+=n, cix+=2 )
		{
			int minindex = indexmin(a, aix, init, n, builtin);
			c[cix+0] = (double)minindex + 1;
			c[cix+1] = a[aix+minindex]; //min value
		}
	}
	
	/**
	 * MEAN, opcode: uamean, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uamean( double[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru )
	{
		int len = Math.min((ru-rl)*n, a.length);
		mean(a, rl*n, len, 0, kbuff, kmean);
		c[0] = kbuff._sum;
		c[1] = len;
		c[2] = kbuff._correction;
	}
	
	/**
	 * ROWMEAN, opcode: uarmean, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void d_uarmean( double[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru )
	{
		for( int i=rl, aix=rl*n, cix=rl*3; i<ru; i++, aix+=n, cix+=3 )
		{
			kbuff.set(0, 0); //reset buffer
			mean(a, aix, n, 0, kbuff, kmean);
			c[cix+0] = kbuff._sum;
			c[cix+1] = n;
			c[cix+2] = kbuff._correction;	
		}
	}
	
	/**
	 * COLMEAN, opcode: uacmean, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param builtin
	 */
	private static void d_uacmean( double[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru )
	{
		//init output (base for incremental agg)
		Arrays.fill(c, 0); //including counts
		
		//execute builtin aggregate
		for( int i=rl, aix=rl*n; i<ru; i++, aix+=n )
			meanAgg( a, c, aix, 0, n, kbuff, kmean );
	}
	
	
	/**
	 * PROD, opcode: ua*, dense input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 */
	private static void d_uam( double[] a, double[] c, int m, int n, int rl, int ru )
	{
		int len = Math.min((ru-rl)*n, a.length);
		c[0] = product( a, rl*n, len );	
	}
	
	
	/**
	 * SUM, opcode: uak+, sparse input. 
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uakp( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru )
	{
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				sum(avals, 0, alen, kbuff, kplus);
			}
		}
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;	
	}
	
	/**
	 * ROWSUM, opcode: uark+, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uarkp( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		//compute row aggregates
		for( int i=rl, cix=rl*2; i<ru; i++, cix+=2 )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				kbuff.set(0, 0); //reset buffer
				sum( avals, 0, alen, kbuff, kplus );
				c[cix+0] = kbuff._sum;
				c[cix+1] = kbuff._correction;			
			}
		}
	}
	
	/**
	 * COLSUM, opcode: uack+, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uackp( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		//init result (for empty columns)
		Arrays.fill(c, 0); 
				
		//compute column aggregates
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				sumAgg( avals, c, aix, alen, n, kbuff, kplus );
			}
		}
	}

	/**
	 * SUM_SQ, opcode: uasqk+, sparse input.
	 *
	 * @param a Sparse array of values to square & sum.
	 * @param c Output array to store sum and correction factor.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void s_uasqkp(SparseRow[] a, double[] c, int m, int n, KahanObject kbuff,
	                             KahanPlusSq kplusSq, int rl, int ru )
	{
		for (int i=rl; i<ru; i++) {
			SparseRow arow = a[i];
			if (arow!=null && !arow.isEmpty()) {
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				sumSq(avals, 0, alen, kbuff, kplusSq);
			}
		}
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;
	}

	/**
	 * ROWSUM_SQ, opcode: uarsqk+, sparse input.
	 *
	 * @param a Sparse array of values to square & sum row-wise.
	 * @param c Output array to store sum and correction factor
	 *          for each row.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void s_uarsqkp(SparseRow[] a, double[] c, int m, int n, KahanObject kbuff,
	                              KahanPlusSq kplusSq, int rl, int ru )
	{
		//compute row aggregates
		for (int i=rl, cix=rl*2; i<ru; i++, cix+=2) {
			SparseRow arow = a[i];
			if (arow!=null && !arow.isEmpty()) {
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				kbuff.set(0, 0); //reset buffer
				sumSq(avals, 0, alen, kbuff, kplusSq);
				c[cix+0] = kbuff._sum;
				c[cix+1] = kbuff._correction;
			}
		}
	}

	/**
	 * COLSUM_SQ, opcode: uacsqk+, sparse input.
	 *
	 * @param a Sparse array of values to square & sum column-wise.
	 * @param c Output array to store sum and correction factor
	 *          for each column.
	 * @param m Number of rows.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kplusSq A KahanPlusSq object to perform summation of
	 *                squared values.
	 * @param rl Lower row limit.
	 * @param ru Upper row limit.
	 */
	private static void s_uacsqkp(SparseRow[] a, double[] c, int m, int n, KahanObject kbuff,
	                              KahanPlusSq kplusSq, int rl, int ru )
	{
		//init result (for empty columns)
		Arrays.fill(c, 0);

		//compute column aggregates
		for (int i=rl; i<ru; i++) {
			SparseRow arow = a[i];
			if (arow!=null && !arow.isEmpty()) {
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				sumSqAgg(avals, c, aix, alen, n, kbuff, kplusSq);
			}
		}
	}

	/**
	 * CUMSUM, opcode: ucumk+, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_ucumkp( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus )
	{
		//init current row sum/correction arrays w/ neutral 0
		double[] csums = new double[ 2*n ]; 
		Arrays.fill(csums, 0);
		
		//scan once and compute prefix sums
		for( int i=0, ix=0; i<m; i++, ix+=n )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				sumAgg( avals, csums, aix, alen, n, kbuff, kplus );
			}
			//always copy current sum (not sparse-safe)
			System.arraycopy(csums, 0, c, ix, n);
		}
	}
	
	/**
	 * CUMPROD, opcode: ucum*, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 */
	private static void s_ucumm( SparseRow[] a, double[] c, int m, int n )
	{
		//init current row prod arrays w/ neutral 1
		double[] cprod = new double[ n ]; 
		Arrays.fill(cprod, 1);
		
		//init count arrays (helper, see correction)
		int[] cnt = new int[ n ]; 
		Arrays.fill(cnt, 0); //init count array
				
		//scan once and compute prefix products
		for( int i=0, ix=0; i<m; i++, ix+=n )
		{
			SparseRow arow = a[i];
			
			//multiply row of non-zero elements
			if( arow!=null && !arow.isEmpty() ) {
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				productAgg( avals, cprod, aix, 0, alen );
				countAgg( avals, cnt, aix, alen );
			}

			//correction (not sparse-safe and cumulative)
			//note: we need to determine if there are only nnz in a column
			for( int j=0; j<n; j++ )
				if( cnt[j] < i+1 ) //no dense column
					cprod[j] *= 0;
			
			//always copy current sum (not sparse-safe)
			System.arraycopy(cprod, 0, c, ix, n);
		}	
	}
	
	/**
	 * CUMMIN/CUMMAX, opcode: ucummin/ucummax, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_ucummxx( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin ) 
	{
		//init current row min/max array w/ extreme value 
		double[] cmxx = new double[ n ]; 
		Arrays.fill(cmxx, init);
				
		//init count arrays (helper, see correction)
		int[] cnt = new int[ n ]; 
		Arrays.fill(cnt, 0); //init count array
		
		//compute column aggregates min/max
		for( int i=0, ix=0; i<m; i++, ix+=n )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				builtinAgg( avals, cmxx, aix, alen, builtin );
				countAgg( avals, cnt, aix, alen );
			}
			
			//correction (not sparse-safe and cumulative)
			//note: we need to determine if there are only nnz in a column
			for( int j=0; j<n; j++ )
				if( cnt[j] < i+1 ) //no dense column
					cmxx[j] = builtin.execute2(cmxx[j], 0);
			
			//always copy current sum (not sparse-safe)
			System.arraycopy(cmxx, 0, c, ix, n);
		}
	}
	
	/**
	 * TRACE, opcode: uaktrace, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uakptrace( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, KahanPlus kplus, int rl, int ru ) 
	{
		for( int i=rl; i<ru; i++ ) {
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() ) 
			{
				double val = arow.get( i );
				kplus.execute2(kbuff, val);
			}
		}
		c[0] = kbuff._sum;
		c[1] = kbuff._correction;	
	}
	
	/**
	 * MIN/MAX, opcode: uamin/uamax, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_uamxx( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru )
	{
		double ret = init; //keep init val
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				double lval = builtin(avals, 0, init, alen, builtin);
				ret = builtin.execute2(ret, lval);
			}
		
			//correction (not sparse-safe)
			if( arow==null || arow.size()<n )
				ret = builtin.execute2(ret, 0); 
		}
		c[0] = ret; 
	}
	
	/**
	 * ROWMIN/ROWMAX, opcode: uarmin/uarmax, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_uarmxx( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru ) 
	{
		//init result (for empty rows)
		Arrays.fill(c, rl, ru, init); //not sparse-safe
		
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				c[ i ] = builtin(avals, 0, init, alen, builtin);
			}
			//correction (not sparse-safe)
			if( arow==null || arow.size()<n )
				c[ i ] = builtin.execute2(c[ i ], 0); 
		}
	}
	
	/**
	 * COLMIN/COLMAX, opcode: uacmin/uacmax, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_uacmxx( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru ) 
	{
		//init output (base for incremental agg)
		Arrays.fill(c, init);
		
		//init count arrays (helper, see correction)
		int[] cnt = new int[ n ]; 
		Arrays.fill(cnt, 0); //init count array
		
		//compute column aggregates min/max
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				builtinAgg( avals, c, aix, alen, builtin );
				countAgg( avals, cnt, aix, alen );
			}
		}
		
		//correction (not sparse-safe)
		//note: we need to determine if there are only nnz in a column
		// in order to know if for example a colMax of -8 is true or need
		// to be replaced with a 0 because there was a missing nonzero. 
		for( int i=0; i<n; i++ )
			if( cnt[i] < m ) //no dense column
				c[i] = builtin.execute2(c[i], 0);	
	}

	/**
	 * ROWINDEXMAX, opcode: uarimax, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_uarimxx( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru ) 
	{
		for( int i=rl, cix=rl*2; i<ru; i++, cix+=2 )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				int maxindex = indexmax(avals, 0, init, alen, builtin);
				c[cix+0] = (double)aix[maxindex] + 1;
				c[cix+1] = avals[maxindex]; //max value
				
				//correction (not sparse-safe)	
				if(alen < n && (builtin.execute2( 0, c[cix+1] ) == 1))
				{
					int ix = n-1; //find last 0 value
					for( int j=alen-1; j>=0; j--, ix-- )
						if( aix[j]!=ix )
							break;
					c[cix+0] = ix + 1; //max index (last)
					c[cix+1] = 0; //max value
				}
			}
			else //if( arow==null )
			{
				//correction (not sparse-safe)	
				c[cix+0] = n; //max index (last)
				c[cix+1] = 0; //max value
			}
		}
	}
	
	/**
	 * ROWINDEXMIN, opcode: uarimin, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param builtin
	 */
	private static void s_uarimin( SparseRow[] a, double[] c, int m, int n, double init, Builtin builtin, int rl, int ru ) 
	{
		for( int i=rl, cix=rl*2; i<ru; i++, cix+=2 )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				int minindex = indexmin(avals, 0, init, alen, builtin);
				c[cix+0] = (double)aix[minindex] + 1;
				c[cix+1] = avals[minindex]; //min value among non-zeros
				
				//correction (not sparse-safe)	
				if(alen < n && (builtin.execute2( 0, c[cix+1] ) == 1))
				{
					int ix = n-1; //find last 0 value
					for( int j=alen-1; j>=0; j--, ix-- )
						if( aix[j]!=ix )
							break;
					c[cix+0] = ix + 1; //min index (last)
					c[cix+1] = 0; //min value
				}
			}
			else //if( arow==null )
			{
				//correction (not sparse-safe)	
				c[cix+0] = n; //min index (last)
				c[cix+1] = 0; //min value
			}
		}
	}
	
	/**
	 * MEAN, opcode: uamean, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uamean( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru )
	{
		int len = (ru-rl) * n;
		int count = 0;
		
		//correction remaining tuples (not sparse-safe)
		//note: before aggregate computation in order to
		//exploit 0 sum (noop) and better numerical stability
		for( int i=rl; i<ru; i++ )
			count += (a[i]==null)? n : n-a[i].size();
		
		//compute aggregate mean
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				mean(avals, 0, alen, count, kbuff, kmean);
				count += alen;
			}
		}

		//OLD VERSION: correction remaining tuples (not sparse-safe)
		//mean(0, len-count, count, kbuff, kplus);
		
		c[0] = kbuff._sum;
		c[1] = len;
		c[2] = kbuff._correction;
	}
	
	/**
	 * ROWMEAN, opcode: uarmean, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uarmean( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru ) 
	{
		for( int i=rl, cix=rl*3; i<ru; i++, cix+=3 )
		{
			//correction remaining tuples (not sparse-safe)
			//note: before aggregate computation in order to
			//exploit 0 sum (noop) and better numerical stability
			int count = (a[i]==null)? n : n-a[i].size();
			
			kbuff.set(0, 0); //reset buffer
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				mean(avals, 0, alen, count, kbuff, kmean);
			}
			
			//OLD VERSION: correction remaining tuples (not sparse-safe)
			//int count = ((arow==null) ? 0 : arow.size());
			//mean(0, n-count, count, kbuff, kplus);
			
			c[cix+0] = kbuff._sum;
			c[cix+1] = n;
			c[cix+2] = kbuff._correction;
		}
	}
	
	/**
	 * COLMEAN, opcode: uacmean, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 * @param init
	 * @param kbuff
	 * @param kplus
	 */
	private static void s_uacmean( SparseRow[] a, double[] c, int m, int n, KahanObject kbuff, Mean kmean, int rl, int ru ) 
	{
		//init output (base for incremental agg)
		Arrays.fill(c, 0);
		
		//correction remaining tuples (not sparse-safe)
		//note: before aggregate computation in order to
		//exploit 0 sum (noop) and better numerical stability
		Arrays.fill(c, n, n*2, ru-rl);
		for( int i=rl; i<ru; i++ ) 
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				countDisAgg( avals, c, aix, n, alen );
			}
		} 
		
		//compute column aggregate means
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				int[] aix = arow.getIndexContainer();
				meanAgg( avals, c, aix, alen, n, kbuff, kmean );
			}
		}
	}
	
	/**
	 * PROD, opcode: ua*, sparse input.
	 * 
	 * @param a
	 * @param c
	 * @param m
	 * @param n
	 */
	private static void s_uam( SparseRow[] a, double[] c, int m, int n, int rl, int ru )
	{
		double ret = 1;
		for( int i=rl; i<ru; i++ )
		{
			SparseRow arow = a[i];
			if( arow!=null && !arow.isEmpty() )
			{
				int alen = arow.size();
				double[] avals = arow.getValueContainer();
				ret *= product(avals, 0, alen);
				ret *= (alen<n) ? 0 : 1;
			}
			
			//early abort (note: in case of NaNs this is an invalid optimization)
			if( !NAN_AWARENESS && ret==0 ) break;
		}
		c[0] = ret;
	}
	
	
	////////////////////////////////////////////
	// performance-relevant utility functions //
	////////////////////////////////////////////
	
	/**
	 * Summation using the Kahan summation algorithm with the
	 * KahanPlus function.
	 */
	private static void sum(double[] a, int ai, final int len, KahanObject kbuff, KahanPlus kplus)
	{
		sumWithFn(a, ai, len, kbuff, kplus);
	}

	/**
	 * Aggregated summation using the Kahan summation algorithm with
	 * the KahanPlus function.
	 */
	private static void sumAgg(double[] a, double[] c, int ai, int ci, final int len,
	                           KahanObject kbuff, KahanPlus kplus)
	{
		sumAggWithFn(a, c, ai, ci, len, kbuff, kplus);
	}
	
	/**
	 * Aggregated summation using the Kahan summation algorithm with
	 * the KahanPlus function.
	 */
	private static void sumAgg(double[] a, double[] c, int[] ai, final int len, final int n,
	                           KahanObject kbuff, KahanPlus kplus)
	{
		sumAggWithFn(a, c, ai, len, n, kbuff, kplus);
	}

	/**
	 * Summation of squared values using the Kahan summation algorithm
	 * with the KahanPlusSq function.
	 */
	private static void sumSq(double[] a, int ai, final int len,
	                          KahanObject kbuff, KahanPlusSq kplusSq)
	{
		sumWithFn(a, ai, len, kbuff, kplusSq);
	}

	/**
	 * Aggregated summation of squared values using the Kahan
	 * summation algorithm with the KahanPlusSq function.
	 */
	private static void sumSqAgg(double[] a, double[] c, int ai, int ci, final int len,
	                             KahanObject kbuff, KahanPlusSq kplusSq)
	{
		sumAggWithFn(a, c, ai, ci, len, kbuff, kplusSq);
	}

	/**
	 * Aggregated summation of squared values using the Kahan
	 * summation algorithm with the KahanPlusSq function.
	 */
	private static void sumSqAgg(double[] a, double[] c, int[] ai, final int len, final int n,
	                             KahanObject kbuff, KahanPlusSq kplusSq)
	{
		sumAggWithFn(a, c, ai, len, n, kbuff, kplusSq);
	}

	/**
	 * Summation using the Kahan summation algorithm with one of the
	 * Kahan functions.
	 *
	 * @param a Array of values to sum.
	 * @param ai Index at which to start processing.
	 * @param len Number of values to process, starting at index ai.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kfunc A KahanFunction object to perform the summation.
	 */
	private static void sumWithFn(double[] a, int ai, final int len,
	                              KahanObject kbuff, KahanFunction kfunc)
	{
		for (int i=0; i<len; i++, ai++)
			kfunc.execute2(kbuff, a[ai]);
	}

	/**
	 * Aggregated summation using the Kahan summation algorithm
	 * with one of the Kahan functions.
	 *
	 * @param a Array of values to sum.
	 * @param c Output array to store aggregated sum and correction
	 *          factors.
	 * @param ai Index at which to start processing array `a`.
	 * @param ci Index at which to start storing aggregated results
	 *           into array `c`.
	 * @param len Number of values to process, starting at index ai.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kfunc A KahanFunction object to perform the summation.
	 */
	private static void sumAggWithFn(double[] a, double[] c, int ai, int ci, final int len,
	                                 KahanObject kbuff, KahanFunction kfunc)
	{
		for (int i=0; i<len; i++, ai++, ci++) {
			kbuff._sum = c[ci];
			kbuff._correction = c[ci+len];
			kfunc.execute2(kbuff, a[ai]);
			c[ci] = kbuff._sum;
			c[ci+len] = kbuff._correction;
		}
	}

	/**
	 * Aggregated summation using the Kahan summation algorithm
	 * with one of the Kahan functions.
	 *
	 * @param a Array of values to sum.
	 * @param c Output array to store aggregated sum and correction
	 *          factors.
	 * @param ai Array of indices to process for array `a`.
	 * @param len Number of indices in `ai` to process.
	 * @param n Number of values per row.
	 * @param kbuff A KahanObject to hold the current sum and
	 *              correction factor for the Kahan summation
	 *              algorithm.
	 * @param kfunc A KahanFunction object to perform the summation.
	 */
	private static void sumAggWithFn(double[] a, double[] c, int[] ai, final int len, final int n,
	                                 KahanObject kbuff, KahanFunction kfunc)
	{
		for (int i=0; i<len; i++) {
			kbuff._sum = c[ai[i]];
			kbuff._correction = c[ai[i]+n];
			kfunc.execute2(kbuff, a[i]);
			c[ai[i]] = kbuff._sum;
			c[ai[i]+n] = kbuff._correction;
		}
	}
	/**
	 * 
	 * @param a
	 * @param len
	 * @return
	 */
	private static double product( double[] a, int ai, final int len )
	{
		double val = 1;
		
		if( NAN_AWARENESS )
		{
			//product without early abort
			//even if val is 0, it might turn into NaN.
			for( int i=0; i<len; i++, ai++ )
				val *= a[ ai ];	
		}
		else
		{
			//product with early abort (if 0)
			//note: this will not work with NaNs (invalid optimization)
			for( int i=0; i<len && val!=0; i++, ai++ )
				val *= a[ ai ];
		}
		
		return val;
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param ci
	 * @param len
	 */
	private static void productAgg( double[] a, double[] c, int ai, int ci, final int len )
	{
		//always w/ NAN_AWARENESS: product without early abort; 
		//even if val is 0, it might turn into NaN.
		//(early abort would require column-flags and branches)
		for( int i=0; i<len; i++, ai++, ci++ )
			c[ ci ] *= a[ ai ];	
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param ci
	 * @param len
	 */
	private static void productAgg( double[] a, double[] c, int[] ai, int ci, final int len )
	{
		//always w/ NAN_AWARENESS: product without early abort; 
		//even if val is 0, it might turn into NaN.
		//(early abort would require column-flags and branches)
		for( int i=0; i<len; i++ )
			c[ ci + ai[i] ] *= a[ i ];	
	}
	
	/**
	 * 
	 * @param a
	 * @param ai
	 * @param len
	 * @param kbuff
	 * @param kplus
	 */
	private static void mean( double[] a, int ai, final int len, int count, KahanObject kbuff, Mean mean )
	{
		for( int i=0; i<len; i++, ai++, count++ )
		{
			//delta: (newvalue-buffer._sum)/count
			mean.execute2(kbuff, a[ai], count+1);
		}
	}
	
	/*
	private static void mean( final double aval, final int len, int count, KahanObject kbuff, KahanPlus kplus )
	{
		for( int i=0; i<len; i++, count++ )
		{
			//delta: (newvalue-buffer._sum)/count
			kplus.execute2(kbuff, (aval-kbuff._sum)/(count+1));
		}
	}
	*/
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param ci
	 * @param len
	 * @param kbuff
	 * @param kplus
	 */
	private static void meanAgg( double[] a, double[] c, int ai, int ci, final int len, KahanObject kbuff, Mean mean )
	{
		for( int i=0; i<len; i++, ai++, ci++ )
		{
			kbuff._sum        = c[ci];
			double count      = c[ci+len] + 1;
			kbuff._correction = c[ci+2*len];
			mean.execute2(kbuff, a[ai], count);
			c[ci]       = kbuff._sum;
			c[ci+len]   = count;
			c[ci+2*len] = kbuff._correction;
		}
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param len
	 * @param kbuff
	 * @param kplus
	 */
	private static void meanAgg( double[] a, double[] c, int[] ai, final int len, final int n, KahanObject kbuff, Mean mean )
	{
		for( int i=0; i<len; i++ )
		{
			kbuff._sum        = c[ai[i]];
			double count      = c[ai[i]+n] + 1;
			kbuff._correction = c[ai[i]+2*n];
			mean.execute2(kbuff, a[ i ], count);
			c[ai[i]]     = kbuff._sum;
			c[ai[i]+n]   = count;
			c[ai[i]+2*n] = kbuff._correction;
		}
	}
	
	/**
	 * Meant for builtin function ops (min, max) 
	 * 
	 * @param a
	 * @param ai
	 * @param init
	 * @param len
	 * @param aggop
	 * @return
	 * @throws DMLRuntimeException
	 */
	private static double builtin( double[] a, int ai, final double init, final int len, Builtin aggop ) 
	{
		double val = init;
		for( int i=0; i<len; i++, ai++ )
			val = aggop.execute2( val, a[ ai ] );
		
		return val;
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param len
	 * @param aggop
	 */
	private static void builtinAgg( double[] a, double[] c, int ai, final int len, Builtin aggop ) 
	{
		for( int i=0; i<len; i++, ai++ )
			c[ i ] = aggop.execute2( c[ i ], a[ ai ] );
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param len
	 * @param aggop
	 */
	private static void builtinAgg( double[] a, double[] c, int[] ai, final int len, Builtin aggop ) 
	{
		for( int i=0; i<len; i++ )
			c[ ai[i] ] = aggop.execute2( c[ ai[i] ], a[ i ] );
	}
	
	/**
	 * 
	 * @param a
	 * @param ai
	 * @param init
	 * @param len
	 * @param aggop
	 * @return
	 */
	private static int indexmax( double[] a, int ai, final double init, final int len, Builtin aggop ) 
	{
		double maxval = init;
		int maxindex = -1;
		
		for( int i=ai; i<ai+len; i++ ) {
			maxindex = (a[i]>=maxval) ? i-ai : maxindex;
			maxval = (a[i]>=maxval) ? a[i] : maxval;
		}

		return maxindex;
	}
	
	/**
	 * 
	 * @param a
	 * @param ai
	 * @param init
	 * @param len
	 * @param aggop
	 * @return
	 */
	private static int indexmin( double[] a, int ai, final double init, final int len, Builtin aggop ) 
	{
		double minval = init;
		int minindex = -1;
		
		for( int i=ai; i<ai+len; i++ ) {
			minindex = (a[i]<=minval) ? i-ai : minindex;
			minval = (a[i]<=minval) ? a[i] : minval;
		}
		
		return minindex;
	}
	
	/**
	 * 
	 * @param a
	 * @param c
	 * @param ai
	 * @param len
	 */
	private static void countAgg( double[] a, int[] c, int[] ai, final int len ) 
	{
		final int bn = len%8;
		
		//compute rest, not aligned to 8-block
		for( int i=0; i<bn; i++ )
			c[ ai[i] ]++;
		
		//unrolled 8-block (for better instruction level parallelism)
		for( int i=bn; i<len; i+=8 )
		{
			c[ ai[ i+0 ] ] ++;
			c[ ai[ i+1 ] ] ++;
			c[ ai[ i+2 ] ] ++;
			c[ ai[ i+3 ] ] ++;
			c[ ai[ i+4 ] ] ++;
			c[ ai[ i+5 ] ] ++;
			c[ ai[ i+6 ] ] ++;
			c[ ai[ i+7 ] ] ++;
		}
	}
	
	private static void countDisAgg( double[] a, double[] c, int[] ai, final int ci, final int len ) 
	{
		final int bn = len%8;
		
		//compute rest, not aligned to 8-block
		for( int i=0; i<bn; i++ )
			c[ ci+ai[i] ]--;
		
		//unrolled 8-block (for better instruction level parallelism)
		for( int i=bn; i<len; i+=8 )
		{
			c[ ci+ai[ i+0 ] ] --;
			c[ ci+ai[ i+1 ] ] --;
			c[ ci+ai[ i+2 ] ] --;
			c[ ci+ai[ i+3 ] ] --;
			c[ ci+ai[ i+4 ] ] --;
			c[ ci+ai[ i+5 ] ] --;
			c[ ci+ai[ i+6 ] ] --;
			c[ ci+ai[ i+7 ] ] --;
		}
	}
	
	/////////////////////////////////////////////////////////
	// Task Implementations for Multi-Threaded Operations  //
	/////////////////////////////////////////////////////////
	
	private static abstract class AggTask implements Callable<Object> {}
	
	/**
	 * 
	 * 
	 */
	private static class RowAggTask extends AggTask 
	{
		private MatrixBlock _in  = null;
		private MatrixBlock _ret = null;
		private AggType _aggtype = null;
		private AggregateUnaryOperator _uaop = null;		
		private int _rl = -1;
		private int _ru = -1;

		protected RowAggTask( MatrixBlock in, MatrixBlock ret, AggType aggtype, AggregateUnaryOperator uaop, int rl, int ru )
		{
			_in = in;
			_ret = ret;
			_aggtype = aggtype;
			_uaop = uaop;
			_rl = rl;
			_ru = ru;
		}
		
		@Override
		public Object call() throws DMLRuntimeException
		{
			if( !_in.sparse )
				aggregateUnaryMatrixDense(_in, _ret, _aggtype, _uaop.aggOp.increOp.fn, _uaop.indexFn, _rl, _ru);
			else
				aggregateUnaryMatrixSparse(_in, _ret, _aggtype, _uaop.aggOp.increOp.fn, _uaop.indexFn, _rl, _ru);
			
			return null;
		}
	}

	/**
	 * 
	 * 
	 */
	private static class PartialAggTask extends AggTask 
	{
		private MatrixBlock _in  = null;
		private MatrixBlock _ret = null;
		private AggType _aggtype = null;
		private AggregateUnaryOperator _uaop = null;		
		private int _rl = -1;
		private int _ru = -1;

		protected PartialAggTask( MatrixBlock in, MatrixBlock ret, AggType aggtype, AggregateUnaryOperator uaop, int rl, int ru ) 
			throws DMLRuntimeException
		{
			_in = in;			
			_aggtype = aggtype;
			_uaop = uaop;
			_rl = rl;
			_ru = ru;
			
			//allocate local result for partial aggregation
			_ret = new MatrixBlock(ret.rlen, ret.clen, false);
			_ret.allocateDenseBlock();
		}
		
		@Override
		public Object call() throws DMLRuntimeException
		{
			if( !_in.sparse )
				aggregateUnaryMatrixDense(_in, _ret, _aggtype, _uaop.aggOp.increOp.fn, _uaop.indexFn, _rl, _ru);
			else
				aggregateUnaryMatrixSparse(_in, _ret, _aggtype, _uaop.aggOp.increOp.fn, _uaop.indexFn, _rl, _ru);
			
			//recompute non-zeros of partial result
			_ret.recomputeNonZeros();
			
			return null;
		}
		
		public MatrixBlock getResult() {
			return _ret;
		}
	}
	
	/**
	 * 
	 */
	private static class AggTernaryTask extends AggTask 
	{
		private MatrixBlock _in1  = null;
		private MatrixBlock _in2  = null;
		private MatrixBlock _in3  = null;
		private double _ret = -1;
		private int _rl = -1;
		private int _ru = -1;

		protected AggTernaryTask( MatrixBlock in1, MatrixBlock in2, MatrixBlock in3, int rl, int ru ) 
			throws DMLRuntimeException
		{
			_in1 = in1;	
			_in2 = in2;	
			_in3 = in3;				
			_rl = rl;
			_ru = ru;
		}
		
		@Override
		public Object call() throws DMLRuntimeException
		{
			if( !_in1.sparse && !_in2.sparse && (_in3==null||!_in3.sparse) ) //DENSE
				_ret = aggregateTernaryDense(_in1, _in2, _in3, _rl, _ru);
			else //GENERAL CASE
				_ret = aggregateTernaryGeneric(_in1, _in2, _in3, _rl, _ru);
			
			return null;
		}
		
		public double getResult() {
			return _ret;
		}
	}
}

