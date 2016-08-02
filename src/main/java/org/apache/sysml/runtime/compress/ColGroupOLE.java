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

package org.apache.sysml.runtime.compress;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.compress.utils.ConverterUtils;
import org.apache.sysml.runtime.compress.utils.LinearAlgebraUtils;
import org.apache.sysml.runtime.functionobjects.KahanFunction;
import org.apache.sysml.runtime.functionobjects.KahanPlus;
import org.apache.sysml.runtime.functionobjects.KahanPlusSq;
import org.apache.sysml.runtime.functionobjects.ReduceAll;
import org.apache.sysml.runtime.functionobjects.ReduceCol;
import org.apache.sysml.runtime.functionobjects.ReduceRow;
import org.apache.sysml.runtime.instructions.cp.KahanObject;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysml.runtime.matrix.operators.ScalarOperator;

/**
 * Class to encapsulate information about a column group that is encoded with
 * simple lists of offsets for each set of distinct values.
 * 
 */
public class ColGroupOLE extends ColGroupBitmap 
{
	private static final long serialVersionUID = -9157676271360528008L;

	public ColGroupOLE() {
		super(CompressionType.OLE_BITMAP);
	}
	
	/**
	 * Main constructor. Constructs and stores the necessary bitmaps.
	 * 
	 * @param colIndices
	 *            indices (within the block) of the columns included in this
	 *            column
	 * @param numRows
	 *            total number of rows in the parent block
	 * @param ubm
	 *            Uncompressed bitmap representation of the block
	 */
	public ColGroupOLE(int[] colIndices, int numRows, UncompressedBitmap ubm) 
	{
		super(CompressionType.OLE_BITMAP, colIndices, numRows, ubm);

		// compress the bitmaps
		final int numVals = ubm.getNumValues();
		char[][] lbitmaps = new char[numVals][];
		int totalLen = 0;
		for( int i=0; i<numVals; i++ ) {
			lbitmaps[i] = BitmapEncoder.genOffsetBitmap(ubm.getOffsetsList(i));
			totalLen += lbitmaps[i].length;
		}
		
		// compact bitmaps to linearized representation
		createCompressedBitmaps(numVals, totalLen, lbitmaps);
		

		if( LOW_LEVEL_OPT && CREATE_SKIPLIST
				&& numRows > 2*BitmapEncoder.BITMAP_BLOCK_SZ )
		{
			int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
			_skiplist = new int[numVals];
			int rl = (getNumRows()/2/blksz)*blksz;
			for (int k = 0; k < numVals; k++) {
				int bitmapOff = _ptr[k];
				int bitmapLen = len(k);
				int bufIx = 0;
				for( int i=0; i<rl && bufIx<bitmapLen; i+=blksz ) {
					bufIx += _data[bitmapOff+bufIx] + 1;
				}
				_skiplist[k] = bufIx;
			}		
		}
	}

	/**
	 * Constructor for internal use.
	 * 
	 * @param colIndices
	 *            indices (within the block) of the columns included in this
	 *            column
	 * @param numRows
	 *            total number of rows in the parent block
	 * @param values the values
	 * @param bitmaps the bitmaps
	 * @param bitmapOffs the bitmap offsets
	 */
	public ColGroupOLE(int[] colIndices, int numRows, double[] values, char[] bitmaps, int[] bitmapOffs) {
		super(CompressionType.OLE_BITMAP, colIndices, numRows, values);
		_data = bitmaps;
		_ptr = bitmapOffs;
	}

	@Override
	public Iterator<Integer> getDecodeIterator(int bmpIx) {
		return new BitmapDecoderOLE(_data, _ptr[bmpIx], len(bmpIx));
	}
	
	@Override
	public void decompressToBlock(MatrixBlock target) 
	{
		if( LOW_LEVEL_OPT && getNumValues() > 1 )
		{
			final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
			final int numCols = getNumCols();
			final int numVals = getNumValues();
			final int n = getNumRows();
			
			//cache blocking config and position array
			int[] apos = new int[numVals];
					
			//cache conscious append via horizontal scans 
			for( int bi=0; bi<n; bi+=blksz ) {
				for (int k = 0, off=0; k < numVals; k++, off+=numCols) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);					
					int bufIx = apos[k];
					if( bufIx >= bitmapLen ) 
						continue;
					int len = _data[bitmapOff+bufIx];
					int pos = bitmapOff+bufIx+1;
					for( int i=pos; i<pos+len; i++ )
						for( int j=0, rix = bi+_data[i]; j<numCols; j++ )
							if( _values[off+j]!=0 )
								target.appendValue(rix, _colIndexes[j], _values[off+j]);
					apos[k] += len + 1;
				}
			}		
		}
		else
		{
			//call generic decompression with decoder
			super.decompressToBlock(target);
		}
	}

	@Override
	public void decompressToBlock(MatrixBlock target, int[] colixTargets) 
	{
		if( LOW_LEVEL_OPT && getNumValues() > 1 )
		{
			final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
			final int numCols = getNumCols();
			final int numVals = getNumValues();
			final int n = getNumRows();
			
			//cache blocking config and position array
			int[] apos = new int[numVals];					
			int[] cix = new int[numCols];
			
			//prepare target col indexes
			for( int j=0; j<numCols; j++ )
				cix[j] = colixTargets[_colIndexes[j]];
			
			//cache conscious append via horizontal scans 
			for( int bi=0; bi<n; bi+=blksz ) {
				for (int k = 0, off=0; k < numVals; k++, off+=numCols) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);					
					int bufIx = apos[k];
					if( bufIx >= bitmapLen ) 
						continue;
					int len = _data[bitmapOff+bufIx];
					int pos = bitmapOff+bufIx+1;
					for( int i=pos; i<pos+len; i++ )
						for( int j=0, rix = bi+_data[i]; j<numCols; j++ )
							if( _values[off+j]!=0 )
								target.appendValue(rix, cix[j], _values[off+j]);
					apos[k] += len + 1;
				}
			}		
		}
		else
		{
			//call generic decompression with decoder
			super.decompressToBlock(target, colixTargets);
		}
	}
	
	@Override
	public void decompressToBlock(MatrixBlock target, int colpos) 
	{
		if( LOW_LEVEL_OPT && getNumValues() > 1 )
		{
			final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
			final int numCols = getNumCols();
			final int numVals = getNumValues();
			final int n = getNumRows();
			double[] c = target.getDenseBlock();
			
			//cache blocking config and position array
			int[] apos = new int[numVals];					
			
			//cache conscious append via horizontal scans 
			for( int bi=0; bi<n; bi+=blksz ) {
				for (int k = 0, off=0; k < numVals; k++, off+=numCols) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);					
					int bufIx = apos[k];
					if( bufIx >= bitmapLen ) 
						continue;
					int len = _data[bitmapOff+bufIx];
					int pos = bitmapOff+bufIx+1;
					for( int i=pos; i<pos+len; i++ ) {
						c[bi+_data[i]] = _values[off+colpos];
					}
					apos[k] += len + 1;
				}
			}	
			
			target.recomputeNonZeros();
		}
		else
		{
			//call generic decompression with decoder
			super.decompressToBlock(target, colpos);
		}
	}
	
	@Override
	public ColGroup scalarOperation(ScalarOperator op)
		throws DMLRuntimeException 
	{
		double val0 = op.executeScalar(0);
		
		//fast path: sparse-safe operations
		// Note that bitmaps don't change and are shallow-copied
		if( op.sparseSafe || val0==0 ) {
			return new ColGroupOLE(_colIndexes, _numRows, 
					applyScalarOp(op), _data, _ptr);
		}
		
		//slow path: sparse-unsafe operations (potentially create new bitmap)
		//note: for efficiency, we currently don't drop values that become 0
		boolean[] lind = computeZeroIndicatorVector();
		int[] loff = computeOffsets(lind);
		if( loff.length==0 ) { //empty offset list: go back to fast path
			return new ColGroupOLE(_colIndexes, _numRows, 
					applyScalarOp(op), _data, _ptr);
		}
		
		double[] rvalues = applyScalarOp(op, val0, getNumCols());		
		char[] lbitmap = BitmapEncoder.genOffsetBitmap(loff);
		char[] rbitmaps = Arrays.copyOf(_data, _data.length+lbitmap.length);
		System.arraycopy(lbitmap, 0, rbitmaps, _data.length, lbitmap.length);
		int[] rbitmapOffs = Arrays.copyOf(_ptr, _ptr.length+1);
		rbitmapOffs[rbitmapOffs.length-1] = rbitmaps.length; 
		
		return new ColGroupOLE(_colIndexes, _numRows, 
				rvalues, rbitmaps, rbitmapOffs);
	}

	@Override
	public void rightMultByVector(MatrixBlock vector, MatrixBlock result, int rl, int ru)
			throws DMLRuntimeException 
	{
		double[] b = ConverterUtils.getDenseVector(vector);
		double[] c = result.getDenseBlock();
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int numCols = getNumCols();
		final int numVals = getNumValues();
		
		//prepare reduced rhs w/ relevant values
		double[] sb = new double[numCols];
		for (int j = 0; j < numCols; j++) {
			sb[j] = b[_colIndexes[j]];
		}
		
		if( LOW_LEVEL_OPT && numVals > 1 && _numRows > blksz )
		{
			//since single segment scans already exceed typical L2 cache sizes
			//and because there is some overhead associated with blocking, the
			//best configuration aligns with L3 cache size (x*vcores*64K*8B < L3)
			//x=4 leads to a good yet slightly conservative compromise for single-/
			//multi-threaded and typical number of cores and L3 cache sizes
			final int blksz2 = ColGroupBitmap.WRITE_CACHE_BLKSZ;
			
			//step 1: prepare position and value arrays
			
			//current pos / values per OLs
			int[] apos = new int[numVals];
			double[] aval = new double[numVals];
			
			//skip-scan to beginning for all OLs 
			if( rl > 0 ) { //rl aligned with blksz		
				int rskip = (getNumRows()/2/blksz)*blksz;
				
				for (int k = 0; k < numVals; k++) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);
					int start = (rl>=rskip)?rskip:0;
					int bufIx = (rl>=rskip)?_skiplist[k]:0;
					for( int i=start; i<rl && bufIx<bitmapLen; i+=blksz ) {
						bufIx += _data[bitmapOff+bufIx] + 1;
					}
					apos[k] = bufIx;
				}
			}
			
			//pre-aggregate values per OLs
			for( int k = 0; k < numVals; k++ )
				aval[k] = sumValues(k, sb);
					
			//step 2: cache conscious matrix-vector via horizontal scans 
			for( int bi=rl; bi<ru; bi+=blksz2 ) 
			{
				int bimax = Math.min(bi+blksz2, ru);
				
				//horizontal segment scan, incl pos maintenance
				for (int k = 0; k < numVals; k++) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);
					double val = aval[k];
					int bufIx = apos[k];
					
					for( int ii=bi; ii<bimax && bufIx<bitmapLen; ii+=blksz ) {
						//prepare length, start, and end pos
						int len = _data[bitmapOff+bufIx];
						int pos = bitmapOff+bufIx+1;
						
						//compute partial results
						//LinearAlgebraUtils.vectAdd(val, c, bitmaps, pos, ii, len);
						for( int i=pos; i<pos+len; i++ )
							c[ii + _data[i]] += val;	
						bufIx += len + 1;
					}

					apos[k] = bufIx;
				}
			}		
		}
		else
		{	
			//iterate over all values and their bitmaps
			for (int k = 0; k < numVals; k++) 
			{
				//prepare value-to-add for entire value bitmap
				int bitmapOff = _ptr[k];
				int bitmapLen = len(k);
				double val = sumValues(k, sb);
				
				//iterate over bitmap blocks and add values
				if (val != 0) {
					int offsetBase = 0;
					int bufIx = 0;
					int curBlckLen;
					
					//scan to beginning offset if necessary 
					if( rl > 0 ){
						for (; bufIx<bitmapLen & offsetBase<rl; bufIx += curBlckLen + 1, offsetBase += blksz) {
							curBlckLen = _data[bitmapOff+bufIx];
						}	
					}
					
					//compute partial results
					for (; bufIx<bitmapLen & offsetBase<ru; bufIx += curBlckLen + 1, offsetBase += blksz) {
						curBlckLen = _data[bitmapOff+bufIx];
						for (int blckIx = 1; blckIx <= curBlckLen; blckIx++) {
							c[offsetBase + _data[bitmapOff+bufIx + blckIx]] += val;
						}
					}
				}
			}
		}
	}

	@Override
	public void leftMultByRowVector(MatrixBlock vector, MatrixBlock result)
		throws DMLRuntimeException 
	{
		double[] a = ConverterUtils.getDenseVector(vector);
		double[] c = result.getDenseBlock();
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int numCols = getNumCols();
		final int numVals = getNumValues();
		final int n = getNumRows();
		
		if( LOW_LEVEL_OPT && numVals > 1 && _numRows > blksz )
		{
			//cache blocking config (see matrix-vector mult for explanation)
			final int blksz2 = ColGroupBitmap.READ_CACHE_BLKSZ;
			
			//step 1: prepare position and value arrays
			
			//current pos per OLs / output values
			int[] apos = new int[numVals];
			double[] cvals = new double[numVals];
			
			//step 2: cache conscious matrix-vector via horizontal scans 
			for( int ai=0; ai<n; ai+=blksz2 ) 
			{
				int aimax = Math.min(ai+blksz2, n);
				
				//horizontal segment scan, incl pos maintenance
				for (int k = 0; k < numVals; k++) {
					int bitmapOff = _ptr[k];
					int bitmapLen = len(k);
					int bufIx = apos[k];
					double vsum = 0;	
					
					for( int ii=ai; ii<aimax && bufIx<bitmapLen; ii+=blksz ) {
						//prepare length, start, and end pos
						int len = _data[bitmapOff+bufIx];
						int pos = bitmapOff+bufIx+1;
						
						//iterate over bitmap blocks and compute partial results (a[i]*1)
						vsum += LinearAlgebraUtils.vectSum(a, _data, ii, pos, len);
						bufIx += len + 1;
					}

					apos[k] = bufIx;
					cvals[k] += vsum;
				}
			}
			
			//step 3: scale partial results by values and write to global output
			for (int k = 0, valOff=0; k < numVals; k++, valOff+=numCols)
				for( int j = 0; j < numCols; j++ )
					c[ _colIndexes[j] ] += cvals[k] * _values[valOff+j];		
		}
		else
		{
			//iterate over all values and their bitmaps
			for (int k=0, valOff=0; k<numVals; k++, valOff+=numCols) 
			{
				int bitmapOff = _ptr[k];
				int bitmapLen = len(k);
				
				//iterate over bitmap blocks and add partial results
				double vsum = 0;
				for (int bix=0, off=0; bix < bitmapLen; bix+=_data[bitmapOff+bix]+1, off+=blksz)
					vsum += LinearAlgebraUtils.vectSum(a, _data, off, bitmapOff+bix+1, _data[bitmapOff+bix]);
				
				//scale partial results by values and write results
				for( int j = 0; j < numCols; j++ )
					c[ _colIndexes[j] ] += vsum * _values[ valOff+j ];
			}
		}
	}

	@Override
	public void unaryAggregateOperations(AggregateUnaryOperator op, MatrixBlock result) 
		throws DMLRuntimeException 
	{
		KahanFunction kplus = (op.aggOp.increOp.fn instanceof KahanPlus) ?
				KahanPlus.getKahanPlusFnObject() : KahanPlusSq.getKahanPlusSqFnObject();
		
		if( op.indexFn instanceof ReduceAll )
			computeSum(result, kplus);
		else if( op.indexFn instanceof ReduceCol )
			computeRowSums(result, kplus);
		else if( op.indexFn instanceof ReduceRow )
			computeColSums(result, kplus);
	}
	
	private void computeSum(MatrixBlock result, KahanFunction kplus)
	{
		KahanObject kbuff = new KahanObject(result.quickGetValue(0, 0), result.quickGetValue(0, 1));
		
		//iterate over all values and their bitmaps
		final int numVals = getNumValues();
		final int numCols = getNumCols();
		
		for (int bitmapIx = 0; bitmapIx < numVals; bitmapIx++) 
		{
			int bitmapOff = _ptr[bitmapIx];
			int bitmapLen = len(bitmapIx);
			int valOff = bitmapIx * numCols;
			
			//iterate over bitmap blocks and count partial lengths
			int count = 0;
			for (int bix=0; bix < bitmapLen; bix+=_data[bitmapOff+bix]+1)
				count += _data[bitmapOff+bix];
			
			//scale counts by all values
			for( int j = 0; j < numCols; j++ )
				kplus.execute3(kbuff, _values[ valOff+j ], count);
		}
		
		result.quickSetValue(0, 0, kbuff._sum);
		result.quickSetValue(0, 1, kbuff._correction);
	}
	
	private void computeRowSums(MatrixBlock result, KahanFunction kplus)
	{
		KahanObject kbuff = new KahanObject(0, 0);
	
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int numVals = getNumValues();
		
		//iterate over all values and their bitmaps
		for (int bitmapIx = 0; bitmapIx < numVals; bitmapIx++) 
		{
			//prepare value-to-add for entire value bitmap
			int bitmapOff = _ptr[bitmapIx];
			int bitmapLen = len(bitmapIx);
			double val = sumValues(bitmapIx);
			
			//iterate over bitmap blocks and add values
			if (val != 0) {
				int offsetBase = 0;
				int bufIx = 0;
				int curBlckLen;
				for (; bufIx < bitmapLen; bufIx += curBlckLen + 1, offsetBase += blksz) {
					curBlckLen = _data[bitmapOff+bufIx];
					for (int blckIx = 1; blckIx <= curBlckLen; blckIx++) {
						int rix = offsetBase + _data[bitmapOff+bufIx + blckIx];
						kbuff.set(result.quickGetValue(rix, 0), result.quickGetValue(rix, 1));
						kplus.execute2(kbuff, val);
						result.quickSetValue(rix, 0, kbuff._sum);
						result.quickSetValue(rix, 1, kbuff._correction);
					}
				}
			}
		}
	}
	
	private void computeColSums(MatrixBlock result, KahanFunction kplus)
	{
		KahanObject kbuff = new KahanObject(0, 0);
		
		//iterate over all values and their bitmaps
		final int numVals = getNumValues();
		final int numCols = getNumCols();
		for (int bitmapIx = 0; bitmapIx < numVals; bitmapIx++) 
		{
			int bitmapOff = _ptr[bitmapIx];
			int bitmapLen = len(bitmapIx);
			int valOff = bitmapIx * numCols;
			
			//iterate over bitmap blocks and count partial lengths
			int count = 0;
			for (int bix=0; bix < bitmapLen; bix+=_data[bitmapOff+bix]+1)
				count += _data[bitmapOff+bix];
			
			//scale counts by all values
			for( int j = 0; j < numCols; j++ ) {
				kbuff.set(result.quickGetValue(0, _colIndexes[j]),result.quickGetValue(1, _colIndexes[j]));
				kplus.execute3(kbuff, _values[ valOff+j ], count);
				result.quickSetValue(0, _colIndexes[j], kbuff._sum);
				result.quickSetValue(1, _colIndexes[j], kbuff._correction);
			}
		}
	}
	
	/**
	 * Utility function of sparse-unsafe operations.
	 * 
	 * @return zero indicator vector
	 * @throws DMLRuntimeException if DMLRuntimeException occurs
	 */
	private boolean[] computeZeroIndicatorVector()
		throws DMLRuntimeException 
	{
		boolean[] ret = new boolean[_numRows];
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int numVals = getNumValues();
		
		//initialize everything with zero
		Arrays.fill(ret, true);
		
		//iterate over all values and their bitmaps
		for (int bitmapIx = 0; bitmapIx < numVals; bitmapIx++) 
		{
			//prepare value-to-add for entire value bitmap
			int bitmapOff = _ptr[bitmapIx];
			int bitmapLen = len(bitmapIx);
			
			//iterate over bitmap blocks and add values
			int offsetBase = 0;
			int bufIx = 0;
			int curBlckLen;
			for (; bufIx < bitmapLen; bufIx += curBlckLen + 1, offsetBase += blksz) {
				curBlckLen = _data[bitmapOff+bufIx];
				for (int blckIx = 1; blckIx <= curBlckLen; blckIx++) {
					ret[offsetBase + _data[bitmapOff+bufIx + blckIx]] &= false;
				}
			}
		}
		
		return ret;
	}
	
	@Override
	protected void countNonZerosPerRow(int[] rnnz)
	{
		final int blksz = BitmapEncoder.BITMAP_BLOCK_SZ;
		final int numVals = getNumValues();
		final int numCols = getNumCols();
		
		//iterate over all values and their bitmaps
		for (int k = 0; k < numVals; k++) 
		{
			//prepare value-to-add for entire value bitmap
			int bitmapOff = _ptr[k];
			int bitmapLen = len(k);
			
			//iterate over bitmap blocks and add values
			int offsetBase = 0;
			int curBlckLen;
			for (int bufIx=0; bufIx<bitmapLen; bufIx+=curBlckLen+1, offsetBase+=blksz) {
				curBlckLen = _data[bitmapOff+bufIx];
				for (int blckIx = 1; blckIx <= curBlckLen; blckIx++) {
					rnnz[offsetBase + _data[bitmapOff+bufIx + blckIx]] += numCols;
				}
			}
		}
	}
}
