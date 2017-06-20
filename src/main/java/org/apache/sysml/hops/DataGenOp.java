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

package org.apache.sysml.hops;


import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop.MultiThreadedHop;
import org.apache.sysml.hops.rewrite.HopRewriteUtils;
import org.apache.sysml.lops.DataGen;
import org.apache.sysml.lops.Lop;
import org.apache.sysml.lops.LopProperties.ExecType;
import org.apache.sysml.lops.LopsException;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.controlprogram.parfor.ProgramConverter;
import org.apache.sysml.utils.GlobalState;

/**
 * A DataGenOp can be rand (or matrix constructor), sequence, and sample -
 * these operators have different parameters and use a map of parameter type to hop position.
 */
public class DataGenOp extends Hop implements MultiThreadedHop
{
	
	public static final long UNSPECIFIED_SEED = -1;
	
	 // defines the specific data generation method
	private DataGenMethod _op;
	private int _maxNumThreads = -1; //-1 for unlimited
	
	/**
	 * List of "named" input parameters. They are maintained as a hashmap:
	 * parameter names (String) are mapped as indices (Integer) into getInput()
	 * arraylist.
	 * 
	 * i.e., getInput().get(_paramIndexMap.get(parameterName)) refers to the Hop
	 * that is associated with parameterName.
	 */
	private HashMap<String, Integer> _paramIndexMap = new HashMap<String, Integer>();
		

	/** target identifier which will hold the random object */
	private DataIdentifier _id;
	
	//Rand-specific attributes
	
	/** sparsity of the random object, this is used for mem estimate */
	private double _sparsity = -1;
	/** base directory for temp file (e.g., input seeds)*/
	private String _baseDir;
	
	//seq-specific attributes (used for recompile/recompile)
	private double _incr = Double.MAX_VALUE; 
	
	
	private DataGenOp() {
		//default constructor for clone
	}
	
	/**
	 * <p>Creates a new Rand HOP.</p>
	 * 
	 * @param mthd data gen method
	 * @param id the target identifier
	 * @param inputParameters HashMap of the input parameters for Rand Hop
	 */
	public DataGenOp(DataGenMethod mthd, DataIdentifier id, HashMap<String, Hop> inputParameters)
	{
		super(id.getName(), DataType.MATRIX, ValueType.DOUBLE);
		
		_id = id;
		_op = mthd;

		int index = 0;
		for( Entry<String, Hop> e: inputParameters.entrySet() ) {
			String s = e.getKey();
			Hop input = e.getValue();
			getInput().add(input);
			input.getParent().add(this);

			_paramIndexMap.put(s, index);
			index++;
		}
		
		Hop sparsityOp = inputParameters.get(DataExpression.RAND_SPARSITY);
		if ( mthd == DataGenMethod.RAND && sparsityOp instanceof LiteralOp)
			_sparsity = Double.valueOf(((LiteralOp)sparsityOp).getName());
		
		//generate base dir
		String scratch = ConfigurationManager.getScratchSpace();
		_baseDir = scratch + Lop.FILE_SEPARATOR + Lop.PROCESS_PREFIX + GlobalState.uuid + Lop.FILE_SEPARATOR + 
	               Lop.FILE_SEPARATOR + ProgramConverter.CP_ROOT_THREAD_ID + Lop.FILE_SEPARATOR;
		
		//compute unknown dims and nnz
		refreshSizeInformation();
	}

	@Override
	public void checkArity() throws HopsException {
		int sz = _input.size();
		int pz = _paramIndexMap.size();
		HopsException.check(sz == pz, this, "has %d inputs but %d parameters", sz, pz);
	}

	@Override
	public String getOpString() {
		return "dg(" + _op.toString().toLowerCase() +")";
	}
	
	public DataGenMethod getOp() {
		return _op;
	}
	
	@Override
	public void setMaxNumThreads( int k ) {
		_maxNumThreads = k;
	}
	
	@Override
	public int getMaxNumThreads() {
		return _maxNumThreads;
	}
	
	@Override
	public Lop constructLops() 
		throws HopsException, LopsException
	{
		//return already created lops
		if( getLops() != null )
			return getLops();

		ExecType et = optFindExecType();
		
		HashMap<String, Lop> inputLops = new HashMap<String, Lop>();
		for (Entry<String, Integer> cur : _paramIndexMap.entrySet()) {
			if( cur.getKey().equals(DataExpression.RAND_ROWS) && _dim1>0 )
				inputLops.put(cur.getKey(), new LiteralOp(_dim1).constructLops());
			else if( cur.getKey().equals(DataExpression.RAND_COLS) && _dim2>0 )
				inputLops.put(cur.getKey(), new LiteralOp(_dim2).constructLops());
			else
				inputLops.put(cur.getKey(), getInput().get(cur.getValue()).constructLops());
		}
		
		DataGen rnd = new DataGen(_op, _id, inputLops,_baseDir,
								getDataType(), getValueType(), et);
		
		int k = OptimizerUtils.getConstrainedNumThreads(_maxNumThreads);
		rnd.setNumThreads(k);
		
		rnd.getOutputParameters().setDimensions(
				getDim1(), getDim2(),
				//robust handling for blocksize (important for -exec singlenode; otherwise incorrect results)
				(getRowsInBlock()>0)?getRowsInBlock():ConfigurationManager.getBlocksize(), 
				(getColsInBlock()>0)?getColsInBlock():ConfigurationManager.getBlocksize(),  
				//actual rand nnz might differ (in cp/mr they are corrected after execution)
				(_op==DataGenMethod.RAND && et==ExecType.SPARK && getNnz()!=0) ? -1 : getNnz(),
				getUpdateType());
		
		setLineNumbers(rnd);
		setLops(rnd);
		
		//add reblock/checkpoint lops if necessary
		constructAndSetLopsDataFlowProperties();
		
		return getLops();
	}

	@Override
	public boolean allowsAllExecTypes()
	{
		return true;
	}	
	
	@Override
	protected double computeOutputMemEstimate( long dim1, long dim2, long nnz )
	{		
		double ret = 0;
		
		if ( _op == DataGenMethod.RAND && _sparsity != -1 ) {
			if( hasConstantValue(0.0) ) { //if empty block
				ret = OptimizerUtils.estimateSizeEmptyBlock(dim1, dim2);
			}
			else {
				//sparsity-aware estimation (dependent on sparse generation approach); for pure dense generation
				//we would need to disable sparsity-awareness and estimate via sparsity=1.0
				ret = OptimizerUtils.estimateSizeExactSparsity(dim1, dim2, _sparsity);
			}
		}
		else {
			ret = OptimizerUtils.estimateSizeExactSparsity(dim1, dim2, 1.0);	
		}
		
		return ret;
	}
	
	@Override
	protected double computeIntermediateMemEstimate( long dim1, long dim2, long nnz )
	{
		if ( _op == DataGenMethod.RAND && dimsKnown() ) {
			long numBlocks = (long) (Math.ceil((double)dim1/ConfigurationManager.getBlocksize()) 
					* Math.ceil((double)dim2/ConfigurationManager.getBlocksize()));
			return 32 + numBlocks*8.0; // 32 bytes of overhead for an array of long & numBlocks long values.
		}
		else 
			return 0;
	}
	
	@Override
	protected long[] inferOutputCharacteristics( MemoTable memo )
	{
		//infer rows and 
		if( (_op == DataGenMethod.RAND || _op == DataGenMethod.SINIT ) &&
			OptimizerUtils.ALLOW_WORSTCASE_SIZE_EXPRESSION_EVALUATION )
		{
			long dim1 = computeDimParameterInformation(getInput().get(_paramIndexMap.get(DataExpression.RAND_ROWS)), memo);
			long dim2 = computeDimParameterInformation(getInput().get(_paramIndexMap.get(DataExpression.RAND_COLS)), memo);
			long nnz = _sparsity >= 0 ? (long)(_sparsity * dim1 * dim2) : -1;
			if( dim1>0 && dim2>0 )
				return new long[]{ dim1, dim2, nnz };
		}
		else if ( _op == DataGenMethod.SEQ )
		{
			Hop from = getInput().get(_paramIndexMap.get(Statement.SEQ_FROM));
			Hop to = getInput().get(_paramIndexMap.get(Statement.SEQ_TO));
			Hop incr = getInput().get(_paramIndexMap.get(Statement.SEQ_INCR)); 
			//in order to ensure correctness we also need to know from and incr
			//here, we check for the common case of seq(1,x), i.e. from=1, incr=1
			if(    from instanceof LiteralOp && HopRewriteUtils.getDoubleValueSafe((LiteralOp)from)==1
				&& incr instanceof LiteralOp && HopRewriteUtils.getDoubleValueSafe((LiteralOp)incr)==1 )
			{
				long toVal = computeDimParameterInformation(to, memo);
				if( toVal > 0 )	
					return new long[]{ toVal, 1, -1 };
			}
			//here, we check for the common case of seq(x,1,-1), i.e. from=x, to=1 incr=-1
			if(    to instanceof LiteralOp && HopRewriteUtils.getDoubleValueSafe((LiteralOp)to)==1
				&& incr instanceof LiteralOp && HopRewriteUtils.getDoubleValueSafe((LiteralOp)incr)==-1 )
			{
				long fromVal = computeDimParameterInformation(from, memo);
				if( fromVal > 0 )	
					return new long[]{ fromVal, 1, -1 };
			}
		}
		
		return null;
	}

	@Override
	protected ExecType optFindExecType() throws HopsException {
		
		checkAndSetForcedPlatform();

		ExecType REMOTE = OptimizerUtils.isSparkExecutionMode() ? ExecType.SPARK : ExecType.MR;
		
		if( _etypeForced != null ) 			
			_etype = _etypeForced;
		else 
		{
			if ( OptimizerUtils.isMemoryBasedOptLevel() ) {
				_etype = findExecTypeByMemEstimate();
			}
			else if (this.areDimsBelowThreshold() || this.isVector())
				_etype = ExecType.CP;
			else
				_etype = REMOTE;
		
			//check for valid CP dimensions and matrix size
			checkAndSetInvalidCPDimsAndSize();
		}

		//mark for recompile (forever)
		if( ConfigurationManager.isDynamicRecompilation() && !dimsKnown(true) && _etype==REMOTE )
			setRequiresRecompile();

		//always force string initialization into CP (not supported in MR)
		//similarly, sample is currently not supported in MR either
		if( _op == DataGenMethod.SINIT )
			_etype = ExecType.CP;
		
		//workaround until sample supported in MR
		if( _op == DataGenMethod.SAMPLE && _etype == ExecType.MR )
			_etype = ExecType.CP;
		
		return _etype;
	}
	
	@Override
	public void refreshSizeInformation()
	{		
		Hop input1 = null;  
		Hop input2 = null; 
		Hop input3 = null;

		if ( _op == DataGenMethod.RAND || _op == DataGenMethod.SINIT ) 
		{
			input1 = getInput().get(_paramIndexMap.get(DataExpression.RAND_ROWS)); //rows
			input2 = getInput().get(_paramIndexMap.get(DataExpression.RAND_COLS)); //cols
			
			//refresh rows information
			refreshRowsParameterInformation(input1);
			
			//refresh cols information
			refreshColsParameterInformation(input2);
		}
		else if (_op == DataGenMethod.SEQ ) 
		{
			input1 = getInput().get(_paramIndexMap.get(Statement.SEQ_FROM));
			input2 = getInput().get(_paramIndexMap.get(Statement.SEQ_TO)); 
			input3 = getInput().get(_paramIndexMap.get(Statement.SEQ_INCR)); 

			double from = computeBoundsInformation(input1);
			boolean fromKnown = (from != Double.MAX_VALUE);
			
			double to = computeBoundsInformation(input2);
			boolean toKnown = (to != Double.MAX_VALUE);
			
			double incr = computeBoundsInformation(input3);
			boolean incrKnown = (incr != Double.MAX_VALUE);
			if(  fromKnown && toKnown && incr == 1) {
				incr = ( from >= to ) ? -1 : 1;
			}
			
			if ( fromKnown && toKnown && incrKnown ) {
				setDim1(1 + (long)Math.floor(((double)(to-from))/incr));
				setDim2(1);
				_incr = incr;
			}
		}
		
		//refresh nnz information (for seq, sparsity is always -1)
		if( _op == DataGenMethod.RAND && hasConstantValue(0.0) )
			_nnz = 0;
		else if ( dimsKnown() && _sparsity>=0 ) //general case
			_nnz = (long) (_sparsity * _dim1 * _dim2);
		else
			_nnz = -1;
	}
	

	public HashMap<String, Integer> getParamIndexMap()
	{
		return _paramIndexMap;
	}
	
	public int getParamIndex(String key)
	{
		return _paramIndexMap.get(key);
	}

	public boolean hasConstantValue() 
	{
		//robustness for other operations, not specifying min/max/sparsity
		if( _op != DataGenMethod.RAND )
			return false;
		
		Hop min = getInput().get(_paramIndexMap.get(DataExpression.RAND_MIN)); //min 
		Hop max = getInput().get(_paramIndexMap.get(DataExpression.RAND_MAX)); //max
		Hop sparsity = getInput().get(_paramIndexMap.get(DataExpression.RAND_SPARSITY)); //sparsity
		
		//literal value comparison
		if( min instanceof LiteralOp && max instanceof LiteralOp && sparsity instanceof LiteralOp) {
			try {
				double minVal = HopRewriteUtils.getDoubleValue((LiteralOp)min);
				double maxVal = HopRewriteUtils.getDoubleValue((LiteralOp)max);
				double sp = HopRewriteUtils.getDoubleValue((LiteralOp)sparsity);
				return (sp==1.0 && minVal == maxVal);
			}
			catch(Exception ex) {
				return false;
			}
		}
		//reference comparison (based on common subexpression elimination)
		else if ( min == max && sparsity instanceof LiteralOp ) {
			return (HopRewriteUtils.getDoubleValueSafe((LiteralOp)sparsity)==1);
		}
		
		return false;
	}

	public boolean hasConstantValue(double val) 
	{
		//string initialization does not exhibit constant values
		if( _op != DataGenMethod.RAND )
			return false;
		
		boolean ret = false;
		
		Hop min = getInput().get(_paramIndexMap.get(DataExpression.RAND_MIN)); //min 
		Hop max = getInput().get(_paramIndexMap.get(DataExpression.RAND_MAX)); //max
		
		//literal value comparison
		if( min instanceof LiteralOp && max instanceof LiteralOp){
			double minVal = HopRewriteUtils.getDoubleValueSafe((LiteralOp)min);
			double maxVal = HopRewriteUtils.getDoubleValueSafe((LiteralOp)max);
			ret = (minVal == val && maxVal == val);
		}
		
		//sparsity awareness if requires
		if( ret && val != 0 ) {
			Hop sparsity = getInput().get(_paramIndexMap.get(DataExpression.RAND_SPARSITY)); //sparsity
			ret &= (sparsity == null || sparsity instanceof LiteralOp 
				&& HopRewriteUtils.getDoubleValueSafe((LiteralOp)sparsity) == 1);
		}
		
		return ret;
	}
	
	public void setIncrementValue(double incr)
	{
		_incr = incr;
	}
	
	public double getIncrementValue()
	{
		return _incr;
	}
	
	public static long generateRandomSeed()
	{
		return System.nanoTime();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object clone() throws CloneNotSupportedException 
	{
		DataGenOp ret = new DataGenOp();	
		
		//copy generic attributes
		ret.clone(this, false);
		
		//copy specific attributes
		ret._op = _op;
		ret._id = _id;
		ret._sparsity = _sparsity;
		ret._baseDir = _baseDir;
		ret._paramIndexMap = (HashMap<String, Integer>) _paramIndexMap.clone();
		ret._maxNumThreads = _maxNumThreads;
		//note: no deep cp of params since read-only 
		
		return ret;
	}
	
	@Override
	public boolean compare( Hop that )
	{
		if( !(that instanceof DataGenOp) )
			return false;
		
		DataGenOp that2 = (DataGenOp)that;	
		boolean ret = (  _op == that2._op
				      && _sparsity == that2._sparsity
				      && _baseDir.equals(that2._baseDir)
					  && _paramIndexMap!=null && that2._paramIndexMap!=null
					  && _maxNumThreads == that2._maxNumThreads );
		
		if( ret )
		{
			for( Entry<String,Integer> e : _paramIndexMap.entrySet() )
			{
				String key1 = e.getKey();
				int pos1 = e.getValue();
				int pos2 = that2._paramIndexMap.get(key1);
				ret &= (   that2.getInput().get(pos2)!=null
					    && getInput().get(pos1) == that2.getInput().get(pos2) );
			}
			
			//special case for rand seed (no CSE if unspecified seed because runtime generated)
			//note: if min and max is constant, we can safely merge those hops
			if( _op == DataGenMethod.RAND || _op == DataGenMethod.SINIT ){
				Hop seed = getInput().get(_paramIndexMap.get(DataExpression.RAND_SEED));
				Hop min = getInput().get(_paramIndexMap.get(DataExpression.RAND_MIN));
				Hop max = getInput().get(_paramIndexMap.get(DataExpression.RAND_MAX));
				if( seed.getName().equals(String.valueOf(DataGenOp.UNSPECIFIED_SEED)) && min != max )
					ret = false;
			}
		}
		
		return ret;
	}
}
