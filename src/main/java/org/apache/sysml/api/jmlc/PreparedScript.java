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

package org.apache.sysml.api.jmlc;

import java.util.HashSet;

import org.apache.sysml.api.DMLException;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.instructions.cp.BooleanObject;
import org.apache.sysml.runtime.instructions.cp.DoubleObject;
import org.apache.sysml.runtime.instructions.cp.IntObject;
import org.apache.sysml.runtime.instructions.cp.ScalarObject;
import org.apache.sysml.runtime.instructions.cp.StringObject;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.DataConverter;

/**
 * JMLC (Java Machine Learning Connector) API:
 * 
 * NOTE: Currently fused API and implementation in order to reduce complexity. 
 */
public class PreparedScript 
{
	
	//input/output specification
	private HashSet<String> _inVarnames = null;
	private HashSet<String> _outVarnames = null;
	
	//internal state (reused)
	private Program _prog = null;
	private LocalVariableMap _vars = null; 
	
	/**
	 * Meant to be invoked only from Connection
	 */
	protected PreparedScript( Program prog, String[] inputs, String[] outputs )
	{
		_prog = prog;
		_vars = new LocalVariableMap();
		
		//populate input/output vars
		_inVarnames = new HashSet<String>();
		for( String var : inputs )
			_inVarnames.add( var );
		_outVarnames = new HashSet<String>();
		for( String var : outputs )
			_outVarnames.add( var );
	}
	
	/**
	 * 
	 * @param varname
	 * @param scalar
	 * @throws DMLException 
	 */
	public void setScalar(String varname, ScalarObject scalar) 
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		_vars.put(varname, scalar);
	}
	
	/**
	 * 
	 * @param varname
	 * @param scalar
	 * @throws DMLException 
	 */
	public void setScalar(String varname, boolean scalar) 
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		BooleanObject bo = new BooleanObject(varname, scalar);
		_vars.put(varname, bo);
	}
	
	/**
	 * 
	 * @param varname
	 * @param scalar
	 * @throws DMLException 
	 */
	public void setScalar(String varname, long scalar) 
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		IntObject io = new IntObject(varname, scalar);
		_vars.put(varname, io);
	}
	
	/**
	 * 
	 * @param varname
	 * @param scalar
	 * @throws DMLException 
	 */
	public void setScalar(String varname, double scalar) 
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		DoubleObject doo = new DoubleObject(varname, scalar);
		_vars.put(varname, doo);	
	}
	
	/**
	 * 
	 * @param varname
	 * @param scalar
	 * @throws DMLException 
	 */
	public void setScalar(String varname, String scalar) 
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		StringObject so = new StringObject(varname, scalar);
		_vars.put(varname, so);
	}
	
	/**
	 * 
	 * @param varname
	 * @param matrix
	 * @throws DMLException
	 */
	public void setMatrix(String varname, MatrixBlock matrix)
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		
		DMLConfig conf = ConfigurationManager.getConfig();
		String scratch_space = conf.getTextValue(DMLConfig.SCRATCH_SPACE);
		int blocksize = conf.getIntValue(DMLConfig.DEFAULT_BLOCK_SIZE);
		
		//create new matrix object
		MatrixCharacteristics mc = new MatrixCharacteristics(matrix.getNumRows(), matrix.getNumColumns(), blocksize, blocksize);
		MatrixFormatMetaData meta = new MatrixFormatMetaData(mc, OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo);
		MatrixObject mo = new MatrixObject(ValueType.DOUBLE, scratch_space+"/"+varname, meta);
		mo.acquireModify(matrix); 
		mo.release();
		
		//put create matrix wrapper into symbol table
		_vars.put(varname, mo);
	}
	
	/**
	 * 
	 * @param varname
	 * @param matrix
	 * @throws DMLException
	 */
	public void setMatrix(String varname, double[][] matrix)
		throws DMLException
	{
		if( !_inVarnames.contains(varname) )
			throw new DMLException("Unspecified input variable: "+varname);
		
		MatrixBlock mb = DataConverter.convertToMatrixBlock(matrix);
		setMatrix(varname, mb);
	}
	
	
	/**
	 * 
	 */
	public void clearParameters()
	{
		_vars.removeAll();
	}
	
	/**
	 * 
	 * @return
	 * @throws DMLException 
	 */
	public ResultVariables executeScript() 
		throws DMLException
	{
		//create and populate execution context
		ExecutionContext ec = ExecutionContextFactory.createContext(_prog);	
		ec.setVariables(_vars);
		
		//core execute runtime program	
		_prog.execute( ec );  
		
		//construct results
		ResultVariables rvars = new ResultVariables();
		for( String ovar : _outVarnames )
			if( _vars.keySet().contains(ovar) )
				rvars.addResult(ovar, _vars.get(ovar));
			
		return rvars;
	}
}
