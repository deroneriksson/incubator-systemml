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


package org.apache.sysml.runtime.matrix.operators;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.functionobjects.GreaterThan;
import org.apache.sysml.runtime.functionobjects.GreaterThanEquals;
import org.apache.sysml.runtime.functionobjects.LessThan;
import org.apache.sysml.runtime.functionobjects.LessThanEquals;
import org.apache.sysml.runtime.functionobjects.Power;
import org.apache.sysml.runtime.functionobjects.ValueFunction;


public class LeftScalarOperator extends ScalarOperator 
{
	
	private static final long serialVersionUID = 2360577666575746424L;
	
	public LeftScalarOperator(ValueFunction p, double cst) {
		super(p, cst);
		
		//disable sparse-safe for c^M because 1^0=1
		if( fn instanceof Power )
			sparseSafe = false;
	}

	@Override
	public double executeScalar(double in) throws DMLRuntimeException {
		return fn.execute(_constant, in);
	}
	
	@Override
	public void setConstant(double cst) 
	{
		super.setConstant(cst);
		
		//disable sparse-safe for c^M because 1^0=1
		if( fn instanceof Power )
			sparseSafe = false;
		
		//enable conditionally sparse safe operations
		if(    (fn instanceof GreaterThan && _constant<=0)
			|| (fn instanceof GreaterThanEquals && _constant<0)
			|| (fn instanceof LessThan && _constant>=0)
			|| (fn instanceof LessThanEquals && _constant>0))
		{
			sparseSafe = true;
		}
	}
}
