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


package org.apache.sysml.parser;

import java.util.ArrayList;

public class DMLParseException extends ParseException
{
	
	
	/**
	 * The version identifier for this Serializable class.
	 * Increment only if the <i>serialized</i> form of the
	 * class changes.
	 */
	private static final long serialVersionUID = 1L;
	
	private ArrayList<DMLParseException> _exceptionList;
	
	public ArrayList<DMLParseException> getExceptionList(){
		return _exceptionList;
	}
	
	public DMLParseException(String fname){
		super();
		_exceptionList = new ArrayList<DMLParseException>();
	}
	
	public DMLParseException(String fname, String msg){
		super(msg);
		_exceptionList = new ArrayList<DMLParseException>();
		_exceptionList.add(this);
	}
	
	public int size(){
		return _exceptionList.size();
	}
	
	public void add(DMLParseException e){
		_exceptionList.addAll(e.getExceptionList());
	}
}