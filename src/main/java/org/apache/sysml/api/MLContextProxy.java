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

package org.apache.sysml.api;

import java.util.ArrayList;

import org.apache.sysml.api.mlcontext.NewMLContext;
import org.apache.sysml.api.monitoring.Location;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.LanguageException;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.spark.SPInstruction;

/**
 * The purpose of this proxy is to shield systemml internals from direct access to MLContext
 * which would try to load spark libraries and hence fail if these are not available. This
 * indirection is much more efficient than catching NoClassDefFoundErrors for every access
 * to MLContext (e.g., on each recompile).
 * 
 */
public class MLContextProxy 
{
	
	private static boolean _active = false;
	
	/**
	 * 
	 * @param flag
	 */
	public static void setActive(boolean flag) {
		_active = flag;
	}
	
	/**
	 * 
	 * @return
	 */
	public static boolean isActive() {
		return _active;
	}

	/**
	 * 
	 * @param tmp
	 */
	public static ArrayList<Instruction> performCleanupAfterRecompilation(ArrayList<Instruction> tmp) 
	{
		if(NewMLContext.getActiveMLContext() != null) {
			return NewMLContext.getActiveMLContext().performCleanupAfterRecompilation(tmp);
		}
		return tmp;
	}

	/**
	 * 
	 * @param source
	 * @param targetname
	 * @throws LanguageException 
	 */
	public static void setAppropriateVarsForRead(Expression source, String targetname) 
		throws LanguageException 
	{
		NewMLContext mlContext = NewMLContext.getActiveMLContext();
		if (mlContext != null) {
			mlContext.setAppropriateVarsForRead(source, targetname);
		}
	}
	
	public static NewMLContext getActiveMLContext() {
		return NewMLContext.getActiveMLContext();
	}
	
	public static void setInstructionForMonitoring(Instruction inst) {
		Location loc = inst.getLocation();
		NewMLContext mlContext = NewMLContext.getActiveMLContext();
		if(loc != null && mlContext != null && mlContext.getMonitoringUtil() != null) {
			mlContext.getMonitoringUtil().setInstructionLocation(loc, inst);
		}
	}
	
	public static void addRDDForInstructionForMonitoring(SPInstruction inst, Integer rddID) {
		NewMLContext mlContext = NewMLContext.getActiveMLContext();
		if(mlContext != null && mlContext.getMonitoringUtil() != null) {
			mlContext.getMonitoringUtil().addRDDForInstruction(inst, rddID);
		}
	}
	
}
