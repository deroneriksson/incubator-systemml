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

package org.apache.sysml.api.mlcontext;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

public class ScriptRepo {

	public static final String DEFAULT_SCRIPT_PATH = "scripts/algorithms";
//	public static final String DEFAULT_SCRIPT_PATH = "scripts/datagen";
	
	static String path = DEFAULT_SCRIPT_PATH;
	
	static List<Script> scripts = new ArrayList<Script>();
	
//	public static List<Script> getResourceScripts() {
//		return null;
//	}
	
	public static void loadFromFS() {
		scripts.clear();
		File scriptDir = new File(path);
		if (scriptDir.exists()) {
			File[] files = scriptDir.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return (name.toLowerCase().endsWith(".dml") || name.toLowerCase().endsWith(".pydml"));
				}
			});
			for (File file : files) {
				// System.out.println("File name: " + file.getName());
				if (file.getName().toLowerCase().endsWith(".pydml")) {
					Script script = ScriptFactory.pydmlFromLocalFile(file);
					scripts.add(script);
				} else {
					Script script = ScriptFactory.dmlFromLocalFile(file);
					scripts.add(script);
				}
			}
		}
//		return scripts;
	}

	public static String list() {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<scripts.size(); i++) {
			Script script = scripts.get(i);
			if (script.getScriptMetadata() != null) {
				ScriptMetadata sm = script.getScriptMetadata();
				sb.append(" [" + i + "] ");
				sb.append(script.getName());
				sb.append(" (");
				sb.append(sm.name);
				sb.append(")");
				sb.append("\n     ");
				sb.append(sm.description);
				sb.append("\n");
			} else {
				sb.append(" [" + i + "] " + script.getName() + "\n");
			}
			
		}
		return sb.toString();
	}
	
	public static String getPath() {
		return path;
	}

	public static void setPath(String path) {
		ScriptRepo.path = path;
	}
	
	public static void resetPath() {
		path = DEFAULT_SCRIPT_PATH;
	}
	
	public static Script get(int index) {
		return scripts.get(index);
	}
	
	public static void main(String [] args) {
//		loadFromFS();
//		System.out.println(list());
//		Script script = ScriptFactory.dmlFromFile(DEFAULT_SCRIPT_PATH + "/ALS-CG.dml");
//		scripts.add(script);
//		String str = MLContextUtil.displayScriptMetadata(script);
//		System.out.println(str);
		
//		String property = System.getProperty("scala.color");
//		System.out.println("property" + property);
		Script script = ScriptFactory.dmlFromFile("scripts/utils/two-subsets.dml");
		String str = MLContextUtil.displayScriptMetadata(script);
		System.out.println(str);
	}
}
