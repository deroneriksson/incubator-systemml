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
package org.apache.sysml.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.mlcontext.MLResults;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptExecutor;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.FunctionStatement;
import org.apache.sysml.parser.FunctionStatementBlock;
import org.apache.sysml.parser.LanguageException;
import org.apache.sysml.parser.Statement;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.Modifier;
import javassist.NotFoundException;

public class GenerateResourceClassesForMLContext {

	public static final String SOURCE = "scripts";

	public static String source = SOURCE;
	public static String destination = "target/classes";

	public static final String BASE_DEST_PACKAGE = "org.apache.sysml.api.mlcontext.resources";
	public static final String CONVENIENCE_BASE_DEST_PACKAGE = "org.apache.sysml.api.mlcontext.convenience";

	public static boolean skipStaging = true;
	public static boolean skipPerfTest = true;
	public static boolean skipObsolete = true;

	public static void main(String[] args) throws Throwable {
		if (args.length == 2) {
			source = args[0];
			destination = args[1];
		} else if (args.length == 1) {
			source = args[0];
		}
		try {
			DMLScript.VALIDATOR_IGNORE_ISSUES = true;
			System.out.println("**********************************************");
			System.out.println("**** MLContext Javassist Class Generation ****");
			System.out.println("**********************************************");
			System.out.println("Source: " + source);
			System.out.println("Destination: " + destination);
			recurseDirectoriesForClassGeneration(source);
			String fullDirClassName = recurseDirectoriesForConvenienceClassGeneration(source);
			addConvenienceMethodToMLContext(source, fullDirClassName);
		} finally {
			DMLScript.VALIDATOR_IGNORE_ISSUES = false;
		}
	}

	public static void addConvenienceMethodToMLContext(String dirPath, String fullDirClassName) {
		try {
			File mlContextClassFile = new File(destination + "/org/apache/sysml/api/mlcontext/MLContext.class");
			InputStream is = new FileInputStream(mlContextClassFile);
			ClassPool pool = ClassPool.getDefault();
			CtClass ctMLContext = pool.makeClass(is);

			CtClass dirClass = pool.get(fullDirClassName);
			String methodName = methodNameForMLContext(fullDirClassName);
			System.out.println("Adding method " + methodName + "() to " + ctMLContext.getName());

			String methodBody = "{ " + fullDirClassName + " z = new " + fullDirClassName + "(); return z; }";
			// System.out.println("BODY" + methodBody);
			CtMethod ctMethod = CtNewMethod.make(Modifier.PUBLIC, dirClass, methodName, null, null, methodBody,
					ctMLContext);
			ctMLContext.addMethod(ctMethod);

			addPackageConvenienceMethodsToMLContext(dirPath, ctMLContext);

			ctMLContext.writeFile(destination);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		}
	}

	public static void addPackageConvenienceMethodsToMLContext(String dirPath, CtClass ctMLContext) {

		try {
			if (!SOURCE.equalsIgnoreCase(dirPath)) {
				return;
			}
			File dir = new File(dirPath);
			File[] subdirs = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File f) {
					return f.isDirectory();
				}
			});
			for (File subdir : subdirs) {
				String subDirPath = dirPath + File.separator + subdir.getName();
				if (skipDir(subdir)) {
					continue;
				}

				String fullSubDirClassName = dirPathToFullDirClassName(subDirPath);

				ClassPool pool = ClassPool.getDefault();
				CtClass subDirClass = pool.get(fullSubDirClassName);
				String subDirName = subdir.getName();
				subDirName = subDirName.replaceAll("-", "_");
				subDirName = subDirName.toLowerCase();

				System.out.println("Adding method " + subDirName + "() to " + ctMLContext.getName());

				String methodBody = "{ " + fullSubDirClassName + " z = new " + fullSubDirClassName + "(); return z; }";
				CtMethod ctMethod = CtNewMethod.make(Modifier.PUBLIC, subDirClass, subDirName, null, null, methodBody,
						ctMLContext);
				ctMLContext.addMethod(ctMethod);

			}
		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		}

	}

	public static String methodNameForMLContext(String fullDirClassName) {
		String m = fullDirClassName;
		m = m.substring(m.lastIndexOf(".") + 1);
		m = m.toLowerCase();
		return m;
	}

	public static String recurseDirectoriesForConvenienceClassGeneration(String dirPath) {
		try {
			// System.out.println("Directory:" + dirPath);
			File dir = new File(dirPath);

			String fullDirClassName = dirPathToFullDirClassName(dirPath);
			System.out.println("Generating Class: " + fullDirClassName);

			ClassPool pool = ClassPool.getDefault();
			CtClass ctDir = pool.makeClass(fullDirClassName);

			File[] subdirs = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File f) {
					return f.isDirectory();
				}
			});
			for (File subdir : subdirs) {
				String subDirPath = dirPath + File.separator + subdir.getName();
				if (skipDir(subdir)) {
					continue;
				}
				String fullSubDirClassName = recurseDirectoriesForConvenienceClassGeneration(subDirPath);

				CtClass subDirClass = pool.get(fullSubDirClassName);
				String subDirName = subdir.getName();

				subDirName = subDirName.replaceAll("-", "_");
				subDirName = subDirName.toLowerCase();

				System.out.println("Adding method " + subDirName + "() to " + fullDirClassName);

				String methodBody = "{ " + fullSubDirClassName + " z = new " + fullSubDirClassName + "(); return z; }";
				CtMethod ctMethod = CtNewMethod.make(Modifier.PUBLIC, subDirClass, subDirName, null, null, methodBody,
						ctDir);
				ctDir.addMethod(ctMethod);

			}

			File[] scriptFiles = dir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return (name.toLowerCase().endsWith(".dml") || name.toLowerCase().endsWith(".pydml"));
				}
			});
			for (File scriptFile : scriptFiles) {
				String scriptFilePath = scriptFile.getPath();
				String fullScriptClassName = BASE_DEST_PACKAGE + "."
						+ scriptFilePathToFullClassNameNoBase(scriptFilePath);
				CtClass scriptClass = pool.get(fullScriptClassName);
				String methodName = scriptFilePathToSimpleClassName(scriptFilePath);
				String methodBody = "{ " + fullScriptClassName + " z = new " + fullScriptClassName + "(); return z; }";
				CtMethod ctMethod = CtNewMethod.make(Modifier.PUBLIC, scriptClass, methodName, null, null, methodBody,
						ctDir);
				ctDir.addMethod(ctMethod);

			}

			ctDir.writeFile(destination);

			return fullDirClassName;
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String dirPathToFullDirClassName(String dirPath) {
		if (!dirPath.contains(File.separator)) {
			String c = dirPath;
			c = c.replace("-", "_");
			c = c.substring(0, 1).toUpperCase() + c.substring(1);
			c = CONVENIENCE_BASE_DEST_PACKAGE + "." + c;
			return c;
		}

		String p = dirPath;
		p = p.substring(0, p.lastIndexOf(File.separator));
		p = p.replace("-", "_");
		p = p.replace(File.separator, ".");
		p = p.toLowerCase();

		String c = dirPath;
		c = c.substring(c.lastIndexOf(File.separator) + 1);
		c = c.replace("-", "_");
		c = c.substring(0, 1).toUpperCase() + c.substring(1);

		return CONVENIENCE_BASE_DEST_PACKAGE + "." + p + "." + c;
	}

	public static boolean skipDir(File subdir) {
		if ("staging".equalsIgnoreCase(subdir.getName()) && skipStaging) {
			System.out.println("Skipping staging directory: " + subdir.getPath());
			return true;
		}
		if ("perftest".equalsIgnoreCase(subdir.getName()) && skipPerfTest) {
			System.out.println("Skipping perftest directory: " + subdir.getPath());
			return true;
		}
		if ("obsolete".equalsIgnoreCase(subdir.getName()) && skipObsolete) {
			System.out.println("Skipping obsolete directory: " + subdir.getPath());
			return true;
		}
		return false;
	}

	public static void recurseDirectoriesForClassGeneration(String dirPath) {
		// System.out.println("Directory: " + dirPath);
		File dir = new File(dirPath);

		iterateScriptFilesInDirectory(dir);

		File[] subdirs = dir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				return f.isDirectory();
			}
		});
		for (File subdir : subdirs) {
			String subdirpath = dirPath + File.separator + subdir.getName();
			if (skipDir(subdir)) {
				continue;
			}
			recurseDirectoriesForClassGeneration(subdirpath);
		}
	}

	public static void iterateScriptFilesInDirectory(File dir) {
		File[] scriptFiles = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return (name.toLowerCase().endsWith(".dml") || name.toLowerCase().endsWith(".pydml"));
			}
		});
		for (File scriptFile : scriptFiles) {
			String scriptFilePath = scriptFile.getPath();
			// System.out.println(" Script:" + scriptFilePath);
			// System.out.println(" Class:" +
			// scriptFilePathToFullClassNameNoBase(scriptFilePath));
			createScriptClass(scriptFilePath);
		}
	}

	public static String scriptFilePathToPackageNoBase(String scriptFilePath) {
		String p = scriptFilePath;
		p = p.substring(0, p.lastIndexOf(File.separator));
		p = p.replace("-", "_");
		p = p.replace(File.separator, ".");
		p = p.toLowerCase();
		return p;
	}

	public static String scriptFilePathToSimpleClassName(String scriptFilePath) {
		String c = scriptFilePath;
		c = c.substring(c.lastIndexOf(File.separator) + 1);
		c = c.replace("-", "_");
		c = c.substring(0, 1).toUpperCase() + c.substring(1);
		c = c.substring(0, c.indexOf("."));
		return c;
	}

	public static String scriptFilePathToFullClassNameNoBase(String scriptFilePath) {
		String p = scriptFilePathToPackageNoBase(scriptFilePath);
		String c = scriptFilePathToSimpleClassName(scriptFilePath);
		return p + "." + c;
	}

	public static void createScriptClass(String scriptFilePath) {
		try {
			String fullScriptClassName = BASE_DEST_PACKAGE + "." + scriptFilePathToFullClassNameNoBase(scriptFilePath);
			System.out.println("Generating Class: " + fullScriptClassName);
			ClassPool pool = ClassPool.getDefault();
			CtClass ctNewScript = pool.makeClass(fullScriptClassName);

			File scriptClassFile = new File(destination + "/org/apache/sysml/api/mlcontext/Script.class");
			InputStream is = new FileInputStream(scriptClassFile);
			CtClass ctScript = pool.makeClass(is);
			pool.makeClass(
					new FileInputStream(new File(destination + "/org/apache/sysml/api/mlcontext/ScriptType.class")));

			ctNewScript.setSuperclass(ctScript);

			CtConstructor ctCon = new CtConstructor(null, ctNewScript);
			ctCon.setBody(scriptConstructorBody(scriptFilePath));
			ctNewScript.addConstructor(ctCon);

			addFunctionMethods(scriptFilePath, ctNewScript);

			ctNewScript.writeFile(destination);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		}

	}

	public static DMLProgram dmlProgramFromScriptFilePath(String scriptFilePath) {
		String scriptString = fileToString(scriptFilePath);
		Script script = new Script(scriptString);
		ScriptExecutor se = new ScriptExecutor() {
			public MLResults execute(Script script) {
				setup(script);
				parseScript();
				return null;
			}
		};
		se.execute(script);
		DMLProgram dmlProgram = se.getDmlProgram();
		return dmlProgram;
	}

	public static void addFunctionMethods(String scriptFilePath, CtClass ctNewScript) {
		// if
		// (!"scripts/nn/examples/mnist_lenet-predict.dml".equalsIgnoreCase(scriptFilePath))
		// {
		// return;
		// }
		try {
			DMLProgram dmlProgram = dmlProgramFromScriptFilePath(scriptFilePath);
			if (dmlProgram == null) {
				System.out.println("Could not generate DML Program for: " + scriptFilePath);
				return;
			}
			Map<String, FunctionStatementBlock> defaultNsFsbsMap = dmlProgram
					.getFunctionStatementBlocks(DMLProgram.DEFAULT_NAMESPACE);
			List<FunctionStatementBlock> fsbs = new ArrayList<FunctionStatementBlock>();
			fsbs.addAll(defaultNsFsbsMap.values());
			for (FunctionStatementBlock fsb : fsbs) {
				ArrayList<Statement> sts = fsb.getStatements();
				for (Statement st : sts) {
					if (!(st instanceof FunctionStatement)) {
						continue;
					}
					FunctionStatement fs = (FunctionStatement) st;

					String dmlFunctionCall = generateDmlFunctionCall(scriptFilePath, fs);
					String functionCallMethod = generateFunctionCallMethod(scriptFilePath, fs, dmlFunctionCall);

					ClassPool pool = ClassPool.getDefault();
					pool.makeClass(new FileInputStream(
							new File(destination + "/org/apache/sysml/api/mlcontext/MLResults.class")));

					CtMethod m = CtNewMethod.make(functionCallMethod, ctNewScript);
					ctNewScript.addMethod(m);

					addFunctionDocsMethod(fs, scriptFilePath, ctNewScript, false);
					addFunctionDocsMethod(fs, scriptFilePath, ctNewScript, true);
				}
			}
		} catch (LanguageException e) {
			System.out.println("Could not add function methods for " + ctNewScript.getName());
		} catch (CannotCompileException e) {
			System.out.println("Could not add function methods for " + ctNewScript.getName());
		} catch (FileNotFoundException e) {
			System.out.println("Could not add function methods for " + ctNewScript.getName());
		} catch (IOException e) {
			System.out.println("Could not add function methods for " + ctNewScript.getName());
		} catch (RuntimeException e) {
			System.out.println("Could not add function methods for " + ctNewScript.getName());
		}
	}

	public static void addFunctionDocsMethod(FunctionStatement fs, String scriptFilePath, CtClass ctNewScript,
			boolean full) {

		try {

			if (fs.getInputParams().size() == 0) {
				return;
			}

			int bl = fs.getBeginLine();
			int el = fs.getEndLine();
			File f = new File(scriptFilePath);
			List<String> lines = FileUtils.readLines(f);

			int end = el;
			if (!full) {
				for (int i = bl - 1; i < el; i++) {
					String line = lines.get(i);
					if (line.contains("*/")) {
						end = i + 1;
						break;
					}
				}
			}
			List<String> sub = lines.subList(bl - 1, end);
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < sub.size(); i++) {
				// System.out.println("#" + i + ":" + sub.get(i));
				String line = sub.get(i);
				String escapeLine = StringEscapeUtils.escapeJava(line);
				sb.append(escapeLine);
				sb.append("\\n");
			}

			String docString = sb.toString();
			String docFunctionCallMethod = generateDocFunctionCallMethod(fs, docString, full);
			// System.out.println(docFunctionCallMethod);
			CtMethod m = CtNewMethod.make(docFunctionCallMethod, ctNewScript);
			ctNewScript.addMethod(m);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		}

	}

	public static String generateDocFunctionCallMethod(FunctionStatement fs, String docString, boolean full) {
		StringBuilder sb = new StringBuilder();
		sb.append("public String ");
		sb.append(fs.getName());
		if (full) {
			sb.append("_");
		}
		sb.append("() {\n");
		sb.append("String docString = \"" + docString + "\";\n");
		sb.append("return docString;\n");
		sb.append("}\n");
		return sb.toString();
	}

	public static String generateFunctionCallMethod(String scriptFilePath, FunctionStatement fs,
			String dmlFunctionCall) {
		StringBuilder sb = new StringBuilder();

		sb.append("public org.apache.sysml.api.mlcontext.MLResults ");
		sb.append(fs.getName());
		sb.append("(");

		ArrayList<DataIdentifier> inputParams = fs.getInputParams();
		for (int i = 0; i < inputParams.size(); i++) {
			if (i > 0) {
				sb.append(", ");
			}
			DataIdentifier inputParam = inputParams.get(i);
			// DataType dataType = inputParam.getDataType();
			// ValueType valueType = inputParam.getValueType();
			// if ((dataType == DataType.SCALAR) && (valueType ==
			// ValueType.INT)) {
			// sb.append("Integer ");
			// } else if ((dataType == DataType.SCALAR) && (valueType ==
			// ValueType.DOUBLE)) {
			// sb.append("Double ");
			// } else if ((dataType == DataType.SCALAR) && (valueType ==
			// ValueType.BOOLEAN)) {
			// sb.append("Boolean ");
			// } else if ((dataType == DataType.SCALAR) && (valueType ==
			// ValueType.STRING)) {
			// sb.append("String ");
			// } else {
			sb.append("Object ");
			// }
			sb.append(inputParam.getName());
		}

		sb.append(") {\n");
		sb.append("String scriptString = \"" + dmlFunctionCall + "\";\n");
		sb.append(
				"org.apache.sysml.api.mlcontext.Script script = new org.apache.sysml.api.mlcontext.Script(scriptString);\n");

		ArrayList<DataIdentifier> outputParams = fs.getOutputParams();
		if ((inputParams.size() > 0) || (outputParams.size() > 0)) {
			sb.append("script");
		}
		for (int i = 0; i < inputParams.size(); i++) {
			DataIdentifier inputParam = inputParams.get(i);
			String name = inputParam.getName();
			sb.append(".in(\"" + name + "\", " + name + ")");
		}
		for (int i = 0; i < outputParams.size(); i++) {
			DataIdentifier outputParam = outputParams.get(i);
			String name = outputParam.getName();
			sb.append(".out(\"" + name + "\")");
		}
		if ((inputParams.size() > 0) || (outputParams.size() > 0)) {
			sb.append(";\n");
		}

		sb.append("org.apache.sysml.api.mlcontext.MLResults results = script.execute();\n");
		sb.append("return results;\n");
		sb.append("}\n");
		return sb.toString();
	}

	public static String generateDmlFunctionCall(String scriptFilePath, FunctionStatement fs) {
		StringBuilder sb = new StringBuilder();
		sb.append("source('" + scriptFilePath + "') as mlcontextns;");

		ArrayList<DataIdentifier> outputParams = fs.getOutputParams();
		if (outputParams.size() == 1) {
			DataIdentifier outputParam = outputParams.get(0);
			sb.append(outputParam.getName());
			sb.append(" = mlcontextns::");
		} else if (outputParams.size() > 1) {
			sb.append("[");
			for (int i = 0; i < outputParams.size(); i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(outputParams.get(i).getName());
			}
			sb.append("] = mlcontextns::");
		}
		sb.append(fs.getName());
		sb.append("(");
		ArrayList<DataIdentifier> inputParams = fs.getInputParams();
		for (int i = 0; i < inputParams.size(); i++) {
			if (i > 0) {
				sb.append(", ");
			}
			DataIdentifier inputParam = inputParams.get(i);
			sb.append(inputParam.getName());
		}
		sb.append(");");
		return sb.toString();
	}

	public static String fileToString(String filePath) {
		try {
			File f = new File(filePath);
			FileReader fr = new FileReader(f);
			StringBuilder sb = new StringBuilder();
			int n;
			char[] charArray = new char[1024];
			while ((n = fr.read(charArray)) > 0) {
				sb.append(charArray, 0, n);
			}
			fr.close();
			return sb.toString();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String scriptConstructorBody(String scriptFilePath) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("String scriptFilePath = \"" + scriptFilePath + "\";");
		sb.append(
				"java.io.InputStream is = org.apache.sysml.api.mlcontext.Script.class.getResourceAsStream(\"/\"+scriptFilePath);");
		sb.append("java.io.InputStreamReader isr = new java.io.InputStreamReader(is);");
		sb.append("int n;");
		sb.append("char[] charArray = new char[1024];");
		sb.append("StringBuilder s = new StringBuilder();");
		sb.append("try {");
		sb.append("  while ((n = isr.read(charArray)) > 0) {");
		sb.append("    s.append(charArray, 0, n);");
		sb.append("  }");
		sb.append("} catch (java.io.IOException e) {");
		sb.append("  e.printStackTrace();");
		sb.append("}");
		sb.append("setScriptString(s.toString());");
		sb.append("}");
		return sb.toString();
	}

}
