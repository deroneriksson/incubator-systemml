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

/**
 * Experimental.
 * 
 * Automatically generate classes and methods for interaction with DML scripts
 * and functions through the MLContext API.
 * 
 */
public class GenerateResourceClassesForMLContext {

	public static final String SOURCE = "scripts";
	public static final String DESTINATION = "target/classes";
	public static final String BASE_DEST_PACKAGE = "org.apache.sysml.api.mlcontext.resources";
	public static final String CONVENIENCE_BASE_DEST_PACKAGE = "org.apache.sysml.api.mlcontext.convenience";
	public static final String PATH_TO_MLCONTEXT_CLASS = "org/apache/sysml/api/mlcontext/MLContext.class";
	public static final String PATH_TO_MLRESULTS_CLASS = "org/apache/sysml/api/mlcontext/MLResults.class";
	public static final String PATH_TO_SCRIPT_CLASS = "org/apache/sysml/api/mlcontext/Script.class";
	public static final String PATH_TO_SCRIPTTYPE_CLASS = "org/apache/sysml/api/mlcontext/ScriptType.class";

	public static String source = SOURCE;
	public static String destination = DESTINATION;
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
			System.out.println("************************************");
			System.out.println("**** MLContext Class Generation ****");
			System.out.println("************************************");
			System.out.println("Source: " + source);
			System.out.println("Destination: " + destination);
			recurseDirectoriesForClassGeneration(source);
			String fullDirClassName = recurseDirectoriesForConvenienceClassGeneration(source);
			addConvenienceMethodsToMLContext(source, fullDirClassName);
		} finally {
			DMLScript.VALIDATOR_IGNORE_ISSUES = false;
		}
	}

	/**
	 * Add methods to MLContext to allow tab-completion to folders/packages
	 * (such as {@code ml.scripts()}).
	 * 
	 * @param source
	 *            path to source directory (typically, the scripts directory)
	 * @param fullDirClassName
	 *            the full name of the class representing the source (scripts)
	 *            directory
	 */
	public static void addConvenienceMethodsToMLContext(String source, String fullDirClassName) {
		try {
			File mlContextClassFile = new File(destination + File.separator + PATH_TO_MLCONTEXT_CLASS);
			InputStream is = new FileInputStream(mlContextClassFile);
			ClassPool pool = ClassPool.getDefault();
			CtClass ctMLContext = pool.makeClass(is);

			CtClass dirClass = pool.get(fullDirClassName);
			String methodName = convertFullClassNameToConvenienceMethodName(fullDirClassName);
			System.out.println("Adding " + methodName + "() to " + ctMLContext.getName());

			String methodBody = "{ " + fullDirClassName + " z = new " + fullDirClassName + "(); return z; }";
			CtMethod ctMethod = CtNewMethod.make(Modifier.PUBLIC, dirClass, methodName, null, null, methodBody,
					ctMLContext);
			ctMLContext.addMethod(ctMethod);

			addPackageConvenienceMethodsToMLContext(source, ctMLContext);

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

	/**
	 * Add methods to MLContext to allow tab-completion to packages contained
	 * within the source directory.
	 *
	 * @param dirPath
	 *            path to source directory (typically, the scripts directory)
	 * @param ctMLContext
	 *            javassist compile-time class representation of MLContext
	 */
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
				if (skipDir(subdir, false)) {
					continue;
				}

				String fullSubDirClassName = dirPathToFullDirClassName(subDirPath);

				ClassPool pool = ClassPool.getDefault();
				CtClass subDirClass = pool.get(fullSubDirClassName);
				String subDirName = subdir.getName();
				subDirName = subDirName.replaceAll("-", "_");
				subDirName = subDirName.toLowerCase();

				System.out.println("Adding " + subDirName + "() to " + ctMLContext.getName());

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

	/**
	 * Convert the full name of a class representing a directory to a method
	 * name.
	 * 
	 * @param fullDirClassName
	 *            the full name of the class representing a directory
	 * @return method name
	 */
	public static String convertFullClassNameToConvenienceMethodName(String fullDirClassName) {
		String m = fullDirClassName;
		m = m.substring(m.lastIndexOf(".") + 1);
		m = m.toLowerCase();
		return m;
	}

	/**
	 * Generate convenience classes recursively. This allows for code such as
	 * {@code ml.script.algorithms...}
	 * 
	 * @param dirPath
	 *            path to directory
	 * @return the full name of the class representing the dirPath directory
	 */
	public static String recurseDirectoriesForConvenienceClassGeneration(String dirPath) {
		try {
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
				if (skipDir(subdir, false)) {
					continue;
				}
				String fullSubDirClassName = recurseDirectoriesForConvenienceClassGeneration(subDirPath);

				CtClass subDirClass = pool.get(fullSubDirClassName);
				String subDirName = subdir.getName();
				subDirName = subDirName.replaceAll("-", "_");
				subDirName = subDirName.toLowerCase();

				System.out.println("Adding " + subDirName + "() to " + fullDirClassName);

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

	/**
	 * Convert a directory path to a full class name for a convenience class.
	 * 
	 * @param dirPath
	 *            path to directory
	 * @return the full name of the class representing the dirPath directory
	 */
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

	/**
	 * Whether or not the directory (and subdirectories of the directory) should
	 * be skipped.
	 * 
	 * @param dir
	 *            path to directory to check
	 * @param displayMessage
	 *            if {@code true}, display skip information to standard output
	 * @return {@code true} if the directory should be skipped, {@code false}
	 *         otherwise
	 */
	public static boolean skipDir(File dir, boolean displayMessage) {
		if ("staging".equalsIgnoreCase(dir.getName()) && skipStaging) {
			if (displayMessage) {
				System.out.println("Skipping staging directory: " + dir.getPath());
			}
			return true;
		}
		if ("perftest".equalsIgnoreCase(dir.getName()) && skipPerfTest) {
			if (displayMessage) {
				System.out.println("Skipping perftest directory: " + dir.getPath());
			}
			return true;
		}
		if ("obsolete".equalsIgnoreCase(dir.getName()) && skipObsolete) {
			if (displayMessage) {
				System.out.println("Skipping obsolete directory: " + dir.getPath());
			}
			return true;
		}
		return false;
	}

	/**
	 * Recursively traverse the directories to create classes representing the
	 * script files.
	 * 
	 * @param dirPath
	 *            path to directory
	 */
	public static void recurseDirectoriesForClassGeneration(String dirPath) {
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
			if (skipDir(subdir, true)) {
				continue;
			}
			recurseDirectoriesForClassGeneration(subdirpath);
		}
	}

	/**
	 * Iterate through the script files in a directory and create a class for
	 * each script file.
	 * 
	 * @param dir
	 *            the directory to iterate through
	 */
	public static void iterateScriptFilesInDirectory(File dir) {
		File[] scriptFiles = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return (name.toLowerCase().endsWith(".dml") || name.toLowerCase().endsWith(".pydml"));
			}
		});
		for (File scriptFile : scriptFiles) {
			String scriptFilePath = scriptFile.getPath();
			createScriptClass(scriptFilePath);
		}
	}

	/**
	 * Obtain the relative package for a script file. For example,
	 * {@code scripts/algorithms/LinearRegCG.dml} resolves to
	 * {@code scripts.algorithms}.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @return the relative package for a script file
	 */
	public static String scriptFilePathToPackageNoBase(String scriptFilePath) {
		String p = scriptFilePath;
		p = p.substring(0, p.lastIndexOf(File.separator));
		p = p.replace("-", "_");
		p = p.replace(File.separator, ".");
		p = p.toLowerCase();
		return p;
	}

	/**
	 * Obtain the simple class name for a script file. For example,
	 * {@code scripts/algorithms/LinearRegCG} resolves to {@code LinearRegCG}.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @return the simple class name for a script file
	 */
	public static String scriptFilePathToSimpleClassName(String scriptFilePath) {
		String c = scriptFilePath;
		c = c.substring(c.lastIndexOf(File.separator) + 1);
		c = c.replace("-", "_");
		c = c.substring(0, 1).toUpperCase() + c.substring(1);
		c = c.substring(0, c.indexOf("."));
		return c;
	}

	/**
	 * Obtain the relative full class name for a script file. For example,
	 * {@code scripts/algorithms/LinearRegCG.dml} resolves to
	 * {@code scripts.algorithms.LinearRegCG}.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @return the relative full class name for a script file
	 */
	public static String scriptFilePathToFullClassNameNoBase(String scriptFilePath) {
		String p = scriptFilePathToPackageNoBase(scriptFilePath);
		String c = scriptFilePathToSimpleClassName(scriptFilePath);
		return p + "." + c;
	}

	/**
	 * Convert a script file to a Java class that extends the MLContext API's
	 * Script class.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 */
	public static void createScriptClass(String scriptFilePath) {
		try {
			String fullScriptClassName = BASE_DEST_PACKAGE + "." + scriptFilePathToFullClassNameNoBase(scriptFilePath);
			System.out.println("Generating Class: " + fullScriptClassName);
			ClassPool pool = ClassPool.getDefault();
			CtClass ctNewScript = pool.makeClass(fullScriptClassName);

			File scriptClassFile = new File(destination + File.separator + PATH_TO_SCRIPT_CLASS);
			InputStream is = new FileInputStream(scriptClassFile);
			CtClass ctScript = pool.makeClass(is);
			pool.makeClass(new FileInputStream(new File(destination + File.separator + PATH_TO_SCRIPTTYPE_CLASS)));

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

	/**
	 * Create a DMLProgram from a script file.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @return the DMLProgram generated by the script file
	 */
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

	/**
	 * Add methods to a derived script class to allow invocation of script
	 * functions.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @param ctNewScript
	 *            the javassist compile-time class representation of a script
	 */
	public static void addFunctionMethods(String scriptFilePath, CtClass ctNewScript) {
		// if (("scripts/a.dml".equalsIgnoreCase(scriptFilePath)) ||
		// ("scripts/c.dml".equalsIgnoreCase(scriptFilePath))){
		// System.out.println("HOWDY");
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
					pool.makeClass(
							new FileInputStream(new File(destination + File.separator + PATH_TO_MLRESULTS_CLASS)));

					CtMethod m = CtNewMethod.make(functionCallMethod, ctNewScript);
					ctNewScript.addMethod(m);

					addDescriptionFunctionCallMethod(fs, scriptFilePath, ctNewScript, false);
					addDescriptionFunctionCallMethod(fs, scriptFilePath, ctNewScript, true);
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

	/**
	 * If a function has arguments, create a no-arguments method on a derived
	 * script class to return either: (1) the full function body, or (2) the
	 * function body, up to the end of the documentation comment for the
	 * function. If (1) is generated, the method name will be followed by an
	 * underscore. If (2), the method will not be generated if the function does
	 * not have any input parameters, since the method already exists on the
	 * derived script class.
	 * 
	 * @param fs
	 *            a SystemML function statement
	 * @param scriptFilePath
	 *            the path to a script file
	 * @param ctNewScript
	 *            the javassist compile-time class representation of a script
	 * @param full
	 *            if {@code true}, create method to return full function body;
	 *            if {@code false}, create method to return the function body up
	 *            to the end of the documentation comment
	 */
	public static void addDescriptionFunctionCallMethod(FunctionStatement fs, String scriptFilePath,
			CtClass ctNewScript, boolean full) {

		try {

			if ((fs.getInputParams().size() == 0) && (full == false)) {
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
				String line = sub.get(i);
				String escapeLine = StringEscapeUtils.escapeJava(line);
				sb.append(escapeLine);
				sb.append("\\n");
			}

			String functionString = sb.toString();
			String docFunctionCallMethod = generateDescriptionFunctionCallMethod(fs, functionString, full);
			CtMethod m = CtNewMethod.make(docFunctionCallMethod, ctNewScript);
			ctNewScript.addMethod(m);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (CannotCompileException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Obtain method for returning (1) the full function body, or (2) the
	 * function body up to the end of the documentation comment. (1) will have
	 * "_" appended to the end of the function name.
	 * 
	 * @param fs
	 *            a SystemML function statement
	 * @param functionString
	 *            either the full function body or the function body up to the
	 *            end of the documentation comment
	 * @param full
	 *            if {@code true}, append "_" to the end of the function name if
	 *            {@code false}, don't append "_" to the end of the function
	 *            name
	 * @return string representation of the function description method
	 */
	public static String generateDescriptionFunctionCallMethod(FunctionStatement fs, String functionString,
			boolean full) {
		StringBuilder sb = new StringBuilder();
		sb.append("public String ");
		sb.append(fs.getName());
		if (full) {
			sb.append("_");
		}
		sb.append("() {\n");
		sb.append("String docString = \"" + functionString + "\";\n");
		sb.append("return docString;\n");
		sb.append("}\n");
		return sb.toString();
	}

	/**
	 * Obtain method for invoking a script function.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @param fs
	 *            a SystemML function statement
	 * @param dmlFunctionCall
	 *            a string representing the invocation of a script function
	 * @return string representation of a method that performs a function call
	 */
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
			/*
			 * Note: Using Object is currently preferrable to using
			 * datatype/valuetype to explicitly set the input type to
			 * Integer/Double/Boolean/String since Object allows the automatic
			 * handling of things such as automatic conversions from longs to
			 * ints.
			 */
			sb.append("Object ");
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

	/**
	 * Obtain the DML representing a function invocation.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @param fs
	 *            a SystemML function statement
	 * @return string representation of a DML function invocation
	 */
	public static String generateDmlFunctionCall(String scriptFilePath, FunctionStatement fs) {
		StringBuilder sb = new StringBuilder();
		sb.append("source('" + scriptFilePath + "') as mlcontextns;");

		ArrayList<DataIdentifier> outputParams = fs.getOutputParams();
		if (outputParams.size() == 0) {
			sb.append("mlcontextns::");
		}
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

	/**
	 * Obtain the content of a file as a string.
	 * 
	 * @param filePath
	 *            the path to a file
	 * @return the file content as a string
	 */
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

	/**
	 * Obtain a constructor body for a Script subclass that sets the
	 * scriptString based on the content of a script file.
	 * 
	 * @param scriptFilePath
	 *            the path to a script file
	 * @return constructor body for a Script subclass that sets the scriptString
	 *         based on the content of a script file
	 */
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
