package org.apache.sysml.api.mlcontext;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

/**
 * Convenience factory for creating DML and PYDML Script objects from strings, files, URLs, and input streams.
 *
 */
public class ScriptFactory {

	/**
	 * Creates a DML Script object from a file in the local file system. To create a DML Script object from a local file
	 * or HDFS/GPFS, please use createDMLScriptFromFile(String scriptFilePath).
	 * 
	 * @param localScriptFile
	 *            The local file containing DML.
	 * @return DML Script object.
	 */
	public static Script createDMLScriptFromFile(File localScriptFile) {
		return createScriptFromFile(localScriptFile, ScriptType.DML);
	}

	public static Script createDMLScriptFromFile(String scriptFilePath) {
		return createScriptFromFile(scriptFilePath, ScriptType.DML);
	}

	public static Script createDMLScriptFromInputStream(InputStream inputStream) {
		return createScriptFromInputStream(inputStream, ScriptType.DML);
	}

	public static Script dml(String scriptString) {
		return createDMLScriptFromString(scriptString);
	}
	
	public static Script pydml(String scriptString) {
		return createPYDMLScriptFromString(scriptString);
	}
	
	public static Script createDMLScriptFromString(String scriptString) {
		return createScriptFromString(scriptString, ScriptType.DML);
	}

	public static Script createDMLScriptFromUrl(String scriptUrlPath) {
		return createScriptFromUrl(scriptUrlPath, ScriptType.DML);
	}

	public static Script createDMLScriptFromUrl(URL scriptUrl) {
		return createScriptFromUrl(scriptUrl, ScriptType.DML);
	}

	/**
	 * Creates a PYDML Script object from a file in the local file system. To create a PYDML Script object from a local
	 * file or HDFS/GPFS, please use createPYDMLScriptFromFile(String scriptFilePath).
	 * 
	 * @param localScriptFile
	 *            The local file containing PYDML.
	 * @return PYDML Script object.
	 */
	public static Script createPYDMLScriptFromFile(File localScriptFile) {
		return createScriptFromFile(localScriptFile, ScriptType.PYDML);
	}

	public static Script createPYDMLScriptFromFile(String scriptFilePath) {
		return createScriptFromFile(scriptFilePath, ScriptType.PYDML);
	}

	public static Script createPYDMLScriptFromInputStream(InputStream inputStream) {
		return createScriptFromInputStream(inputStream, ScriptType.PYDML);
	}

	public static Script createPYDMLScriptFromString(String scriptString) {
		return createScriptFromString(scriptString, ScriptType.PYDML);
	}

	public static Script createPYDMLScriptFromUrl(String scriptUrlPath) {
		return createScriptFromUrl(scriptUrlPath, ScriptType.PYDML);
	}

	public static Script createPYDMLScriptFromUrl(URL scriptUrl) {
		return createScriptFromUrl(scriptUrl, ScriptType.PYDML);
	}

	/**
	 * Creates a DML or PYDML Script object from a file in the local file system. To create a Script object from a local
	 * file or HDFS/GPFS, please use createScriptFromFile(String scriptFilePath).
	 * 
	 * @param localScriptFile
	 *            The local file containing DML or PYDML.
	 * @param scriptType
	 *            ScriptType.DML or ScriptType.PYDML
	 * @return DML or PYDML Script object
	 */
	public static Script createScriptFromFile(File localScriptFile, ScriptType scriptType) {
		String scriptString = MLContextUtil.getScriptStringFromFile(localScriptFile);
		return createScriptFromString(scriptString, scriptType);
	}

	public static Script createScriptFromFile(String scriptFilePath, ScriptType scriptType) {
		String scriptString = MLContextUtil.getScriptStringFromFile(scriptFilePath);
		return createScriptFromString(scriptString, scriptType);
	}

	public static Script createScriptFromInputStream(InputStream inputStream, ScriptType scriptType) {
		String scriptString = MLContextUtil.getScriptStringFromInputStream(inputStream);
		return createScriptFromString(scriptString, scriptType);
	}

	public static Script createScriptFromString(String scriptString, ScriptType scriptType) {
		Script script = new Script(scriptString, scriptType);
		return script;
	}

	public static Script createScriptFromUrl(String scriptUrlPath, ScriptType scriptType) {
		String scriptString = MLContextUtil.getScriptStringFromUrl(scriptUrlPath);
		return createScriptFromString(scriptString, scriptType);
	}

	public static Script createScriptFromUrl(URL scriptUrl, ScriptType scriptType) {
		String scriptString = MLContextUtil.getScriptStringFromUrl(scriptUrl);
		return createScriptFromString(scriptString, scriptType);
	}
}
