package org.apache.sysml.api.mlcontext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.sysml.api.MLOutput;
import org.apache.sysml.api.mlcontext.matrix.BinaryBlockMatrix;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.cp.Data;

public class Script {

	private ScriptType scriptType;
	private String scriptString;

	private Map<String, Object> inputs = new LinkedHashMap<String, Object>();

	private List<String> inputVariableNames = new ArrayList<String>(); // "X", "Y", etc for registered inputs
	private List<String> outputVariableNames = new ArrayList<String>();
	private LocalVariableMap temporarySymbolTable = new LocalVariableMap(); // map of <"M", matrixObject> entries

	private MLOutput mlOutput;
	private ScriptExecutor scriptExecutor;

	public Script() {
		scriptType = ScriptType.DML;
	}

	public Script(ScriptType scriptType) {
		this.scriptType = scriptType;
	}

	public Script(String scriptString) {
		this.scriptString = scriptString;
	}

	public Script(String scriptString, ScriptType scriptType) {
		this.scriptString = scriptString;
		this.scriptType = scriptType;
	}

	public Script(String scriptString, Map<String, Object> inputs) {
		this.scriptString = scriptString;
		this.inputs = inputs;
	}

	public Script(String scriptString, Map<String, Object> inputs, ScriptType scriptType) {
		this.scriptString = scriptString;
		this.inputs = inputs;
		this.scriptType = scriptType;
	}

	public ScriptType getScriptType() {
		return scriptType;
	}

	public void setScriptType(ScriptType scriptType) {
		this.scriptType = scriptType;
	}

	public String getScriptString() {
		return scriptString;
	}

	public void setScriptString(String scriptString) {
		this.scriptString = scriptString;
	}

	public List<String> getInputVariableNames() {
		return inputVariableNames;
	}

	public void setInputVariableNames(ArrayList<String> inputVariableNames) {
		this.inputVariableNames = inputVariableNames;
	}

	public List<String> getOutputVariableNames() {
		return outputVariableNames;
	}

	public void setOutputVariableNames(ArrayList<String> outputVariableNames) {
		this.outputVariableNames = outputVariableNames;
	}

	public LocalVariableMap getTemporarySymbolTable() {
		return temporarySymbolTable;
	}

	public void setTemporarySymbolTable(LocalVariableMap temporarySymbolTable) {
		this.temporarySymbolTable = temporarySymbolTable;
	}

	public String[] getInputVariableNamesArray() {
		return (inputVariableNames == null) ? new String[0] : inputVariableNames.toArray(new String[0]);
	}

	public String[] getOutputVariableNamesArray() {
		return (outputVariableNames == null) ? new String[0] : outputVariableNames.toArray(new String[0]);
	}

	public void resetInputsAndOutputs() {
		inputVariableNames = null;
		outputVariableNames = null;
		temporarySymbolTable = null;
	}

	public Map<String, Object> getInputs() {
		return inputs;
	}

	public Script setInputs(Map<String, Object> inputs) {
		MLContextUtil.checkInputParameterValueTypes(inputs);
		this.inputs = inputs;
		Map<String, MatrixObject> matrixObjectMap = MLContextUtil.obtainComplexInputParameterMap(inputs);
		temporarySymbolTable = new LocalVariableMap();
		inputVariableNames = new ArrayList<String>();
		for (Entry<String, MatrixObject> entry : matrixObjectMap.entrySet()) {
			temporarySymbolTable.put(entry.getKey(), entry.getValue());
			inputVariableNames.add(entry.getKey());
		}
		return this;
	}

	public Script setInputs(Object... objs) {
		Map<String, Object> inputs = MLContextUtil.generateInputs(objs);
		setInputs(inputs);
		return this;
	}

	public Map<String, Object> getBasicInputParameters() {
		return MLContextUtil.obtainBasicInputParameterMap(inputs);
	}

	public Script putInput(String parameterName, Object parameterValue) {
		MLContextUtil.checkInputParameterValueType(parameterName, parameterValue);
		if (inputs == null) {
			inputs = new LinkedHashMap<String, Object>();
		}
		inputs.put(parameterName, parameterValue);

		MatrixObject matrixObject = MLContextUtil.convertComplexInputTypeIfNeeded(parameterName, parameterValue);
		if (matrixObject != null) {
			temporarySymbolTable.put(parameterName, matrixObject);
			// TODO this should really be a set or something to prevent problems
			inputVariableNames.add(parameterName);
		}
		return this;
	}

	public Script putOutput(String outputName) {
		outputVariableNames.add(outputName);
		return this;
	}

	public Script setOutputs(String... outputNames) {
		outputVariableNames = Arrays.asList(outputNames);
		return this;
	}

	public void clear() {
		inputs.clear();
		// outputs.clear();
		inputVariableNames.clear();
		outputVariableNames.clear();
		temporarySymbolTable = new LocalVariableMap();
	}

	public MLOutput getOutput() {
		return mlOutput;
	}

	public void setOutput(MLOutput output) {
		this.mlOutput = output;
	}

	public Script in(String parameterName, Object parameterValue) {
		putInput(parameterName, parameterValue);
		return this;
	}

	public Script regOut(String outputName) {
		putOutput(outputName);
		return this;
	}

	public BinaryBlockMatrix out(String outputName) {
		if ((outputVariableNames == null) || (outputVariableNames.size() == 0)) {
			throw new MLContextException("No output variables found");
		}

		if (temporarySymbolTable == null) {
			throw new MLContextException("The symbol table returned after executing the script is empty");
		}

		Data data = temporarySymbolTable.get(outputName);
		MatrixObject matrixObject = (MatrixObject) data;
		if (scriptExecutor == null) {
			throw new MLContextException("ScriptExecutor is null");
		}
		BinaryBlockMatrix matrix = new BinaryBlockMatrix(matrixObject, scriptExecutor.getExecutionContext());
		return matrix;
	}

	public ScriptExecutor getScriptExecutor() {
		return scriptExecutor;
	}

	public void setScriptExecutor(ScriptExecutor scriptExecutor) {
		this.scriptExecutor = scriptExecutor;
	}
	
}
