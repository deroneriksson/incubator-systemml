package org.apache.sysml.api.mlcontext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.MLOutput;
import org.apache.sysml.api.jmlc.JMLCUtils;
import org.apache.sysml.api.monitoring.SparkMonitoringUtil;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.hops.HopsException;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.hops.OptimizerUtils.OptimizationLevel;
import org.apache.sysml.hops.globalopt.GlobalOptimizerWrapper;
import org.apache.sysml.hops.rewrite.ProgramRewriter;
import org.apache.sysml.hops.rewrite.RewriteRemovePersistentReadWrite;
import org.apache.sysml.lops.LopsException;
import org.apache.sysml.parser.AParserWrapper;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DMLTranslator;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.LanguageException;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLUnsupportedOperationException;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContextFactory;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.utils.Explain;
import org.apache.sysml.utils.Explain.ExplainCounts;
import org.apache.sysml.utils.Statistics;

/**
 * ScriptExecutor executes a DML or PYDML Script object using SystemML.
 *
 */
public class ScriptExecutor {

	protected DMLConfig config;
	protected SparkMonitoringUtil sparkMonitoringUtil;

	protected DMLProgram dmlProgram;
	protected DMLTranslator dmlTranslator;
	protected Program runtimeProgram;
	protected ExecutionContext executionContext;
	protected Script script;

	public ScriptExecutor() {
		config = new DMLConfig();
	}

	public ScriptExecutor(DMLConfig config) {
		this.config = config;
	}

	public ScriptExecutor(DMLConfig config, SparkMonitoringUtil sparkMonitoringUtil) {
		this.config = config;
		this.sparkMonitoringUtil = sparkMonitoringUtil;
	}

	protected void constructAndRewriteHops() {
		try {
			dmlTranslator.constructHops(dmlProgram);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while constructing HOPS (high-order operations)", e);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred while constructing HOPS (high-order operations)", e);
		}
		try {
			dmlTranslator.rewriteHopsDAG(dmlProgram);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
		} catch (HopsException e) {
			throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
		}
		try { // TODO: change this to output to a log4j logger
				// System.out.println(Explain.explain(dmlProgram));
			Explain.explain(dmlProgram);
		} catch (HopsException e) {
			throw new MLContextException("Exception occurred while explaining dml program", e);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception occurred while explaining dml program", e);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while explaining dml program", e);
		}
	}

	protected void constructLopsAndGenerateRuntimeProgram() {
		try {
			dmlTranslator.constructLops(dmlProgram);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
		} catch (HopsException e) {
			throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
		} catch (LopsException e) {
			throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
		}
		try {
			runtimeProgram = dmlProgram.getRuntimeProgram(config);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while generating runtime program", e);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception occurred while generating runtime program", e);
		} catch (LopsException e) {
			throw new MLContextException("Exception occurred while generating runtime program", e);
		} catch (DMLUnsupportedOperationException e) {
			throw new MLContextException("Exception occurred while generating runtime program", e);
		} catch (IOException e) {
			throw new MLContextException("Exception occurred while generating runtime program", e);
		}
	}

	protected void countCompiledMRJobsAndSPInstructions() {
		ExplainCounts counts = Explain.countDistributedOperations(runtimeProgram);
		Statistics.resetNoOfCompiledJobs(counts.numJobs);
	}

	protected void createAndPopulateExecutionContext() {
		executionContext = ExecutionContextFactory.createContext(runtimeProgram);
		LocalVariableMap temporarySymbolTable = script.getTemporarySymbolTable();
		if (temporarySymbolTable != null) {
			executionContext.setVariables(temporarySymbolTable);
		}
	}

	public void execute(Script script) {
		if (script == null) {
			throw new MLContextException("Can't execute null Script");
		}
		this.script = script;
		try {
			AParserWrapper.IGNORE_UNSPECIFIED_ARGS = true;
			DataExpression.REJECT_READ_WRITE_UNKNOWNS = false;

			checkScriptHasTypeAndString();

			setScriptStringInSparkMonitor();

			parseScript();
			validateScript();
			constructAndRewriteHops();
			rewritePersistentReadsAndWrites();
			constructLopsAndGenerateRuntimeProgram();
			optionalGlobalDataFlowOptimization();
			countCompiledMRJobsAndSPInstructions();
			initializeCachingAndScratchSpace();
			cleanupRuntimeProgram();
			createAndPopulateExecutionContext();
			executeRuntimeProgram();

			setExplainRuntimeProgramInSparkMonitor();

			collectOutput();
		} finally {
			AParserWrapper.IGNORE_UNSPECIFIED_ARGS = false;
			DataExpression.REJECT_READ_WRITE_UNKNOWNS = true;
		}

	}

	protected void cleanupRuntimeProgram() {
		JMLCUtils.cleanupRuntimeProgram(runtimeProgram, script.getOutputVariableNamesArray());		
	}
	
	protected void collectOutput() {
		List<String> outputVariableNames = script.getOutputVariableNames();
		if ((outputVariableNames == null) || (outputVariableNames.size() == 0)) {
			return;
		}

		LocalVariableMap temporarySymbolTable = script.getTemporarySymbolTable();
		if (temporarySymbolTable == null) {
			throw new MLContextException("The symbol table returned after executing the script is empty");
		}

		// TODO: JavaPairRDD<MatrixIndexes,MatrixBlock> and MatrixCharacteristics should be encapsulated into
		// BinaryBlockMatrix class
		// TODO: MLOutput class should take Maps rather than HashMaps
		HashMap<String, JavaPairRDD<MatrixIndexes, MatrixBlock>> binaryBlockOutputs = new HashMap<String, JavaPairRDD<MatrixIndexes, MatrixBlock>>();
		HashMap<String, MatrixCharacteristics> binaryBlockOutputsMetadata = new HashMap<String, MatrixCharacteristics>();
		// If support JMLC, then need to do something about SparkExecutionContext.
		SparkExecutionContext sparkExecutionContext = (SparkExecutionContext) executionContext;

		for (String outputVariableName : outputVariableNames) {
			if (temporarySymbolTable.keySet().contains(outputVariableName)) {
				JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlockMatrix = null;
				MatrixCharacteristics binaryBlockMatrixCharacteristics = null;

				try {
					binaryBlockMatrix = sparkExecutionContext.getBinaryBlockRDDHandleForVariable(outputVariableName);
					binaryBlockOutputs.put(outputVariableName, binaryBlockMatrix);
				} catch (DMLRuntimeException e) {
					throw new MLContextException("Exception obtaining binary block handle for output variable: "
							+ outputVariableName);
				} catch (DMLUnsupportedOperationException e) {
					throw new MLContextException("Exception obtaining binary block handle for output variable: "
							+ outputVariableName);
				}
				try {
					binaryBlockMatrixCharacteristics = sparkExecutionContext
							.getMatrixCharacteristics(outputVariableName);
					binaryBlockOutputsMetadata.put(outputVariableName, binaryBlockMatrixCharacteristics);
				} catch (DMLRuntimeException e) {
					throw new MLContextException("Exception obtaining matrix characteristics for output variable: "
							+ outputVariableName);
				}

			} else {
				throw new MLContextException("The variable " + outputVariableName
						+ " is not available as output after executing the script");
			}
		}

		MLOutput mlOutput = new MLOutput(binaryBlockOutputs, binaryBlockOutputsMetadata);
		script.setMlOutput(mlOutput);

	}

	protected void executeRuntimeProgram() {
		try {
			runtimeProgram.execute(executionContext);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception occurred while executing runtime program", e);
		} catch (DMLUnsupportedOperationException e) {
			throw new MLContextException("Exception occurred while executing runtime program", e);
		}
	}

	public DMLConfig getConfig() {
		return config;
	}

	public SparkMonitoringUtil getSparkMonitoringUtil() {
		return sparkMonitoringUtil;
	}

	// TODO: CHECK IF THIS IS ACTUALLY NEEDED FROM SPARK
	protected void initializeCachingAndScratchSpace() {
		try {
			DMLScript.initHadoopExecution(config);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception occurred initializing caching and scratch space", e);
		} catch (IOException e) {
			throw new MLContextException("Exception occurred initializing caching and scratch space", e);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred initializing caching and scratch space", e);
		}
	}

	protected void optionalGlobalDataFlowOptimization() {
		if (OptimizerUtils.isOptLevel(OptimizationLevel.O4_GLOBAL_TIME_MEMORY)) {
			try {
				runtimeProgram = GlobalOptimizerWrapper.optimizeProgram(dmlProgram, runtimeProgram);
			} catch (DMLRuntimeException e) {
				throw new MLContextException("Exception occurred during global data flow optimization", e);
			} catch (DMLUnsupportedOperationException e) {
				throw new MLContextException("Exception occurred during global data flow optimization", e);
			} catch (HopsException e) {
				throw new MLContextException("Exception occurred during global data flow optimization", e);
			} catch (LopsException e) {
				throw new MLContextException("Exception occurred during global data flow optimization", e);
			}
		}
	}

	protected void parseScript() {
		try {
			AParserWrapper parser = AParserWrapper.createParser(script.getScriptType().isPYDML());
			Map<String, Object> basicInputParameters = script.getBasicInputParameters();
			HashMap<String, String> basicInputParametersStrings = MLContextUtil.convertBasicInputParametersForParser(
					basicInputParameters, script.getScriptType());
			// HashMap<String, String> argVals = new HashMap<String, String>();
			// TODO: update parser.parse method to take a map rather than a hashmap
			// System.out.println("INPUTS:" + basicInputParametersStrings);
			dmlProgram = parser.parse(null, script.getScriptString(), basicInputParametersStrings);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred while parsing script", e);
		}
	}

	protected void rewritePersistentReadsAndWrites() {
		LocalVariableMap temporarySymbolTable = script.getTemporarySymbolTable();
		if (temporarySymbolTable != null) {
			String[] inputs = script.getInputVariableNamesArray();
			String[] outputs = script.getOutputVariableNamesArray();
			RewriteRemovePersistentReadWrite rewrite = new RewriteRemovePersistentReadWrite(inputs, outputs);
			ProgramRewriter programRewriter = new ProgramRewriter(rewrite);
			try {
				programRewriter.rewriteProgramHopDAGs(dmlProgram);
			} catch (LanguageException e) {
				throw new MLContextException("Exception occurred while rewriting persistent reads and writes", e);
			} catch (HopsException e) {
				throw new MLContextException("Exception occurred while rewriting persistent reads and writes", e);
			}
		}

	}

	public void setConfig(DMLConfig config) {
		this.config = config;
	}

	protected void setExplainRuntimeProgramInSparkMonitor() {
		if (sparkMonitoringUtil != null) {
			try {
				String explainOutput = Explain.explain(runtimeProgram);
				sparkMonitoringUtil.setExplainOutput(explainOutput);
			} catch (HopsException e) {
				throw new MLContextException("Exception occurred while explaining runtime program", e);
			}
		}

	}

	protected void setScriptStringInSparkMonitor() {
		if (sparkMonitoringUtil != null) {
			// TODO: DMLString can be DML or PyDML so maybe change setDMLString to setScriptString
			sparkMonitoringUtil.setDMLString(script.getScriptString());
		}
	}

	public void setSparkMonitoringUtil(SparkMonitoringUtil sparkMonitoringUtil) {
		this.sparkMonitoringUtil = sparkMonitoringUtil;
	}

	protected void validateScript() {
		try {
			dmlTranslator = new DMLTranslator(dmlProgram);
			dmlTranslator.liveVariableAnalysis(dmlProgram);
			dmlTranslator.validateParseTree(dmlProgram);
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception occurred while validating script", e);
		} catch (LanguageException e) {
			throw new MLContextException("Exception occurred while validating script", e);
		} catch (ParseException e) {
			throw new MLContextException("Exception occurred while validating script", e);
		} catch (IOException e) {
			throw new MLContextException("Exception occurred while validating script", e);
		}
	}

	protected void checkScriptHasTypeAndString() {
		if (script == null) {
			throw new MLContextException("Script is null");
		} else if (script.getScriptType() == null) {
			throw new MLContextException("ScriptType (DML or PYDML) needs to be specified");
		} else if (script.getScriptString() == null) {
			throw new MLContextException("Script string is null");
		} else if (StringUtils.isBlank(script.getScriptString())) {
			throw new MLContextException("Script string is blank");
		}
	}

	// protected void constructAndRewriteHops(DMLProgram dmlProgram, DMLTranslator dmlTranslator) {
	// try {
	// dmlTranslator.constructHops(dmlProgram);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while constructing HOPS (high-order operations)", e);
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred while constructing HOPS (high-order operations)", e);
	// }
	// try {
	// dmlTranslator.rewriteHopsDAG(dmlProgram);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred while rewriting HOPS (high-order operations)", e);
	// }
	// try { // TODO: change this to output to a log4j logger
	// System.out.println(Explain.explain(dmlProgram));
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred while explaining dml program", e);
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred while explaining dml program", e);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while explaining dml program", e);
	// }
	// }
	//
	// protected Program constructLopsAndGenerateRuntimeProgram(DMLProgram dmlProgram, DMLTranslator dmlTranslator) {
	// try {
	// dmlTranslator.constructLops(dmlProgram);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
	// } catch (LopsException e) {
	// throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred while constructing LOPS (low-order operations)", e);
	// }
	// try {
	// Program runtimeProgram = dmlProgram.getRuntimeProgram(config);
	// return runtimeProgram;
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while generating runtime program", e);
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred while generating runtime program", e);
	// } catch (LopsException e) {
	// throw new MLContextException("Exception occurred while generating runtime program", e);
	// } catch (DMLUnsupportedOperationException e) {
	// throw new MLContextException("Exception occurred while generating runtime program", e);
	// } catch (IOException e) {
	// throw new MLContextException("Exception occurred while generating runtime program", e);
	// }
	// }
	//
	// protected void countCompiledMRJobsAndSPInstructions(Program runtimeProgram) {
	// ExplainCounts counts = Explain.countDistributedOperations(runtimeProgram);
	// Statistics.resetNoOfCompiledJobs(counts.numJobs);
	// }
	//
	// protected ExecutionContext createAndPopulateExecutionContext(Script script, Program runtimeProgram) {
	// ExecutionContext executionContext = ExecutionContextFactory.createContext(runtimeProgram);
	// LocalVariableMap temporarySymbolTable = script.getTemporarySymbolTable();
	// if (temporarySymbolTable != null) {
	// executionContext.setVariables(temporarySymbolTable);
	// }
	// return executionContext;
	// }
	//
	// public void execute(Script script) {
	// try {
	// AParserWrapper.IGNORE_UNSPECIFIED_ARGS = true;
	// DataExpression.REJECT_READ_WRITE_UNKNOWNS = false;
	//
	// checkScriptHasTypeAndString(script);
	//
	// setScriptStringInSparkMonitor(script);
	//
	// DMLProgram dmlProgram = parseScript(script);
	// DMLTranslator dmlTranslator = validateScript(dmlProgram);
	// constructAndRewriteHops(dmlProgram, dmlTranslator);
	// rewritePersistentReadsAndWrites(script, dmlProgram);
	// Program runtimeProgram = constructLopsAndGenerateRuntimeProgram(dmlProgram, dmlTranslator);
	// optionalGlobalDataFlowOptimization(dmlProgram, runtimeProgram);
	// countCompiledMRJobsAndSPInstructions(runtimeProgram);
	// initializeCachingAndScratchSpace();
	// ExecutionContext executionContext = createAndPopulateExecutionContext(script, runtimeProgram);
	// executeRuntimeProgram(runtimeProgram, executionContext);
	//
	// setExplainRuntimeProgramInSparkMonitor(runtimeProgram);
	// } finally {
	// AParserWrapper.IGNORE_UNSPECIFIED_ARGS = false;
	// DataExpression.REJECT_READ_WRITE_UNKNOWNS = true;
	// }
	//
	// }
	//
	// protected void executeRuntimeProgram(Program runtimeProgram, ExecutionContext executionContext) {
	// try {
	// runtimeProgram.execute(executionContext);
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred while executing runtime program", e);
	// } catch (DMLUnsupportedOperationException e) {
	// throw new MLContextException("Exception occurred while executing runtime program", e);
	// }
	// }
	//
	// public DMLConfig getConfig() {
	// return config;
	// }
	//
	// public SparkMonitoringUtil getSparkMonitoringUtil() {
	// return sparkMonitoringUtil;
	// }
	//
	// // TODO: CHECK IF THIS IS ACTUALLY NEEDED FROM SPARK
	// protected void initializeCachingAndScratchSpace() {
	// try {
	// DMLScript.initHadoopExecution(config);
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred initializing caching and scratch space", e);
	// } catch (IOException e) {
	// throw new MLContextException("Exception occurred initializing caching and scratch space", e);
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred initializing caching and scratch space", e);
	// }
	// }
	//
	// protected void optionalGlobalDataFlowOptimization(DMLProgram dmlProgram, Program runtimeProgram) {
	// if (OptimizerUtils.isOptLevel(OptimizationLevel.O4_GLOBAL_TIME_MEMORY)) {
	// try {
	// runtimeProgram = GlobalOptimizerWrapper.optimizeProgram(dmlProgram, runtimeProgram);
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred during global data flow optimization", e);
	// } catch (DMLUnsupportedOperationException e) {
	// throw new MLContextException("Exception occurred during global data flow optimization", e);
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred during global data flow optimization", e);
	// } catch (LopsException e) {
	// throw new MLContextException("Exception occurred during global data flow optimization", e);
	// }
	// }
	// }
	//
	// protected DMLProgram parseScript(Script script) {
	// try {
	// AParserWrapper parser = AParserWrapper.createParser(script.getScriptType().isPYDML());
	// Map<String, Object> basicInputParameters = script.getBasicInputParameters();
	// HashMap<String, String> basicInputParametersStrings = MLContextUtil.convertBasicInputParametersForParser(
	// basicInputParameters, script.getScriptType());
	// // HashMap<String, String> argVals = new HashMap<String, String>();
	// // TODO: update parser.parse method to take a map rather than a hashmap
	// System.out.println("INPUTS:" + basicInputParametersStrings);
	// DMLProgram dmlProgram = parser.parse(null, script.getScriptString(), basicInputParametersStrings);
	// return dmlProgram;
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred while parsing script", e);
	// }
	// }
	//
	// protected void rewritePersistentReadsAndWrites(Script script, DMLProgram dmlProgram) {
	// LocalVariableMap temporarySymbolTable = script.getTemporarySymbolTable();
	// if (temporarySymbolTable != null) {
	// String[] inputs = script.getInputVariableNamesArray();
	// String[] outputs = script.getOutputVariableNamesArray();
	// RewriteRemovePersistentReadWrite rewrite = new RewriteRemovePersistentReadWrite(inputs, outputs);
	// ProgramRewriter programRewriter = new ProgramRewriter(rewrite);
	// try {
	// programRewriter.rewriteProgramHopDAGs(dmlProgram);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while rewriting persistent reads and writes", e);
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred while rewriting persistent reads and writes", e);
	// }
	// }
	//
	// }
	//
	// public void setConfig(DMLConfig config) {
	// this.config = config;
	// }
	//
	// protected void setExplainRuntimeProgramInSparkMonitor(Program runtimeProgram) {
	// if (sparkMonitoringUtil != null) {
	// try {
	// String explainOutput = Explain.explain(runtimeProgram);
	// sparkMonitoringUtil.setExplainOutput(explainOutput);
	// } catch (HopsException e) {
	// throw new MLContextException("Exception occurred while explaining runtime program", e);
	// }
	// }
	//
	// }
	//
	// protected void setScriptStringInSparkMonitor(Script script) {
	// if (sparkMonitoringUtil != null) {
	// // TODO: DMLString can be DML or PyDML so maybe change setDMLString to setScriptString
	// sparkMonitoringUtil.setDMLString(script.getScriptString());
	// }
	// }
	//
	// public void setSparkMonitoringUtil(SparkMonitoringUtil sparkMonitoringUtil) {
	// this.sparkMonitoringUtil = sparkMonitoringUtil;
	// }
	//
	// protected DMLTranslator validateScript(DMLProgram dmlProgram) {
	// try {
	// DMLTranslator dmlTranslator = new DMLTranslator(dmlProgram);
	// dmlTranslator.liveVariableAnalysis(dmlProgram);
	// dmlTranslator.validateParseTree(dmlProgram);
	// return dmlTranslator;
	// } catch (DMLRuntimeException e) {
	// throw new MLContextException("Exception occurred while validating script", e);
	// } catch (LanguageException e) {
	// throw new MLContextException("Exception occurred while validating script", e);
	// } catch (ParseException e) {
	// throw new MLContextException("Exception occurred while validating script", e);
	// } catch (IOException e) {
	// throw new MLContextException("Exception occurred while validating script", e);
	// }
	// }
	//
	// protected void checkScriptHasTypeAndString(Script script) {
	// if (script == null) {
	// throw new MLContextException("Script is null");
	// } else if (script.getScriptType() == null) {
	// throw new MLContextException("ScriptType (DML or PYDML) needs to be specified");
	// } else if (script.getScriptString() == null) {
	// throw new MLContextException("Script string is null");
	// } else if (StringUtils.isBlank(script.getScriptString())) {
	// throw new MLContextException("Script string is blank");
	// }
	// }

}
