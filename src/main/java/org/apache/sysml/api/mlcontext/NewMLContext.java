package org.apache.sysml.api.mlcontext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sysml.api.MLContextProxy;
import org.apache.sysml.api.monitoring.SparkMonitoringUtil;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.IntIdentifier;
import org.apache.sysml.parser.StringIdentifier;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.caching.CacheableData;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysml.runtime.instructions.spark.functions.SparkListener;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.OutputInfo;

/**
 * The MLContext class offers programmatic access to SystemML on Spark from languages such as Scala and Java.
 *
 */
public class NewMLContext {

	/**
	 * Minimum Spark version supported by SystemML.
	 * 
	 * TODO: Move this to a more general class, such as DMLScript or a utility class
	 */
	public static final String SYSTEMML_MINIMUM_SPARK_VERSION = "1.3.0";

	/**
	 * Minimum Spark version required for monitoring MLContext performance.
	 */
	public static final String SYSTEMML_MINIMUM_SPARK_VERSION_MONITOR_MLCONTEXT_PERFORMANCE = "1.4.0";

	/**
	 * SparkContext object.
	 */
	private SparkContext sc = null;

	/**
	 * Class to monitor SystemML performance on Spark.
	 */
	private SparkMonitoringUtil monitoringUtil = null; // TODO: rename this class to SparkMonitor?

	/**
	 * Additional configuration properties.
	 */
	private Properties additionalConfigProperties = new Properties();
	private String configFilePath = null;

	private Script executingScript = null;
	/**
	 * This should not be necessary but due to the heavy use of statics in SystemML, it currently is used.
	 */
	private static NewMLContext activeMLContext = null;

	public static NewMLContext getActiveMLContext() {
		return activeMLContext;
	}

	/**
	 * Constructor to create an MLContext based on a SparkContext. By default, MLContext performance is not monitored
	 * and the execution type is specified to be hybrid Spark/Singlenode mode.
	 * 
	 * @param sparkContext
	 *            SparkContext
	 */
	public NewMLContext(SparkContext sparkContext) {
		this(sparkContext, false, MLContextExecutionType.SPARK_OR_SINGLENODE);
	}

	/**
	 * Constructor to create an MLContext based on a JavaSparkContext. By default, MLContext performance is not
	 * monitored and the execution type is specified to be hybrid Spark/Singlenode mode.
	 * 
	 * @param javaSparkContext
	 *            JavaSparkContext
	 */
	public NewMLContext(JavaSparkContext javaSparkContext) {
		this(javaSparkContext.sc(), false, MLContextExecutionType.SPARK_OR_SINGLENODE);
	}

	/**
	 * Constructor to create an MLContext based on a SparkContext, optionally monitoring performance and specifying
	 * whether MLContext should be executed solely on Spark or optionally in hybrid Spark/Singlenode mode.
	 * 
	 * @param sc
	 *            SparkContext object.
	 * @param monitorPerformance
	 *            Should MLContext performance be monitored.
	 * @param mlContextExecutionType
	 *            MLContextExecutionType.SPARK_ONLY or MLContextExecutionType.SPARK_OR_SINGLENODE
	 */
	public NewMLContext(SparkContext sc, boolean monitorPerformance, MLContextExecutionType mlContextExecutionType) {
		this.sc = sc;

		MLContextUtil.setConfig();
		MLContextProxy.setActive(true); // WHY STATICS???????????????????????
		MLContextUtil.verifySparkVersionSupported(sc);
		MLContextUtil.setRuntimePlatform(mlContextExecutionType);

		if (monitorPerformance) {
			MLContextUtil.verifySparkVersionSupportedForMonitoringMLContextPerformance(sc);
			SparkListener sparkListener = new SparkListener(sc);
			monitoringUtil = new SparkMonitoringUtil(sparkListener);
			sc.addSparkListener(sparkListener);
		}

		NewMLContext.activeMLContext = this;
	}

	/**
	 * Clear MLContext state. To clear any configuration, please use reset(true);
	 */
	public void reset() {
		reset(false);
	}

	/**
	 * Clear MLContext state. If cleanupConfig is <code>true</code>, any additional configuration properties set via
	 * setConfigProperty(..) will also be cleared.
	 * 
	 * @param cleanupConfig
	 *            Whether or not the additional configuration properties should be cleared.
	 */
	public void reset(boolean cleanupConfig) {
		// Cleanup variables from bufferpool, including evicted files, because bufferpool holds references.
		CacheableData.cleanupCacheDir();

		// Clear MLContext state
		// inputVariableNames = null;
		// outputVariableNames = null;
		// temporarySymbolTable = null;

		if (cleanupConfig) {
			additionalConfigProperties.clear();
		}
	}

	/**
	 * Set configuration property, such as setConfigProperty("localtmpdir", "/tmp/systemml")
	 * 
	 * @param paramName
	 *            Parameter name.
	 * @param paramVal
	 *            Parameter value.
	 */
	public void setConfigProperty(String propertyName, String propertyValue) {
		additionalConfigProperties.put(propertyName, propertyValue);
	}

	/**
	 * Execute a DML or PYDML Script object.
	 * 
	 * @param script
	 *            The Script object to execute.
	 */
	public void execute(Script script) {
		DMLConfig config = ConfigurationManager.getConfig();
		ScriptExecutor scriptExecutor = new ScriptExecutor(config, monitoringUtil);
		execute(script, scriptExecutor);
	}

	/**
	 * Execute a DML or PYDML Script object using a ScriptExecutor. The ScriptExecutor class can be extended to allow
	 * for modifying the default execution pathway.
	 * 
	 * @param script
	 * @param scriptExecutor
	 */
	public void execute(Script script, ScriptExecutor scriptExecutor) {
		try {
			this.executingScript = script;
			// NewMLContext.activeMLContext = this;
			scriptExecutor.execute(script);
		} finally { // statics.... fun....
		// NewMLContext.activeMLContext = null;
		}
	}

	// /**
	// * This is called by MLContextProxy.
	// *
	// * @param instructions
	// */
	// TODO: move this to be performed on script class
	// TODO: make this package scope (instead of public) when MLContextProxy and this MLContext are in the same package
	public ArrayList<Instruction> performCleanupAfterRecompilation(ArrayList<Instruction> instructions) {
		if (executingScript == null) {
			return instructions;
		}
		List<String> outputVariableNames = executingScript.getOutputVariableNames();
		if (outputVariableNames == null) {
			return instructions;
		}

		for (int i = 0; i < instructions.size(); i++) {
			Instruction inst = instructions.get(i);
			if (inst instanceof VariableCPInstruction && ((VariableCPInstruction) inst).isRemoveVariable()) {
				VariableCPInstruction varInst = (VariableCPInstruction) inst;
				for (String outputVariableName : outputVariableNames)
					if (varInst.isRemoveVariable(outputVariableName)) {
						instructions.remove(i);
						i--;
						break;
					}
			}
		}
		return instructions;
	}

	public void setConfigFilePath(String configFilePath) {
		this.configFilePath = configFilePath;
	}

	/**
	 * Used by MLProxy. Mostly copy/pasted from old MLContext.
	 * 
	 * @param source
	 * @param target
	 */
	public void setAppropriateVarsForRead(Expression source, String target) {
		boolean isTargetRegistered = isRegisteredAsInput(target);
		boolean isReadExpression = (source instanceof DataExpression && ((DataExpression) source).isRead());
		if (isTargetRegistered && isReadExpression) {
			// Do not check metadata file for registered reads
			((DataExpression) source).setCheckMetadata(false);

			MatrixObject mo = getMatrixObject(target);
			int blp = source.getBeginLine();
			int bcp = source.getBeginColumn();
			int elp = source.getEndLine();
			int ecp = source.getEndColumn();
			((DataExpression) source).addVarParam(DataExpression.READROWPARAM, new IntIdentifier(mo.getNumRows(),
					source.getFilename(), blp, bcp, elp, ecp));
			((DataExpression) source).addVarParam(DataExpression.READCOLPARAM, new IntIdentifier(mo.getNumColumns(),
					source.getFilename(), blp, bcp, elp, ecp));
			((DataExpression) source).addVarParam(DataExpression.READNUMNONZEROPARAM, new IntIdentifier(mo.getNnz(),
					source.getFilename(), blp, bcp, elp, ecp));
			((DataExpression) source).addVarParam(DataExpression.DATATYPEPARAM,
					new StringIdentifier("matrix", source.getFilename(), blp, bcp, elp, ecp));
			((DataExpression) source).addVarParam(DataExpression.VALUETYPEPARAM,
					new StringIdentifier("double", source.getFilename(), blp, bcp, elp, ecp));

			if (mo.getMetaData() instanceof MatrixFormatMetaData) {
				MatrixFormatMetaData metaData = (MatrixFormatMetaData) mo.getMetaData();
				if (metaData.getOutputInfo() == OutputInfo.CSVOutputInfo) {
					((DataExpression) source).addVarParam(DataExpression.FORMAT_TYPE, new StringIdentifier(
							DataExpression.FORMAT_TYPE_VALUE_CSV, source.getFilename(), blp, bcp, elp, ecp));
				} else if (metaData.getOutputInfo() == OutputInfo.TextCellOutputInfo) {
					((DataExpression) source).addVarParam(DataExpression.FORMAT_TYPE, new StringIdentifier(
							DataExpression.FORMAT_TYPE_VALUE_TEXT, source.getFilename(), blp, bcp, elp, ecp));
				} else if (metaData.getOutputInfo() == OutputInfo.BinaryBlockOutputInfo) {
					((DataExpression) source).addVarParam(DataExpression.ROWBLOCKCOUNTPARAM,
							new IntIdentifier(mo.getNumRowsPerBlock(), source.getFilename(), blp, bcp, elp, ecp));
					((DataExpression) source).addVarParam(DataExpression.COLUMNBLOCKCOUNTPARAM,
							new IntIdentifier(mo.getNumColumnsPerBlock(), source.getFilename(), blp, bcp, elp, ecp));
					((DataExpression) source).addVarParam(DataExpression.FORMAT_TYPE, new StringIdentifier(
							DataExpression.FORMAT_TYPE_VALUE_BINARY, source.getFilename(), blp, bcp, elp, ecp));
				} else {
					throw new MLContextException("Unsupported format through MLContext");
				}
			}

		}
	}

	private boolean isRegisteredAsInput(String parameterName) {
		if (executingScript != null) {
			List<String> inputVariableNames = executingScript.getInputVariableNames();
			if (inputVariableNames != null) {
				for (String v : inputVariableNames) {
					if (v.compareTo(parameterName) == 0) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private MatrixObject getMatrixObject(String parameterName) {
		if (executingScript != null) {
			LocalVariableMap temporarySymbolTable = executingScript.getTemporarySymbolTable();
			if (temporarySymbolTable != null) {
				Data matrixObject = temporarySymbolTable.get(parameterName);
				if (matrixObject instanceof MatrixObject) {
					return (MatrixObject) matrixObject;
				} else {
					throw new MLContextException("ERROR: Incorrect type");
				}
			}
		}
		throw new MLContextException("ERROR: getMatrixObject not set for parameter:" + parameterName);
	}

	public SparkMonitoringUtil getMonitoringUtil() {
		return monitoringUtil;
	}

	public SparkContext getSparkContext() {
		return sc;
	}

}
