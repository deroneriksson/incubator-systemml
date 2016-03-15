package org.apache.sysml.api.mlcontext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.DMLScript.RUNTIME_PLATFORM;
import org.apache.sysml.api.MLContextProxy;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.DMLTranslator;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.spark.data.RDDObject;
import org.apache.sysml.runtime.instructions.spark.functions.ConvertStringToLongTextPair;
import org.apache.sysml.runtime.instructions.spark.functions.CopyBlockPairFunction;
import org.apache.sysml.runtime.instructions.spark.functions.CopyTextInputFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.DataFrameAnalysisFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDConverterUtilsExt.DataFrameToBinaryBlockFunction;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.MatrixFormatMetaData;
import org.apache.sysml.runtime.matrix.data.InputInfo;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.runtime.util.LocalFileUtils;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * Utility class containing useful methods for working with MLContext.
 *
 */
public class MLContextUtil {

	@SuppressWarnings("rawtypes")
	public static final Class[] SUPPORTED_BASIC_TYPES = { Integer.class, Boolean.class, Double.class, String.class };
	@SuppressWarnings("rawtypes")
	public static final Class[] SUPPORTED_COMPLEX_TYPES = { JavaRDD.class, RDD.class, DataFrame.class,
			(new double[][] {}).getClass() };
	@SuppressWarnings("rawtypes")
	public static final Class[] ALL_SUPPORTED_TYPES = (Class[]) ArrayUtils.addAll(SUPPORTED_BASIC_TYPES,
			SUPPORTED_COMPLEX_TYPES);

	/**
	 * Compare two version strings (ie, "1.4.0" and "1.4.1").
	 * 
	 * @param versionStr1
	 *            First version string.
	 * @param versionStr2
	 *            Second version string.
	 * @return If versionStr1 is less than versionStr2, return -1. If versionStr1 equals versionStr2, return 0. If
	 *         versionStr1 is greater than versionStr2, return 1.
	 * @throws MLContextException
	 *             if versionStr1 or versionStr2 is null.
	 */
	private static int compareVersion(String versionStr1, String versionStr2) {
		if (versionStr1 == null) {
			throw new MLContextException("First version argument to compareVersion() is null");
		}
		if (versionStr2 == null) {
			throw new MLContextException("Second version argument to compareVersion() is null");
		}

		Scanner scanner1 = null;
		Scanner scanner2 = null;
		try {
			scanner1 = new Scanner(versionStr1);
			scanner2 = new Scanner(versionStr2);
			scanner1.useDelimiter("\\.");
			scanner2.useDelimiter("\\.");

			while (scanner1.hasNextInt() && scanner2.hasNextInt()) {
				int version1 = scanner1.nextInt();
				int version2 = scanner2.nextInt();
				if (version1 < version2) {
					return -1;
				} else if (version1 > version2) {
					return 1;
				}
			}

			return scanner1.hasNextInt() ? 1 : 0;
		} finally {
			scanner1.close();
			scanner2.close();
		}
	}

	/**
	 * Determine whether the Spark version is supported.
	 * 
	 * @param sparkVersion
	 *            Spark version string (ie, "1.5.0").
	 * @return <code>true</code> if Spark version supported; otherwise <code>false</code>.
	 */
	public static boolean isSparkVersionSupported(String sparkVersion) {
		if (compareVersion(sparkVersion, NewMLContext.SYSTEMML_MINIMUM_SPARK_VERSION) < 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Determine whether the Spark version is supported for monitoring MLContext performance.
	 * 
	 * @param sparkVersion
	 * @return <code>true</code> if Spark version is supported for monitoring MLContext performance; otherwise
	 *         <code>false</code>.
	 */
	public static boolean isSparkVersionSupportedForMonitoringMLContextPerformance(String sparkVersion) {
		if (compareVersion(sparkVersion, NewMLContext.SYSTEMML_MINIMUM_SPARK_VERSION_MONITOR_MLCONTEXT_PERFORMANCE) < 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Set the SystemML Runtime Platform for MLContext. Currently Spark mode and hybrid Spark/Singlenode mode are
	 * supported.
	 * 
	 * @param sparkExecutionType
	 *            SparkExecutionType.SPARK_ONLY or SparkExecutionType.SPARK_OR_SINGLENODE
	 */
	// TODO: Move away from all these statics. makes multithreading difficult.
	public static void setRuntimePlatform(MLContextExecutionType sparkExecutionType) {
		if (sparkExecutionType == MLContextExecutionType.SPARK_ONLY) {
			DMLScript.rtplatform = RUNTIME_PLATFORM.SPARK;
		} else {
			DMLScript.rtplatform = RUNTIME_PLATFORM.HYBRID_SPARK;
		}
	}

	/**
	 * Check that the Spark version is supported. If it isn't supported, throw a MLContextException.
	 * 
	 * @param sc
	 *            SparkContext
	 * @throws MLContextException
	 *             If Spark version isn't supported.
	 */
	public static void verifySparkVersionSupported(SparkContext sc) {
		if (!MLContextUtil.isSparkVersionSupported(sc.version())) {
			throw new MLContextException("SystemML requires Spark " + NewMLContext.SYSTEMML_MINIMUM_SPARK_VERSION
					+ " or greater");
		}
	}

	/**
	 * Check that the Spark version is supported for monitoring MLContext performance. If it isn't supported, throw a
	 * MLContextException.
	 * 
	 * @param sc
	 *            SparkContext
	 * @throws MLContextException
	 *             If Spark version isn't supported for monitoring MLContext performance.
	 */
	public static void verifySparkVersionSupportedForMonitoringMLContextPerformance(SparkContext sc) {
		if (!MLContextUtil.isSparkVersionSupportedForMonitoringMLContextPerformance(sc.version())) {
			throw new MLContextException("SystemML requires Spark "
					+ NewMLContext.SYSTEMML_MINIMUM_SPARK_VERSION_MONITOR_MLCONTEXT_PERFORMANCE
					+ " or greater to monitor MLContext performance");
		}
	}

	/**
	 * Set default SystemML configuration properties.
	 */
	public static void setConfig() {
		ConfigurationManager.setConfig(new DMLConfig());
	}

	/**
	 * Set default SystemML configuration properties and specify additional config properties using
	 * additionalConfigProperties.
	 * 
	 * @param additionalConfigProperties
	 *            Additional config properties to add to SystemML config properties.
	 */
	public static void setConfig(Properties additionalConfigProperties) {
		setConfig(null, additionalConfigProperties);
	}

	/**
	 * Set SystemML configuration properties. If configFilePath is <code>null</code>, use default SystemML config
	 * properties. Additional config properties can be specified using additionalConfigProperties.
	 * 
	 * @param configFilePath
	 *            Path to config file. If <code>null</code>, use default SystemML config properties.
	 * @param additionalConfigProperties
	 *            Additional config properties to add to SystemML config properties.
	 */
	public static void setConfig(String configFilePath, Properties additionalConfigProperties) {
		DMLConfig config = null;
		if (configFilePath == null) {
			config = new DMLConfig();
		} else {
			try {
				config = new DMLConfig(configFilePath);
			} catch (FileNotFoundException e) {
				throw new MLContextException("Config file not found at: " + configFilePath);
			} catch (ParseException e) {
				throw new MLContextException("Error parsing config file at: " + configFilePath, e);
			}
		}

		if (additionalConfigProperties != null) {
			for (String propName : additionalConfigProperties.stringPropertyNames()) {
				try {
					config.setTextValue(propName, additionalConfigProperties.getProperty(propName));
				} catch (DMLRuntimeException e) {
					throw new MLContextException(e);
				}
			}
		}

		ConfigurationManager.setConfig(config);
	}

	/**
	 * Obtain a script string from a file.
	 * 
	 * @param scriptFilePath
	 *            The file path to the script file (either local file system of HDFS/GPFS).
	 * @return The script string.
	 * @throws MLContextException
	 *             If a problem occurs reading the script string from the file.
	 */
	public static String getScriptStringFromFile(String scriptFilePath) {
		if (scriptFilePath == null) {
			throw new MLContextException("Script file path is null");
		}
		try {
			if (scriptFilePath.startsWith("hdfs:") || scriptFilePath.startsWith("gpfs:")) { // from hdfs or gpfs
				if (!LocalFileUtils.validateExternalFilename(scriptFilePath, true)) {
					throw new MLContextException("Invalid (non-trustworthy) hdfs/gpfs filename: " + scriptFilePath);
				}
				FileSystem fs = FileSystem.get(ConfigurationManager.getCachedJobConf());
				Path path = new Path(scriptFilePath);
				FSDataInputStream fsdis = fs.open(path);
				String scriptString = IOUtils.toString(fsdis);
				return scriptString;
			} else {// from local file system
				if (!LocalFileUtils.validateExternalFilename(scriptFilePath, false)) {
					throw new MLContextException("Invalid (non-trustworthy) local filename: " + scriptFilePath);
				}
				File scriptFile = new File(scriptFilePath);
				String scriptString = FileUtils.readFileToString(scriptFile);
				return scriptString;
			}
		} catch (IllegalArgumentException e) {
			throw new MLContextException("Error trying to read script string from file: " + scriptFilePath, e);
		} catch (IOException e) {
			throw new MLContextException("Error trying to read script string from file: " + scriptFilePath, e);
		}
	}

	/**
	 * Obtain a script string from a file in the local file system. To obtain a script string from a file in HDFS,
	 * please use getScriptStringFromFile(String scriptFilePath).
	 * 
	 * @param file
	 *            The script file.
	 * @return The script string.
	 * @throws MLContextException
	 *             If a problem occurs reading the script string from the file.
	 */
	public static String getScriptStringFromFile(File file) {
		if (file == null) {
			throw new MLContextException("Script file is null");
		}
		String filePath = file.getPath();
		try {
			if (!LocalFileUtils.validateExternalFilename(filePath, false)) {
				throw new MLContextException("Invalid (non-trustworthy) local filename: " + filePath);
			}
			String scriptString = FileUtils.readFileToString(file);
			return scriptString;
		} catch (IllegalArgumentException e) {
			throw new MLContextException("Error trying to read script string from file: " + filePath, e);
		} catch (IOException e) {
			throw new MLContextException("Error trying to read script string from file: " + filePath, e);
		}
	}

	/**
	 * Obtain a script string from a URL.
	 * 
	 * @param scriptUrlPath
	 *            The URL path to the script file.
	 * @return The script string.
	 * @throws MLContextException
	 *             If a problem occurs reading the script string from the URL.
	 */
	public static String getScriptStringFromUrl(String scriptUrlPath) {
		if (scriptUrlPath == null) {
			throw new MLContextException("Script URL path is null");
		}
		try {
			URL url = new URL(scriptUrlPath);
			return getScriptStringFromUrl(url);
		} catch (MalformedURLException e) {
			throw new MLContextException("Error trying to read script string from URL path: " + scriptUrlPath, e);
		}
	}

	/**
	 * Obtain a script string from a URL.
	 * 
	 * @param url
	 *            The script URL.
	 * @return The script string.
	 * @throws MLContextException
	 *             If a problem occurs reading the script string from the URL.
	 */
	public static String getScriptStringFromUrl(URL url) {
		if (url == null) {
			throw new MLContextException("URL is null");
		}
		String urlString = url.toString();
		if ((!urlString.toLowerCase().startsWith("http:")) && (!urlString.toLowerCase().startsWith("https:"))) {
			throw new MLContextException("Currently only reading from http and https URLs is supported");
		}
		try {
			InputStream is = url.openStream();
			String scriptString = IOUtils.toString(is);
			return scriptString;
		} catch (IOException e) {
			throw new MLContextException("Error trying to read script string from URL: " + url, e);
		}
	}

	/**
	 * Obtain a script string from an InputStream.
	 * 
	 * @param inputStream
	 *            The InputStream from which to read the script string.
	 * @return The script string.
	 * @throws MLContextException
	 *             If a problem occurs reading the script string from the URL.
	 */
	public static String getScriptStringFromInputStream(InputStream inputStream) {
		if (inputStream == null) {
			throw new MLContextException("InputStream is null");
		}
		try {
			String scriptString = IOUtils.toString(inputStream);
			return scriptString;
		} catch (IOException e) {
			throw new MLContextException("Error trying to read script string from InputStream", e);
		}
	}

	/**
	 * Convenience method to generate a map of script input parameter key/value pairs.
	 * <p/>
	 * Example:<br/>
	 * <code>Map<String, Object> map = MLContextUtil.generateInputs("A", 1, "B", "two", "C", 3);</code> <br/>
	 * <br/>
	 * This is equivalent to:<br/>
	 * <code>Map<String, Object> map = new HashMap<String, Object>(){{
	 *     <br/>put("A", 1);
	 *     <br/>put("B", "two");
	 *     <br/>put("C", 3);
	 * <br/>}};</code>
	 * 
	 * @param objs
	 * @return
	 */
	public static Map<String, Object> generateInputs(Object... objs) {
		int len = objs.length;
		if ((len & 1) == 1) {
			throw new MLContextException("The number of arguments needs to be an even number");
		}
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		int i = 0;
		while (i < len) {
			map.put((String) objs[i++], objs[i++]);
		}
		return map;
	}

	public static void checkInputParameterValueTypes(Map<String, Object> inputs) {
		for (Entry<String, Object> entry : inputs.entrySet()) {
			checkInputParameterValueType(entry.getKey(), entry.getValue());
		}
	}

	public static void checkInputParameterValueType(String parameterName, Object parameterValue) {

		if (parameterName == null) {
			throw new MLContextException("No parameter name supplied");
		} else if (parameterValue == null) {
			throw new MLContextException("No parameter value supplied");
		}

		// @SuppressWarnings("rawtypes")
		// Class[] supportedBasicTypes = { Integer.class, Boolean.class, Double.class, String.class };
		// Class[] supportedComplexTypes = {};
		// Class[] supportedTypes = (Class[]) ArrayUtils.addAll(supportedBasicTypes, supportedComplexTypes);

		Object o = parameterValue;
		boolean supported = false;
		for (Class<?> clazz : ALL_SUPPORTED_TYPES) {
			if (o.getClass().equals(clazz)) {
				supported = true;
				break;
			} else if (clazz.isAssignableFrom(o.getClass())) {
				supported = true;
				break;
			}
		}
		if (!supported) {
			throw new MLContextException("Input parameter (\"" + parameterName + "\") value type not supported: "
					+ o.getClass());
		}
	}

	public static Map<String, Object> obtainBasicInputParameterMap(Map<String, Object> inputParameterMap) {
		Map<String, Object> basicInputParameterMap = new LinkedHashMap<String, Object>();

		if ((inputParameterMap == null) || (inputParameterMap.size() == 0)) {
			return basicInputParameterMap;
		}

		// @SuppressWarnings("rawtypes")
		// Class[] basicTypes = { Integer.class, Boolean.class, Double.class, String.class };

		for (Entry<String, Object> entry : inputParameterMap.entrySet()) {
			if (entry.getValue() == null) {
				throw new MLContextException("Input parameter value is null for: " + entry.getKey());
			}
			for (@SuppressWarnings("rawtypes")
			Class clazz : SUPPORTED_BASIC_TYPES) {
				if (entry.getValue().getClass().equals(clazz)) {
					basicInputParameterMap.put(entry.getKey(), entry.getValue());
				}
			}
		}

		return basicInputParameterMap;
	}

	/**
	 * Converts non-string basic input parameter values to strings to pass to the parser.
	 * 
	 * @param basicInputParameterMap
	 * @param scriptType
	 * @return
	 */
	// TODO: update method in parser to take a map rather than a hashmap
	public static HashMap<String, String> convertBasicInputParametersForParser(
			Map<String, Object> basicInputParameterMap, ScriptType scriptType) {
		if (basicInputParameterMap == null) {
			return null;
		}
		if (scriptType == null) {
			throw new MLContextException("ScriptType needs to be specified");
		}
		HashMap<String, String> convertedMap = new HashMap<String, String>();
		for (Entry<String, Object> entry : basicInputParameterMap.entrySet()) {
			String key = entry.getKey();
			String newKey = "$" + key;
			Object value = entry.getValue();
			if (value == null) {
				throw new MLContextException("Input parameter value is null for: " + entry.getKey());
			} else if (value instanceof Integer) {
				convertedMap.put(newKey, Integer.toString((Integer) value));
			} else if (value instanceof Boolean) {
				if (scriptType == ScriptType.DML) {
					convertedMap.put(newKey, String.valueOf((Boolean) value).toUpperCase());
				} else {
					convertedMap.put(newKey, String.valueOf((Boolean) value));
				}
			} else if (value instanceof Double) {
				convertedMap.put(newKey, Double.toString((Double) value));
			} else if (value instanceof String) {
				convertedMap.put(newKey, (String) value);
			}
		}
		return convertedMap;
	}

	public static MatrixObject convertComplexInputTypeIfNeeded(String parameterKey, Object parameterValue) {
		String key = parameterKey;
		Object value = parameterValue;
		if (key == null) {
			throw new MLContextException("Input parameter key is null");
		} else if (value == null) {
			throw new MLContextException("Input parameter value is null for: " + parameterKey);
		} else if (value instanceof JavaRDD<?>) {
			@SuppressWarnings("unchecked")
			JavaRDD<String> javaRDD = (JavaRDD<String>) value;
			MatrixObject matrixObject = javaRDDToMatrixObject(key, javaRDD);
			return matrixObject;
		} else if (value instanceof RDD<?>) {
			@SuppressWarnings("unchecked")
			RDD<String> rdd = (RDD<String>) value;
			JavaRDD<String> javaRDD = rdd.toJavaRDD();
			MatrixObject matrixObject = javaRDDToMatrixObject(key, javaRDD);
			return matrixObject;
		} else if (value instanceof DataFrame) {
			MatrixCharacteristics matrixCharacteristics = new MatrixCharacteristics();
			DataFrame dataFrame = (DataFrame) value;
			JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlock = dataFrameToBinaryBlock(dataFrame,
					matrixCharacteristics);
			MatrixObject matrixObject = binaryBlockToMatrixObject(key, binaryBlock, matrixCharacteristics);
			return matrixObject;
		} else if (value instanceof double[][]) {
			double[][] doubleMatrix = (double[][]) value;
			MatrixObject matrixObject = doubleMatrixToMatrixObject(key, doubleMatrix);
			return matrixObject;
		}
		return null;
	}

	public static MatrixObject doubleMatrixToMatrixObject(String parameterKey, double[][] doubleMatrix) {
		try {
			MatrixBlock matrixBlock = DataConverter.convertToMatrixBlock(doubleMatrix);

			DMLConfig conf = ConfigurationManager.getConfig();
			String scratch_space = conf.getTextValue(DMLConfig.SCRATCH_SPACE);
			int blocksize = conf.getIntValue(DMLConfig.DEFAULT_BLOCK_SIZE);

			MatrixCharacteristics mc = new MatrixCharacteristics(matrixBlock.getNumRows(), matrixBlock.getNumColumns(),
					blocksize, blocksize);
			MatrixFormatMetaData meta = new MatrixFormatMetaData(mc, OutputInfo.BinaryBlockOutputInfo,
					InputInfo.BinaryBlockInputInfo);
			MatrixObject matrixObject = new MatrixObject(ValueType.DOUBLE, scratch_space + "/" + parameterKey, meta);
			matrixObject.acquireModify(matrixBlock);
			matrixObject.release();
			return matrixObject;
		} catch (DMLRuntimeException e) {
			throw new MLContextException("Exception converting double[][] array to MatrixBlock");
		}
	}

	public static MatrixObject binaryBlockToMatrixObject(String parameterKey,
			JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlock, MatrixCharacteristics matrixCharacteristics) {
		// Bug in Spark is messing up blocks and indexes due to too eager reuse of data structures
		JavaPairRDD<MatrixIndexes, MatrixBlock> javaPairRdd = binaryBlock.mapToPair(new CopyBlockPairFunction());

		MatrixObject matrixObject = new MatrixObject(ValueType.DOUBLE, "temp", new MatrixFormatMetaData(
				matrixCharacteristics, OutputInfo.BinaryBlockOutputInfo, InputInfo.BinaryBlockInputInfo));
		matrixObject.setRDDHandle(new RDDObject(javaPairRdd, parameterKey));
		return matrixObject;
	}

	public static JavaPairRDD<MatrixIndexes, MatrixBlock> dataFrameToBinaryBlock(DataFrame dataFrame,
			MatrixCharacteristics matrixCharacteristics) {

		determineDataFrameDimensionsIfNeeded(dataFrame, matrixCharacteristics);

		JavaRDD<Row> javaRDD = dataFrame.javaRDD();
		JavaPairRDD<Row, Long> prepinput = javaRDD.zipWithIndex();
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = prepinput.mapPartitionsToPair(new DataFrameToBinaryBlockFunction(
				matrixCharacteristics, false));
		out = RDDAggregateUtils.mergeByKey(out);
		return out;
	}

	public static void determineDataFrameDimensionsIfNeeded(DataFrame dataFrame,
			MatrixCharacteristics matrixCharacteristics) {
		if (!matrixCharacteristics.dimsKnown(true)) {
			NewMLContext activeMLContext = MLContextProxy.getActiveMLContext();
			SparkContext sparkContext = activeMLContext.getSparkContext();
			@SuppressWarnings("resource")
			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

			Accumulator<Double> aNnz = javaSparkContext.accumulator(0L);
			JavaRDD<Row> javaRDD = dataFrame.javaRDD().map(new DataFrameAnalysisFunction(aNnz, false));
			long numRows = javaRDD.count();
			long numColumns = dataFrame.columns().length;
			long numNonZeros = UtilFunctions.toLong(aNnz.value());
			matrixCharacteristics.set(numRows, numColumns, matrixCharacteristics.getRowsPerBlock(),
					matrixCharacteristics.getColsPerBlock(), numNonZeros);
		}
	}

	public static MatrixObject javaRDDToMatrixObject(String parameterKey, JavaRDD<String> javaRDD) {
		JavaPairRDD<LongWritable, Text> javaPairRDD = javaRDD.mapToPair(new ConvertStringToLongTextPair());
		MatrixCharacteristics matrixCharacteristics = new MatrixCharacteristics(-1, -1, DMLTranslator.DMLBlockSize,
				DMLTranslator.DMLBlockSize, -1);
		MatrixObject matrixObject = new MatrixObject(ValueType.DOUBLE, null, new MatrixFormatMetaData(
				matrixCharacteristics, OutputInfo.CSVOutputInfo, InputInfo.CSVInputInfo));
		JavaPairRDD<LongWritable, Text> javaPairRDD2 = javaPairRDD.mapToPair(new CopyTextInputFunction());
		matrixObject.setRDDHandle(new RDDObject(javaPairRDD2, parameterKey));
		return matrixObject;
	}

	public static Map<String, MatrixObject> obtainComplexInputParameterMap(Map<String, Object> inputParameterMap) {
		Map<String, MatrixObject> complexInputParameterMap = new LinkedHashMap<String, MatrixObject>();

		if ((inputParameterMap == null) || (inputParameterMap.size() == 0)) {
			return complexInputParameterMap;
		}

		for (Entry<String, Object> entry : inputParameterMap.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null) {
				throw new MLContextException("Input parameter value is null for: " + key);
			}
			for (Class<?> clazz : SUPPORTED_COMPLEX_TYPES) {
				if (value.getClass().equals(clazz)) {
					MatrixObject matrixObject = convertComplexInputTypeIfNeeded(key, value);
					if (matrixObject != null) {
						complexInputParameterMap.put(key, matrixObject);
					} else {
						throw new MLContextException("Input parameter type for key \"" + key + "\" not recognized:"
								+ value.getClass());
					}
				} else if (clazz.isAssignableFrom(value.getClass())) {
					MatrixObject matrixObject = convertComplexInputTypeIfNeeded(key, value);
					if (matrixObject != null) {
						complexInputParameterMap.put(key, matrixObject);
					} else {
						throw new MLContextException("Input parameter type for key \"" + key + "\" not recognized:"
								+ value.getClass());
					}
				}
			}
		}

		return complexInputParameterMap;
	}
}
