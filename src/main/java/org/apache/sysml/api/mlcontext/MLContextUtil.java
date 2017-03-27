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
import java.io.FileNotFoundException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.sysml.api.mlcontext.ScriptMetadata.Example;
import org.apache.sysml.api.mlcontext.ScriptMetadata.Input;
import org.apache.sysml.api.mlcontext.ScriptMetadata.Option;
import org.apache.sysml.api.mlcontext.ScriptMetadata.Output;
import org.apache.sysml.conf.CompilerConfig;
import org.apache.sysml.conf.CompilerConfig.ConfigType;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.conf.DMLConfig;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.runtime.controlprogram.ForProgramBlock;
import org.apache.sysml.runtime.controlprogram.FunctionProgramBlock;
import org.apache.sysml.runtime.controlprogram.IfProgramBlock;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.Program;
import org.apache.sysml.runtime.controlprogram.ProgramBlock;
import org.apache.sysml.runtime.controlprogram.WhileProgramBlock;
import org.apache.sysml.runtime.controlprogram.caching.FrameObject;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.runtime.instructions.cp.BooleanObject;
import org.apache.sysml.runtime.instructions.cp.Data;
import org.apache.sysml.runtime.instructions.cp.DoubleObject;
import org.apache.sysml.runtime.instructions.cp.IntObject;
import org.apache.sysml.runtime.instructions.cp.StringObject;
import org.apache.sysml.runtime.instructions.cp.VariableCPInstruction;
import org.apache.sysml.runtime.matrix.data.FrameBlock;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import scala.Console;

/**
 * Utility class containing methods for working with the MLContext API.
 *
 */
public final class MLContextUtil {

	/**
	 * Basic data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] BASIC_DATA_TYPES = { Integer.class, Boolean.class, Double.class, String.class };

	/**
	 * Complex data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] COMPLEX_DATA_TYPES = { JavaRDD.class, RDD.class, Dataset.class, BinaryBlockMatrix.class,
			BinaryBlockFrame.class, Matrix.class, Frame.class, (new double[][] {}).getClass(), MatrixBlock.class,
			URL.class };

	/**
	 * All data types supported by the MLContext API
	 */
	@SuppressWarnings("rawtypes")
	public static final Class[] ALL_SUPPORTED_DATA_TYPES = (Class[]) ArrayUtils.addAll(BASIC_DATA_TYPES,
			COMPLEX_DATA_TYPES);

	/**
	 * Compare two version strings (ie, "1.4.0" and "1.4.1").
	 * 
	 * @param versionStr1
	 *            First version string.
	 * @param versionStr2
	 *            Second version string.
	 * @return If versionStr1 is less than versionStr2, return {@code -1}. If
	 *         versionStr1 equals versionStr2, return {@code 0}. If versionStr1
	 *         is greater than versionStr2, return {@code 1}.
	 * @throws MLContextException
	 *             if versionStr1 or versionStr2 is {@code null}
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
	 * @param minimumRecommendedSparkVersion
	 *            Minimum recommended Spark version string (ie, "2.1.0").
	 * @return {@code true} if Spark version supported; otherwise {@code false}.
	 */
	public static boolean isSparkVersionSupported(String sparkVersion, String minimumRecommendedSparkVersion) {
		return compareVersion(sparkVersion, minimumRecommendedSparkVersion) >= 0;
	}

	/**
	 * Check that the Spark version is supported. If it isn't supported, throw
	 * an MLContextException.
	 * 
	 * @param sc
	 *            SparkContext
	 * @throws MLContextException
	 *             thrown if Spark version isn't supported
	 */
	public static void verifySparkVersionSupported(SparkContext sc) {
		String minimumRecommendedSparkVersion = null;
		try {
			// If this is being called using the SystemML jar file,
			// ProjectInfo should be available.
			ProjectInfo projectInfo = ProjectInfo.getProjectInfo();
			minimumRecommendedSparkVersion = projectInfo.minimumRecommendedSparkVersion();
		} catch (MLContextException e) {
			try {
				// During development (such as in an IDE), there is no jar file
				// typically
				// built, so attempt to obtain the minimum recommended Spark
				// version from
				// the pom.xml file
				minimumRecommendedSparkVersion = getMinimumRecommendedSparkVersionFromPom();
			} catch (MLContextException e1) {
				throw new MLContextException(
						"Minimum recommended Spark version could not be determined from SystemML jar file manifest or pom.xml");
			}
		}
		String sparkVersion = sc.version();
		if (!MLContextUtil.isSparkVersionSupported(sparkVersion, minimumRecommendedSparkVersion)) {
			throw new MLContextException(
					"Spark " + sparkVersion + " or greater is recommended for this version of SystemML.");
		}
	}

	/**
	 * Obtain minimum recommended Spark version from the pom.xml file.
	 * 
	 * @return the minimum recommended Spark version from XML parsing of the pom
	 *         file (during development).
	 */
	static String getMinimumRecommendedSparkVersionFromPom() {
		return getUniquePomProperty("spark.version");
	}

	/**
	 * Obtain the text associated with an XML element from the pom.xml file. In
	 * this implementation, the element should be uniquely named, or results
	 * will be unpredicable.
	 * 
	 * @param property
	 *            unique property (element) from the pom.xml file
	 * @return the text value associated with the given property
	 */
	static String getUniquePomProperty(String property) {
		File f = new File("pom.xml");
		if (!f.exists()) {
			throw new MLContextException("pom.xml not found");
		}
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = dbf.newDocumentBuilder();
			Document document = builder.parse(f);

			NodeList nodes = document.getElementsByTagName(property);
			int length = nodes.getLength();
			if (length == 0) {
				throw new MLContextException("Property not found in pom.xml");
			}
			Node node = nodes.item(0);
			String value = node.getTextContent();
			return value;
		} catch (Exception e) {
			throw new MLContextException("MLContextException when reading property '" + property + "' from pom.xml", e);
		}
	}

	/**
	 * Set default SystemML configuration properties.
	 */
	public static void setDefaultConfig() {
		ConfigurationManager.setGlobalConfig(new DMLConfig());
	}

	/**
	 * Set SystemML configuration properties based on a configuration file.
	 * 
	 * @param configFilePath
	 *            Path to configuration file.
	 * @throws MLContextException
	 *             if configuration file was not found or a parse exception
	 *             occurred
	 */
	public static void setConfig(String configFilePath) {
		try {
			DMLConfig config = new DMLConfig(configFilePath);
			ConfigurationManager.setGlobalConfig(config);
		} catch (ParseException e) {
			throw new MLContextException("Parse Exception when setting config", e);
		} catch (FileNotFoundException e) {
			throw new MLContextException("File not found (" + configFilePath + ") when setting config", e);
		}
	}

	/**
	 * Set SystemML compiler configuration properties for MLContext
	 */
	public static void setCompilerConfig() {
		CompilerConfig compilerConfig = new CompilerConfig();
		compilerConfig.set(ConfigType.IGNORE_UNSPECIFIED_ARGS, true);
		compilerConfig.set(ConfigType.REJECT_READ_WRITE_UNKNOWNS, false);
		compilerConfig.set(ConfigType.ALLOW_CSE_PERSISTENT_READS, false);
		compilerConfig.set(ConfigType.MLCONTEXT, true);
		ConfigurationManager.setGlobalConfig(compilerConfig);
	}

	/**
	 * Verify that the types of input values are supported.
	 * 
	 * @param inputs
	 *            Map of String/Object pairs
	 * @throws MLContextException
	 *             if an input value type is not supported
	 */
	public static void checkInputValueTypes(Map<String, Object> inputs) {
		for (Entry<String, Object> entry : inputs.entrySet()) {
			checkInputValueType(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Verify that the type of input value is supported.
	 * 
	 * @param name
	 *            The name of the input
	 * @param value
	 *            The value of the input
	 * @throws MLContextException
	 *             if the input value type is not supported
	 */
	public static void checkInputValueType(String name, Object value) {

		if (name == null) {
			throw new MLContextException("No input name supplied");
		} else if (value == null) {
			throw new MLContextException("No input value supplied");
		}

		Object o = value;
		boolean supported = false;
		for (Class<?> clazz : ALL_SUPPORTED_DATA_TYPES) {
			if (o.getClass().equals(clazz)) {
				supported = true;
				break;
			} else if (clazz.isAssignableFrom(o.getClass())) {
				supported = true;
				break;
			}
		}
		if (!supported) {
			throw new MLContextException("Input name (\"" + name + "\") value type not supported: " + o.getClass());
		}
	}

	/**
	 * Verify that the type of input parameter value is supported.
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @throws MLContextException
	 *             if the input parameter value type is not supported
	 */
	public static void checkInputParameterType(String parameterName, Object parameterValue) {

		if (parameterName == null) {
			throw new MLContextException("No parameter name supplied");
		} else if (parameterValue == null) {
			throw new MLContextException("No parameter value supplied");
		} else if (!parameterName.startsWith("$")) {
			throw new MLContextException("Input parameter name must start with a $");
		}

		Object o = parameterValue;
		boolean supported = false;
		for (Class<?> clazz : BASIC_DATA_TYPES) {
			if (o.getClass().equals(clazz)) {
				supported = true;
				break;
			} else if (clazz.isAssignableFrom(o.getClass())) {
				supported = true;
				break;
			}
		}
		if (!supported) {
			throw new MLContextException(
					"Input parameter (\"" + parameterName + "\") value type not supported: " + o.getClass());
		}
	}

	/**
	 * Is the object one of the supported basic data types? (Integer, Boolean,
	 * Double, String)
	 * 
	 * @param object
	 *            the object type to be examined
	 * @return {@code true} if type is a basic data type; otherwise
	 *         {@code false}.
	 */
	public static boolean isBasicType(Object object) {
		for (Class<?> clazz : BASIC_DATA_TYPES) {
			if (object.getClass().equals(clazz)) {
				return true;
			} else if (clazz.isAssignableFrom(object.getClass())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Obtain the SystemML scalar value type string equivalent of an accepted
	 * basic type (Integer, Boolean, Double, String)
	 * 
	 * @param object
	 *            the object type to be examined
	 * @return a String representing the type as a SystemML scalar value type
	 */
	public static String getBasicTypeString(Object object) {
		if (!isBasicType(object)) {
			throw new MLContextException("Type (" + object.getClass() + ") not a recognized basic type");
		}
		Class<? extends Object> clazz = object.getClass();
		if (clazz.equals(Integer.class)) {
			return Statement.INT_VALUE_TYPE;
		} else if (clazz.equals(Boolean.class)) {
			return Statement.BOOLEAN_VALUE_TYPE;
		} else if (clazz.equals(Double.class)) {
			return Statement.DOUBLE_VALUE_TYPE;
		} else if (clazz.equals(String.class)) {
			return Statement.STRING_VALUE_TYPE;
		} else {
			return null;
		}
	}

	/**
	 * Is the object one of the supported complex data types? (JavaRDD, RDD,
	 * DataFrame, BinaryBlockMatrix, Matrix, double[][], MatrixBlock, URL)
	 * 
	 * @param object
	 *            the object type to be examined
	 * @return {@code true} if type is a complex data type; otherwise
	 *         {@code false}.
	 */
	public static boolean isComplexType(Object object) {
		for (Class<?> clazz : COMPLEX_DATA_TYPES) {
			if (object.getClass().equals(clazz)) {
				return true;
			} else if (clazz.isAssignableFrom(object.getClass())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Converts non-string basic input parameter values to strings to pass to
	 * the parser.
	 * 
	 * @param basicInputParameterMap
	 *            map of input parameters
	 * @param scriptType
	 *            {@code ScriptType.DML} or {@code ScriptType.PYDML}
	 * @return map of String/String name/value pairs
	 */
	public static Map<String, String> convertInputParametersForParser(Map<String, Object> basicInputParameterMap,
			ScriptType scriptType) {
		if (basicInputParameterMap == null) {
			return null;
		}
		if (scriptType == null) {
			throw new MLContextException("ScriptType needs to be specified");
		}
		Map<String, String> convertedMap = new HashMap<String, String>();
		for (Entry<String, Object> entry : basicInputParameterMap.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null) {
				throw new MLContextException("Input parameter value is null for: " + entry.getKey());
			} else if (value instanceof Integer) {
				convertedMap.put(key, Integer.toString((Integer) value));
			} else if (value instanceof Boolean) {
				if (scriptType == ScriptType.DML) {
					convertedMap.put(key, String.valueOf((Boolean) value).toUpperCase());
				} else {
					convertedMap.put(key, WordUtils.capitalize(String.valueOf((Boolean) value)));
				}
			} else if (value instanceof Double) {
				convertedMap.put(key, Double.toString((Double) value));
			} else if (value instanceof String) {
				convertedMap.put(key, (String) value);
			} else {
				throw new MLContextException("Incorrect type for input parameters");
			}
		}
		return convertedMap;
	}

	/**
	 * Convert input types to internal SystemML representations
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @return input in SystemML data representation
	 */
	public static Data convertInputType(String parameterName, Object parameterValue) {
		return convertInputType(parameterName, parameterValue, null);
	}

	/**
	 * Convert input types to internal SystemML representations
	 * 
	 * @param parameterName
	 *            The name of the input parameter
	 * @param parameterValue
	 *            The value of the input parameter
	 * @param metadata
	 *            matrix/frame metadata
	 * @return input in SystemML data representation
	 */
	public static Data convertInputType(String parameterName, Object parameterValue, Metadata metadata) {
		String name = parameterName;
		Object value = parameterValue;
		boolean hasMetadata = (metadata != null) ? true : false;
		boolean hasMatrixMetadata = hasMetadata && (metadata instanceof MatrixMetadata) ? true : false;
		boolean hasFrameMetadata = hasMetadata && (metadata instanceof FrameMetadata) ? true : false;
		if (name == null) {
			throw new MLContextException("Input parameter name is null");
		} else if (value == null) {
			throw new MLContextException("Input parameter value is null for: " + parameterName);
		} else if (value instanceof JavaRDD<?>) {
			@SuppressWarnings("unchecked")
			JavaRDD<String> javaRDD = (JavaRDD<String>) value;

			if (hasMatrixMetadata) {
				MatrixMetadata matrixMetadata = (MatrixMetadata) metadata;
				if (matrixMetadata.getMatrixFormat() == MatrixFormat.IJV) {
					return MLContextConversionUtil.javaRDDStringIJVToMatrixObject(name, javaRDD, matrixMetadata);
				} else {
					return MLContextConversionUtil.javaRDDStringCSVToMatrixObject(name, javaRDD, matrixMetadata);
				}
			} else if (hasFrameMetadata) {
				FrameMetadata frameMetadata = (FrameMetadata) metadata;
				if (frameMetadata.getFrameFormat() == FrameFormat.IJV) {
					return MLContextConversionUtil.javaRDDStringIJVToFrameObject(name, javaRDD, frameMetadata);
				} else {
					return MLContextConversionUtil.javaRDDStringCSVToFrameObject(name, javaRDD, frameMetadata);
				}
			} else if (!hasMetadata) {
				String firstLine = javaRDD.first();
				boolean isAllNumbers = isCSVLineAllNumbers(firstLine);
				if (isAllNumbers) {
					return MLContextConversionUtil.javaRDDStringCSVToMatrixObject(name, javaRDD);
				} else {
					return MLContextConversionUtil.javaRDDStringCSVToFrameObject(name, javaRDD);
				}
			}

		} else if (value instanceof RDD<?>) {
			@SuppressWarnings("unchecked")
			RDD<String> rdd = (RDD<String>) value;

			if (hasMatrixMetadata) {
				MatrixMetadata matrixMetadata = (MatrixMetadata) metadata;
				if (matrixMetadata.getMatrixFormat() == MatrixFormat.IJV) {
					return MLContextConversionUtil.rddStringIJVToMatrixObject(name, rdd, matrixMetadata);
				} else {
					return MLContextConversionUtil.rddStringCSVToMatrixObject(name, rdd, matrixMetadata);
				}
			} else if (hasFrameMetadata) {
				FrameMetadata frameMetadata = (FrameMetadata) metadata;
				if (frameMetadata.getFrameFormat() == FrameFormat.IJV) {
					return MLContextConversionUtil.rddStringIJVToFrameObject(name, rdd, frameMetadata);
				} else {
					return MLContextConversionUtil.rddStringCSVToFrameObject(name, rdd, frameMetadata);
				}
			} else if (!hasMetadata) {
				String firstLine = rdd.first();
				boolean isAllNumbers = isCSVLineAllNumbers(firstLine);
				if (isAllNumbers) {
					return MLContextConversionUtil.rddStringCSVToMatrixObject(name, rdd);
				} else {
					return MLContextConversionUtil.rddStringCSVToFrameObject(name, rdd);
				}
			}
		} else if (value instanceof MatrixBlock) {
			MatrixBlock matrixBlock = (MatrixBlock) value;
			return MLContextConversionUtil.matrixBlockToMatrixObject(name, matrixBlock, (MatrixMetadata) metadata);
		} else if (value instanceof FrameBlock) {
			FrameBlock frameBlock = (FrameBlock) value;
			return MLContextConversionUtil.frameBlockToFrameObject(name, frameBlock, (FrameMetadata) metadata);
		} else if (value instanceof Dataset<?>) {
			@SuppressWarnings("unchecked")
			Dataset<Row> dataFrame = (Dataset<Row>) value;

			dataFrame = MLUtils.convertVectorColumnsToML(dataFrame);
			if (hasMatrixMetadata) {
				return MLContextConversionUtil.dataFrameToMatrixObject(name, dataFrame, (MatrixMetadata) metadata);
			} else if (hasFrameMetadata) {
				return MLContextConversionUtil.dataFrameToFrameObject(name, dataFrame, (FrameMetadata) metadata);
			} else if (!hasMetadata) {
				boolean looksLikeMatrix = doesDataFrameLookLikeMatrix(dataFrame);
				if (looksLikeMatrix) {
					return MLContextConversionUtil.dataFrameToMatrixObject(name, dataFrame);
				} else {
					return MLContextConversionUtil.dataFrameToFrameObject(name, dataFrame);
				}
			}
		} else if (value instanceof BinaryBlockMatrix) {
			BinaryBlockMatrix binaryBlockMatrix = (BinaryBlockMatrix) value;
			if (metadata == null) {
				metadata = binaryBlockMatrix.getMatrixMetadata();
			}
			JavaPairRDD<MatrixIndexes, MatrixBlock> binaryBlocks = binaryBlockMatrix.getBinaryBlocks();
			return MLContextConversionUtil.binaryBlocksToMatrixObject(name, binaryBlocks, (MatrixMetadata) metadata);
		} else if (value instanceof BinaryBlockFrame) {
			BinaryBlockFrame binaryBlockFrame = (BinaryBlockFrame) value;
			if (metadata == null) {
				metadata = binaryBlockFrame.getFrameMetadata();
			}
			JavaPairRDD<Long, FrameBlock> binaryBlocks = binaryBlockFrame.getBinaryBlocks();
			return MLContextConversionUtil.binaryBlocksToFrameObject(name, binaryBlocks, (FrameMetadata) metadata);
		} else if (value instanceof Matrix) {
			Matrix matrix = (Matrix) value;
			return matrix.toMatrixObject();
		} else if (value instanceof Frame) {
			Frame frame = (Frame) value;
			return frame.toFrameObject();
		} else if (value instanceof double[][]) {
			double[][] doubleMatrix = (double[][]) value;
			return MLContextConversionUtil.doubleMatrixToMatrixObject(name, doubleMatrix, (MatrixMetadata) metadata);
		} else if (value instanceof URL) {
			URL url = (URL) value;
			return MLContextConversionUtil.urlToMatrixObject(name, url, (MatrixMetadata) metadata);
		} else if (value instanceof Integer) {
			return new IntObject((Integer) value);
		} else if (value instanceof Double) {
			return new DoubleObject((Double) value);
		} else if (value instanceof String) {
			return new StringObject((String) value);
		} else if (value instanceof Boolean) {
			return new BooleanObject((Boolean) value);
		}
		return null;
	}

	/**
	 * If no metadata is supplied for an RDD or JavaRDD, this method can be used
	 * to determine whether the data appears to be matrix (or a frame)
	 * 
	 * @param line
	 *            a line of the RDD
	 * @return {@code true} if all the csv-separated values are numbers,
	 *         {@code false} otherwise
	 */
	public static boolean isCSVLineAllNumbers(String line) {
		if (StringUtils.isBlank(line)) {
			return false;
		}
		String[] parts = line.split(",");
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i].trim();
			try {
				Double.parseDouble(part);
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Examine the DataFrame schema to determine whether the data appears to be
	 * a matrix.
	 * 
	 * @param df
	 *            the DataFrame
	 * @return {@code true} if the DataFrame appears to be a matrix,
	 *         {@code false} otherwise
	 */
	public static boolean doesDataFrameLookLikeMatrix(Dataset<Row> df) {
		StructType schema = df.schema();
		StructField[] fields = schema.fields();
		if (fields == null) {
			return true;
		}
		for (StructField field : fields) {
			DataType dataType = field.dataType();
			if ((dataType != DataTypes.DoubleType) && (dataType != DataTypes.IntegerType)
					&& (dataType != DataTypes.LongType) && (!(dataType instanceof org.apache.spark.ml.linalg.VectorUDT))
					&& (!(dataType instanceof org.apache.spark.mllib.linalg.VectorUDT))) {
				// uncomment if we support arrays of doubles for matrices
				// if (dataType instanceof ArrayType) {
				// ArrayType arrayType = (ArrayType) dataType;
				// if (arrayType.elementType() == DataTypes.DoubleType) {
				// continue;
				// }
				// }
				return false;
			}
		}
		return true;
	}

	/**
	 * Return a double-quoted string with inner single and double quotes
	 * escaped.
	 * 
	 * @param str
	 *            the original string
	 * @return double-quoted string with inner single and double quotes escaped
	 */
	public static String quotedString(String str) {
		if (str == null) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("\"");
		for (int i = 0; i < str.length(); i++) {
			char ch = str.charAt(i);
			if ((ch == '\'') || (ch == '"')) {
				if ((i > 0) && (str.charAt(i - 1) != '\\')) {
					sb.append('\\');
				} else if (i == 0) {
					sb.append('\\');
				}
			}
			sb.append(ch);
		}
		sb.append("\"");

		return sb.toString();
	}

	/**
	 * Display the keys and values in a Map
	 * 
	 * @param mapName
	 *            the name of the map
	 * @param map
	 *            Map of String keys and Object values
	 * @return the keys and values in the Map as a String
	 */
	public static String displayMap(String mapName, Map<String, Object> map) {
		StringBuilder sb = new StringBuilder();
		sb.append(mapName);
		sb.append(":\n");
		Set<String> keys = map.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");
				sb.append(key);
				sb.append(": ");
				sb.append(map.get(key));
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Display the values in a Set
	 * 
	 * @param setName
	 *            the name of the Set
	 * @param set
	 *            Set of String values
	 * @return the values in the Set as a String
	 */
	public static String displaySet(String setName, Set<String> set) {
		StringBuilder sb = new StringBuilder();
		sb.append(setName);
		sb.append(":\n");
		if (set.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String value : set) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");
				sb.append(value);
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Display the keys and values in the symbol table
	 * 
	 * @param name
	 *            the name of the symbol table
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @return the keys and values in the symbol table as a String
	 */
	public static String displaySymbolTable(String name, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		sb.append(displaySymbolTable(symbolTable));
		return sb.toString();
	}

	/**
	 * Display the keys and values in the symbol table
	 * 
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @return the keys and values in the symbol table as a String
	 */
	public static String displaySymbolTable(LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		Set<String> keys = symbolTable.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				sb.append("  [");
				sb.append(++count);
				sb.append("]");

				sb.append(" (");
				sb.append(determineOutputTypeAsString(symbolTable, key));
				sb.append(") ");

				sb.append(key);

				sb.append(": ");
				sb.append(symbolTable.get(key));
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Obtain a symbol table output type as a String
	 * 
	 * @param symbolTable
	 *            the symbol table
	 * @param outputName
	 *            the name of the output variable
	 * @return the symbol table output type for a variable as a String
	 */
	public static String determineOutputTypeAsString(LocalVariableMap symbolTable, String outputName) {
		Data data = symbolTable.get(outputName);
		if (data instanceof BooleanObject) {
			return "Boolean";
		} else if (data instanceof DoubleObject) {
			return "Double";
		} else if (data instanceof IntObject) {
			return "Long";
		} else if (data instanceof StringObject) {
			return "String";
		} else if (data instanceof MatrixObject) {
			return "Matrix";
		} else if (data instanceof FrameObject) {
			return "Frame";
		}
		return "Unknown";
	}

	/**
	 * Obtain a display of script inputs.
	 * 
	 * @param name
	 *            the title to display for the inputs
	 * @param map
	 *            the map of inputs
	 * @param symbolTable
	 *            the symbol table
	 * @return the script inputs represented as a String
	 */
	public static String displayInputs(String name, Map<String, Object> map, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		Set<String> keys = map.keySet();
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				Object object = map.get(key);
				@SuppressWarnings("rawtypes")
				Class clazz = object.getClass();
				String type = clazz.getSimpleName();
				if (object instanceof JavaRDD<?>) {
					type = "JavaRDD";
				} else if (object instanceof RDD<?>) {
					type = "RDD";
				}

				sb.append("  [");
				sb.append(++count);
				sb.append("]");

				sb.append(" (");
				sb.append(type);
				if (doesSymbolTableContainMatrixObject(symbolTable, key)) {
					sb.append(" as Matrix");
				} else if (doesSymbolTableContainFrameObject(symbolTable, key)) {
					sb.append(" as Frame");
				}
				sb.append(") ");

				sb.append(key);
				sb.append(": ");
				String str = object.toString();
				str = StringUtils.abbreviate(str, 100);
				sb.append(str);
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * Obtain a display of the script outputs.
	 * 
	 * @param name
	 *            the title to display for the outputs
	 * @param outputNames
	 *            the names of the output variables
	 * @param symbolTable
	 *            the symbol table
	 * @return the script outputs represented as a String
	 * 
	 */
	public static String displayOutputs(String name, Set<String> outputNames, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		sb.append(":\n");
		sb.append(displayOutputs(outputNames, symbolTable));
		return sb.toString();
	}

	/**
	 * Obtain a display of the script outputs.
	 * 
	 * @param outputNames
	 *            the names of the output variables
	 * @param symbolTable
	 *            the symbol table
	 * @return the script outputs represented as a String
	 * 
	 */
	public static String displayOutputs(Set<String> outputNames, LocalVariableMap symbolTable) {
		StringBuilder sb = new StringBuilder();
		if (outputNames.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String outputName : outputNames) {
				sb.append("  [");
				sb.append(++count);
				sb.append("] ");

				if (symbolTable.get(outputName) != null) {
					sb.append("(");
					sb.append(determineOutputTypeAsString(symbolTable, outputName));
					sb.append(") ");
				}

				sb.append(outputName);

				if (symbolTable.get(outputName) != null) {
					sb.append(": ");
					sb.append(symbolTable.get(outputName));
				}

				sb.append("\n");
			}
		}
		return sb.toString();
	}

	/**
	 * The SystemML welcome message
	 * 
	 * @return the SystemML welcome message
	 */
	public static String welcomeMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nWelcome to Apache SystemML!\n");
		return sb.toString();
	}

	/**
	 * Generate a String history entry for a script.
	 * 
	 * @param script
	 *            the script
	 * @param when
	 *            when the script was executed
	 * @return a script history entry as a String
	 */
	public static String createHistoryForScript(Script script, long when) {
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
		StringBuilder sb = new StringBuilder();
		sb.append("Script Name: " + script.getName() + "\n");
		sb.append("When: " + dateFormat.format(new Date(when)) + "\n");
		sb.append(script.displayInputs());
		sb.append(script.displayOutputs());
		sb.append(script.displaySymbolTable());
		return sb.toString();
	}

	/**
	 * Generate a String listing of the script execution history.
	 * 
	 * @param scriptHistory
	 *            the list of script history entries
	 * @return the listing of the script execution history as a String
	 */
	public static String displayScriptHistory(List<String> scriptHistory) {
		StringBuilder sb = new StringBuilder();
		sb.append("MLContext Script History:\n");
		if (scriptHistory.isEmpty()) {
			sb.append("None");
		}
		int i = 1;
		for (String history : scriptHistory) {
			sb.append("--------------------------------------------\n");
			sb.append("#" + (i++) + ":\n");
			sb.append(history);
		}
		return sb.toString();
	}

	/**
	 * Obtain the Spark Context
	 * 
	 * @param mlContext
	 *            the SystemML MLContext
	 * @return the Spark Context
	 */
	public static SparkContext getSparkContext(MLContext mlContext) {
		return mlContext.getSparkContext();
	}

	/**
	 * Obtain the Java Spark Context
	 * 
	 * @param mlContext
	 *            the SystemML MLContext
	 * @return the Java Spark Context
	 */
	public static JavaSparkContext getJavaSparkContext(MLContext mlContext) {
		return new JavaSparkContext(mlContext.getSparkContext());
	}

	/**
	 * Determine if the symbol table contains a FrameObject with the given
	 * variable name.
	 * 
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @param variableName
	 *            the variable name
	 * @return {@code true} if the variable in the symbol table is a
	 *         FrameObject, {@code false} otherwise.
	 */
	public static boolean doesSymbolTableContainFrameObject(LocalVariableMap symbolTable, String variableName) {
		return (symbolTable != null && symbolTable.keySet().contains(variableName)
				&& symbolTable.get(variableName) instanceof FrameObject);
	}

	/**
	 * Determine if the symbol table contains a MatrixObject with the given
	 * variable name.
	 * 
	 * @param symbolTable
	 *            the LocalVariableMap
	 * @param variableName
	 *            the variable name
	 * @return {@code true} if the variable in the symbol table is a
	 *         MatrixObject, {@code false} otherwise.
	 */
	public static boolean doesSymbolTableContainMatrixObject(LocalVariableMap symbolTable, String variableName) {
		return (symbolTable != null && symbolTable.keySet().contains(variableName)
				&& symbolTable.get(variableName) instanceof MatrixObject);
	}

	/**
	 * Delete the 'remove variable' instructions from a runtime program.
	 * 
	 * @param progam
	 *            runtime program
	 */
	public static void deleteRemoveVariableInstructions(Program progam) {
		Map<String, FunctionProgramBlock> fpbs = progam.getFunctionProgramBlocks();
		if (fpbs != null && !fpbs.isEmpty()) {
			for (Entry<String, FunctionProgramBlock> e : fpbs.entrySet()) {
				FunctionProgramBlock fpb = e.getValue();
				for (ProgramBlock pb : fpb.getChildBlocks()) {
					deleteRemoveVariableInstructions(pb);
				}
			}
		}

		for (ProgramBlock pb : progam.getProgramBlocks()) {
			deleteRemoveVariableInstructions(pb);
		}
	}

	/**
	 * Recursively traverse program block to delete 'remove variable'
	 * instructions.
	 * 
	 * @param pb
	 *            Program block
	 */
	private static void deleteRemoveVariableInstructions(ProgramBlock pb) {
		if (pb instanceof WhileProgramBlock) {
			WhileProgramBlock wpb = (WhileProgramBlock) pb;
			for (ProgramBlock pbc : wpb.getChildBlocks())
				deleteRemoveVariableInstructions(pbc);
		} else if (pb instanceof IfProgramBlock) {
			IfProgramBlock ipb = (IfProgramBlock) pb;
			for (ProgramBlock pbc : ipb.getChildBlocksIfBody())
				deleteRemoveVariableInstructions(pbc);
			for (ProgramBlock pbc : ipb.getChildBlocksElseBody())
				deleteRemoveVariableInstructions(pbc);
		} else if (pb instanceof ForProgramBlock) {
			ForProgramBlock fpb = (ForProgramBlock) pb;
			for (ProgramBlock pbc : fpb.getChildBlocks())
				deleteRemoveVariableInstructions(pbc);
		} else {
			ArrayList<Instruction> instructions = pb.getInstructions();
			deleteRemoveVariableInstructions(instructions);
		}
	}

	/**
	 * Delete 'remove variable' instructions.
	 * 
	 * @param instructions
	 *            list of instructions
	 */
	private static void deleteRemoveVariableInstructions(ArrayList<Instruction> instructions) {
		for (int i = 0; i < instructions.size(); i++) {
			Instruction linst = instructions.get(i);
			if (linst instanceof VariableCPInstruction && ((VariableCPInstruction) linst).isRemoveVariable()) {
				VariableCPInstruction varinst = (VariableCPInstruction) linst;
				instructions.remove(varinst);
				i--;
			}
		}
	}

	/**
	 * Determine if script contains metadata.
	 * 
	 * @param script
	 *            the DML or PyDML script object
	 * @return true if the script contains metadata, false otherwise
	 */
	public static boolean doesScriptContainMetadata(Script script) {
		if (script == null) {
			return false;
		}
		if (script.getScriptString() == null) {
			return false;
		}
		return (script.getScriptString().contains(ScriptMetadata.BEGIN_SCRIPT_METADATA)) ? true : false;
	}

	public static String displayScriptMetadata(Script script) {
		if (script == null) {
			return "";
		} else if (script.getScriptMetadata() == null) {
			return "No script metadata available";
		}
		ScriptMetadata sm = script.getScriptMetadata();
		StringBuilder sb = new StringBuilder();
		sb.append(consoleBold("Name:"));
		sb.append(" " + sm.name);
		sb.append("\n");
		sb.append(consoleBold("Description:"));
		sb.append(" " + sm.description);
		List<Input> inputs = sm.inputs;
		if ((inputs == null) || (inputs.size() == 0)) {
			sb.append("\n");
			sb.append(consoleBold("Inputs:"));
			sb.append("\n   None");
		} else {
			if (sm.hasRequiredInputs()) {
				sb.append("\n");
				sb.append(consoleBold("Required Inputs:"));
				for (Input input : inputs) {
					if (input.isRequired()) {
						sb.append("\n   ");

						if (doesMetadataInputExistInScript(input, script.getInputs())) {
							if (doColor()) {
								sb.append(Console.GREEN() + Console.BOLD() + "+ ");
							} else {
								sb.append("+ ");
							}
						} else {
							if (doColor()) {
								sb.append(Console.RED() + Console.BOLD() + "- ");
							} else {
								sb.append("- ");
							}
						}

						sb.append("(" + input.type + ") " + input.name);
						if (input.description != null) {
							sb.append(": " + input.description);
						}

						if (doesMetadataInputExistInScript(input, script.getInputs())) {
							if (doColor()) {
								sb.append(Console.RESET());
							}
						} else {
							if (doColor()) {
								sb.append(Console.RESET());
							}
						}
					}
				}
			}
			if (sm.hasOptionalInputs()) {
				sb.append("\n");
				sb.append(consoleBold("Optional Inputs:"));
				for (Input input : inputs) {
					if (input.isOptional()) {
						sb.append("\n   ");

						if (doesMetadataInputExistInScript(input, script.getInputs())) {
							if (doColor()) {
								sb.append(Console.GREEN() + Console.BOLD() + "+ ");
							} else {
								sb.append("+ ");
							}
						}

						sb.append("(" + input.type + ") " + input.name);
						if (input.defaultValue != null) {
							if ("String".equalsIgnoreCase(input.type)) {
								sb.append(": (Default:\"" + input.defaultValue + "\")");
							} else {
								sb.append(": (Default:" + input.defaultValue + ")");
							}
						}
						if (input.description != null) {
							sb.append(" " + input.description);
						}

						if (input.hasOptions()) {
							sb.append(", Options:[");
							for (int i = 0; i < input.options.size(); i++) {
								Option option = input.options.get(i);
								if (i > 0) {
									sb.append(", ");
								}
								if ("String".equalsIgnoreCase(input.type)) {
									sb.append("\"");
									sb.append(option.name);
									sb.append("\"");
								} else {
									sb.append(option.name);
								}
								if (option.description != null) {
									sb.append(" (");
									sb.append(option.description);
									sb.append(")");
								}
							}
							sb.append("]");
						}

						if (doesMetadataInputExistInScript(input, script.getInputs())) {
							if (doColor()) {
								sb.append(Console.RESET());
							}
						}
					}
				}
			}
		}
		List<Output> outputs = sm.outputs;
		if ((outputs == null) || (outputs.size() == 0)) {
			sb.append("\n");
			sb.append(consoleBold("Outputs:"));
			sb.append("\n   None");
		} else {
			sb.append("\n");
			sb.append(consoleBold("Outputs:"));
			for (Output output : outputs) {
				sb.append("\n   ");

				if (doesMetadataOutputExistInScript(output, script.getOutputVariables())) {
					if (doColor()) {
						sb.append(Console.GREEN() + Console.BOLD() + "+ ");
					} else {
						sb.append("+ ");
					}
				} else {
					if (doColor()) {
						sb.append(Console.RED() + Console.BOLD() + "- ");
					} else {
						sb.append("- ");
					}
				}

				sb.append("(" + output.type + ") " + output.name);
				if (output.description != null) {
					sb.append(": " + output.description);
				}

				if (doesMetadataOutputExistInScript(output, script.getOutputVariables())) {
					if (doColor()) {
						sb.append(Console.RESET());
					}
				} else {
					if (doColor()) {
						sb.append(Console.RESET());
					}
				}
			}
		}
		sb.append("\n");

		if (!doAllRequiredMetadataInputsAndOutputsExistInScript(script)) {
			sb.append(consoleBold("Note:"));
			if (doColor()) {
				sb.append("\n   " + Console.RED() + Console.BOLD()
						+ "'-' above indicates a Required Input or Output has not been supplied yet."
						+ Console.RESET());
			} else {
				sb.append("\n   '-' above indicates a Required Input or Output has not been supplied yet.");
			}
		} else {
			sb.append(consoleBold("Note:"));
			sb.append("\n   All Required Inputs and Outputs have been supplied.");
		}

		List<Example> examples = sm.examples;
		if ((examples != null) && (examples.size() > 0)) {
			sb.append("\n");
			sb.append(consoleBold("Examples:"));
			boolean first = true;
			for (Example example : examples) {
				if (!first) {
					sb.append("\n");
				}
				String ex = example.example;
				String lines[] = ex.split("\\r?\\n");
				for (String line : lines) {
					sb.append("\n   " + line);
				}
				first = false;
			}
		}

		return sb.toString();
	}

	private static boolean doAllRequiredMetadataInputsAndOutputsExistInScript(Script script) {
		ScriptMetadata sm = script.getScriptMetadata();
		for (Input input : sm.inputs) {
			if (input.isRequired()) {
				boolean mdInputExists = doesMetadataInputExistInScript(input, script.getInputs());
				if (!mdInputExists) {
					return false;
				}
			}
		}
		for (Output output : sm.outputs) {
			boolean mdOutputExists = doesMetadataOutputExistInScript(output, script.getOutputVariables());
			if (!mdOutputExists) {
				return false;
			}
		}
		return true;
	}

	private static boolean doesMetadataInputExistInScript(Input metadataInput, Map<String, Object> inputs) {
		String mdInputName = metadataInput.name;
		return (inputs.containsKey(mdInputName));
	}

	private static boolean doesMetadataOutputExistInScript(Output metadataOutput, Set<String> outputs) {
		String mdOutputName = metadataOutput.name;
		return (outputs.contains(mdOutputName));
	}

	private static String consoleBold(String topic) {
		if (doColor()) {
			return Console.BOLD() + topic + Console.RESET();
		} else {
			return topic;
		}
	}

	private static boolean doColor() {
		if (System.getProperty("scala.color") == null) {
			return false;
		} else {
			return true;
		}
	}
}
