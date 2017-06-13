package org.apache.sysml.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.hops.DataOp;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.LanguageException;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.util.MapReduceTool;
import org.apache.wink.json4j.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class HDFSUtils {

	public static BufferedReader hadoopPathToBufferedReader(String path) throws IOException {
		Path hdfsPath = new Path(path);
		FileSystem fs = IOUtilFunctions.getFileSystem(hdfsPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		return br;
	}

	public static Document hadoopPathToDocument(DocumentBuilder builder, String path) throws IOException, SAXException {
		Path hdfsPath = new Path(path);
		FileSystem fs = IOUtilFunctions.getFileSystem(hdfsPath);
		Document domTree = builder.parse(fs.open(hdfsPath));
		return domTree;
	}

	public static long hadoopPathToFileSize(String path) throws IOException {
		Path hdfsPath = new Path(path);
		long size = MapReduceTool.getFilesizeOnHDFS(hdfsPath);
		return size;
	}

	public static void tryReadMetaDataFileMatrixCharacteristics(DataOp dop) throws DMLRuntimeException {
		try {
			// get meta data filename
			String mtdname = DataExpression.getMTDFileName(dop.getFileName());
			Path path = new Path(mtdname);
			FileSystem fs = IOUtilFunctions.getFileSystem(mtdname);
			if (fs.exists(path)) {
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					JSONObject mtd = JSONHelper.parse(br);

					DataType dt = DataType.valueOf(String.valueOf(mtd.get(DataExpression.DATATYPEPARAM)).toUpperCase());
					dop.setDataType(dt);
					if (dt != DataType.FRAME)
						dop.setValueType(ValueType
								.valueOf(String.valueOf(mtd.get(DataExpression.VALUETYPEPARAM)).toUpperCase()));
					dop.setDim1((dt == DataType.MATRIX || dt == DataType.FRAME)
							? Long.parseLong(mtd.get(DataExpression.READROWPARAM).toString()) : 0);
					dop.setDim2((dt == DataType.MATRIX || dt == DataType.FRAME)
							? Long.parseLong(mtd.get(DataExpression.READCOLPARAM).toString()) : 0);
				} finally {
					IOUtilFunctions.closeSilently(br);
				}
			}
		} catch (Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	public static JSONObject readMetadataFile(String filename, boolean conditional, Expression expression)
			throws LanguageException {
		JSONObject retVal = null;
		boolean exists = MapReduceTool.existsFileOnHDFS(filename);
		boolean isDir = MapReduceTool.isDirectory(filename);

		// CASE: filename is a directory -- process as a directory
		if (exists && isDir) {
			retVal = new JSONObject();
			for (FileStatus stat : MapReduceTool.getDirectoryListing(filename)) {
				Path childPath = stat.getPath(); // gives directory name
				if (!childPath.getName().startsWith("part"))
					continue;
				BufferedReader br = null;
				try {
					FileSystem fs = IOUtilFunctions.getFileSystem(childPath);
					br = new BufferedReader(new InputStreamReader(fs.open(childPath)));
					JSONObject childObj = JSONHelper.parse(br);
					for (Object obj : childObj.entrySet()) {
						@SuppressWarnings("unchecked")
						Entry<Object, Object> e = (Entry<Object, Object>) obj;
						Object key = e.getKey();
						Object val = e.getValue();
						retVal.put(key, val);
					}
				} catch (Exception e) {
					expression.raiseValidateError("for MTD file in directory, error parting part of MTD file with path "
							+ childPath.toString() + ": " + e.getMessage(), conditional);
				} finally {
					IOUtilFunctions.closeSilently(br);
				}
			}
		}
		// CASE: filename points to a file
		else if (exists) {
			BufferedReader br = null;
			try {
				Path path = new Path(filename);
				FileSystem fs = IOUtilFunctions.getFileSystem(path);
				br = new BufferedReader(new InputStreamReader(fs.open(path)));
				retVal = new JSONObject(br);
			} catch (Exception e) {
				expression.raiseValidateError("error parsing MTD file with path " + filename + ": " + e.getMessage(),
						conditional);
			} finally {
				IOUtilFunctions.closeSilently(br);
			}
		}

		return retVal;
	}

	public static String[] readMatrixMarketFile(String filename, boolean conditional, Expression expression)
			throws LanguageException {
		String[] retVal = new String[2];
		retVal[0] = new String("");
		retVal[1] = new String("");
		boolean exists = false;

		try {
			Path path = new Path(filename);
			FileSystem fs = IOUtilFunctions.getFileSystem(path);
			exists = fs.exists(path);
			boolean getFileStatusIsDir = fs.getFileStatus(path).isDirectory();

			if (exists && getFileStatusIsDir) {
				expression.raiseValidateError("MatrixMarket files as directories not supported", conditional);
			} else if (exists) {
				BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
				try {
					retVal[0] = in.readLine();
					// skip all commented lines
					do {
						retVal[1] = in.readLine();
					} while (retVal[1].charAt(0) == '%');

					if (!retVal[0].startsWith("%%")) {
						expression.raiseValidateError("MatrixMarket files must begin with a header line.", conditional);
					}
				} finally {
					IOUtilFunctions.closeSilently(in);
				}
			} else {
				expression.raiseValidateError("Could not find the file: " + filename, conditional);
			}

		} catch (IOException e) {
			// LOG.error(this.printErrorLocation() + "Error reading MatrixMarket
			// file: " + filename );
			// throw new LanguageException(this.printErrorLocation() + "Error
			// reading MatrixMarket file: " + filename );
			throw new LanguageException(e);
		}

		return retVal;
	}

	public static boolean checkHasMatrixMarketFormat(String inputFileName, String mtdFileName, boolean conditional,
			Expression expression) throws LanguageException {
		// Check the MTD file exists. if there is an MTD file, return false.
		JSONObject mtdObject = readMetadataFile(mtdFileName, conditional, expression);
		if (mtdObject != null)
			return false;

		if (MapReduceTool.existsFileOnHDFS(inputFileName) && !MapReduceTool.isDirectory(inputFileName)) {
			BufferedReader in = null;
			try {
				Path path = new Path(inputFileName);
				FileSystem fs = IOUtilFunctions.getFileSystem(path);
				in = new BufferedReader(new InputStreamReader(fs.open(path)));
				String headerLine = new String("");
				if (in.ready())
					headerLine = in.readLine();
				return (headerLine != null && headerLine.startsWith("%%"));
			} catch (Exception ex) {
				throw new LanguageException("Failed to read mtd file.", ex);
			} finally {
				IOUtilFunctions.closeSilently(in);
			}
		}

		return false;
	}

}
