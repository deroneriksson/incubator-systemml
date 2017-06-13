package org.apache.sysml.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.parsers.DocumentBuilder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.hops.DataOp;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
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
}
