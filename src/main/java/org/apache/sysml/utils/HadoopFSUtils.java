package org.apache.sysml.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.io.IOUtilFunctions;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.CSVFileFormatProperties;
import org.apache.sysml.runtime.matrix.data.FileFormatProperties;
import org.apache.sysml.runtime.matrix.data.OutputInfo;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.OrderedJSONObject;

public class HadoopFSUtils {

	public static boolean isSameFileScheme(Path path1, Path path2) {
		if( path1 == null || path2 == null || path1.toUri() == null || path2.toUri() == null)
			return false;
		String scheme1 = path1.toUri().getScheme();
		String scheme2 = path2.toUri().getScheme();
		return (scheme1 == null && scheme2 == null)
			|| (scheme1 != null && scheme1.equals(scheme2));
	}
	
	public static void writeMetaDataFile(String mtdfile, ValueType vt, ValueType[] schema, DataType dt, MatrixCharacteristics mc, 
			OutputInfo outinfo, FileFormatProperties formatProperties) 
		throws IOException 
	{
		Path path = new Path(mtdfile);
		FileSystem fs = IOUtilFunctions.getFileSystem(path);
		try( BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path,true))) ) {
			String mtd = metaDataToString(vt, schema, dt, mc, outinfo, formatProperties);
			br.write(mtd);
		} catch (Exception e) {
			throw new IOException("Error creating and writing metadata JSON file", e);
		}
	}
	
	public static String metaDataToString(ValueType vt, ValueType[] schema, DataType dt, MatrixCharacteristics mc,
			OutputInfo outinfo, FileFormatProperties formatProperties) throws JSONException, DMLRuntimeException
	{
		OrderedJSONObject mtd = new OrderedJSONObject(); // maintain order in output file

		//handle data type and value types (incl schema for frames)
		mtd.put(DataExpression.DATATYPEPARAM, dt.toString().toLowerCase());
		if (schema == null) {
			mtd.put(DataExpression.VALUETYPEPARAM, vt.toString().toLowerCase());
		}	
		else {
			StringBuffer schemaSB = new StringBuffer();
			for(int i=0; i < schema.length; i++) {
				if( schema[i] == ValueType.UNKNOWN )
					schemaSB.append("*");
				else
					schemaSB.append(schema[i].toString());
				schemaSB.append(DataExpression.DEFAULT_DELIM_DELIMITER);
			}
			mtd.put(DataExpression.SCHEMAPARAM, schemaSB.toString());
		}
		
		//handle output dimensions
		if( !dt.isScalar() ) {
			mtd.put(DataExpression.READROWPARAM, mc.getRows());
			mtd.put(DataExpression.READCOLPARAM, mc.getCols());
			// handle output nnz and binary block configuration
			if( dt.isMatrix() ) {
				if (outinfo == OutputInfo.BinaryBlockOutputInfo ) {
					mtd.put(DataExpression.ROWBLOCKCOUNTPARAM, mc.getRowsPerBlock());
					mtd.put(DataExpression.COLUMNBLOCKCOUNTPARAM, mc.getColsPerBlock());
				}
				mtd.put(DataExpression.READNUMNONZEROPARAM, mc.getNonZeros());
			}
		}
			
		//handle format type and additional arguments	
		mtd.put(DataExpression.FORMAT_TYPE, OutputInfo.outputInfoToStringExternal(outinfo));
		if (outinfo == OutputInfo.CSVOutputInfo) {
			CSVFileFormatProperties csvProperties = (formatProperties==null) ?
				new CSVFileFormatProperties() : (CSVFileFormatProperties)formatProperties;
			mtd.put(DataExpression.DELIM_HAS_HEADER_ROW, csvProperties.hasHeader());
			mtd.put(DataExpression.DELIM_DELIMITER, csvProperties.getDelim());
		}

		if (formatProperties != null) {
			String description = formatProperties.getDescription();
			if (StringUtils.isNotEmpty(description)) {
				String jsonDescription = StringEscapeUtils.escapeJson(description);
				mtd.put(DataExpression.DESCRIPTIONPARAM, jsonDescription);
			}
		}

		String userName = System.getProperty("user.name");
		if (StringUtils.isNotEmpty(userName)) {
			mtd.put(DataExpression.AUTHORPARAM, userName);
		} else {
			mtd.put(DataExpression.AUTHORPARAM, "SystemML");
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
		mtd.put(DataExpression.CREATEDPARAM, sdf.format(new Date()));

		return mtd.toString(4); // indent with 4 spaces	
	}
}
