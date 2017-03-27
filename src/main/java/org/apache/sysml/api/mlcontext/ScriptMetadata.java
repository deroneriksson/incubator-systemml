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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

/**
 * A ScriptMetadata object encapsulates metadata about a DML or PYDML script.
 *
 */
public class ScriptMetadata {

	/**
	 * Logger for ScriptMetadata
	 */
	public static Logger log = Logger.getLogger(ScriptMetadata.class);

	public static final String BEGIN_SCRIPT_METADATA = "BEGIN-SCRIPT-METADATA";
	public static final String END_SCRIPT_METADATA = "END-SCRIPT-METADATA";

	String name;
	String description;
	List<Input> inputs = new ArrayList<Input>();
	List<Output> outputs = new ArrayList<Output>();
	List<Example> examples = new ArrayList<Example>();

	String metaString;
	boolean success = false;

	public class Input {
		String name;
		String type;
		String description;
		String defaultValue;
		List<Option> options = new ArrayList<Option>();

		@SuppressWarnings("unchecked")
		public Input(JSONObject jInput) {
			name = getOptionalJsonString(jInput, "name");
			type = getOptionalJsonString(jInput, "type");
			description = getOptionalJsonString(jInput, "description");
			defaultValue = getOptionalJsonString(jInput, "default");

			JSONArray jOptions = getOptionalJsonArray(jInput, "options");
			if (jOptions != null) {
				jOptions.forEach(jOption -> options.add(new Option((JSONObject) jOption)));
			}
		}

		public boolean isRequired() {
			return (defaultValue == null) ? true : false;
		}

		public boolean isOptional() {
			return !isRequired();
		}

		public boolean hasOptions() {
			return (options.size() > 0);
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
		}
	}

	public class Option {
		String name;
		String description;

		public Option(JSONObject jOption) {
			name = getOptionalJsonString(jOption, "name");
			description = getOptionalJsonString(jOption, "description");
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}

	public class Output {
		String name;
		String type;
		String description;

		public Output(JSONObject jOutput) {
			name = getOptionalJsonString(jOutput, "name");
			type = getOptionalJsonString(jOutput, "type");
			description = getOptionalJsonString(jOutput, "description");
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
		}
	}

	public class Example {
		String example;

		public Example(JSONObject jExample) {
			example = getOptionalJsonString(jExample, "example");
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
		}
	}

	@SuppressWarnings("unchecked")
	public ScriptMetadata(Script script) {
		String scriptString = script.getScriptString();
		if (!scriptString.contains(BEGIN_SCRIPT_METADATA)) {
			return;
		}
		if (!scriptString.contains(END_SCRIPT_METADATA)) {
			return;
		}
		if (scriptString.indexOf(BEGIN_SCRIPT_METADATA) > scriptString.indexOf(END_SCRIPT_METADATA)) {
			return;
		}
		String lines[] = scriptString.split("\\r?\\n");
		StringBuilder sb = new StringBuilder();
		boolean isMetadata = false;
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			if (line.contains(BEGIN_SCRIPT_METADATA)) {
				isMetadata = true;
				continue;
			}
			if (isMetadata) {
				if (line.contains(END_SCRIPT_METADATA)) {
					break;
				}
				sb.append(line);
				sb.append("\n");
			}
		}
		metaString = sb.toString();
		JSONObject json = null;
		try {
			json = new JSONObject(metaString);
			name = getOptionalJsonString(json, "name");
			description = getOptionalJsonString(json, "description");

			JSONArray jInputs = getOptionalJsonArray(json, "inputs");
			if (jInputs != null) {
				jInputs.forEach(jInput -> inputs.add(new Input((JSONObject) jInput)));
			}

			JSONArray jOutputs = getOptionalJsonArray(json, "outputs");
			if (jOutputs != null) {
				jOutputs.forEach(jOutput -> outputs.add(new Output((JSONObject) jOutput)));
			}

			JSONArray jExamples = getOptionalJsonArray(json, "examples");
			if (jExamples != null) {
				jExamples.forEach(jExample -> examples.add(new Example((JSONObject) jExample)));
			}
		} catch (JSONException e) {
			success = false;
			if (metaString != null) {
				List<String> meta = Arrays.asList(metaString.split("\\r?\\n"));
				int dig = String.valueOf(meta.size()).length();
				StringBuilder sb1 = new StringBuilder();
				AtomicInteger count = new AtomicInteger(0);
				meta.forEach(l -> sb1.append(String.format("%" + dig + "d: %s\n", count.incrementAndGet(), l)));
				log.warn("Exception parsing script metadata to JSON:\n" + e.getMessage() + "\n" + sb1.toString());
			} else {
				log.warn("Exception parsing script metadata to JSON", e);
			}
			return;
		}
		success = true;
	}

	private String getOptionalJsonString(JSONObject json, String key) {
		try {
			return json.getString(key);
		} catch (JSONException e) {
		}
		return null;
	}

	private JSONArray getOptionalJsonArray(JSONObject json, String key) {
		try {
			return json.getJSONArray(key);
		} catch (JSONException e) {
		}
		return null;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}

	public boolean hasRequiredInputs() {
		return inputs.stream().filter(input -> input.isRequired()).findFirst().isPresent();
	}

	public boolean hasOptionalInputs() {
		return inputs.stream().filter(input -> input.isOptional()).findFirst().isPresent();
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public List<Input> getInputs() {
		return inputs;
	}

	public List<Output> getOutputs() {
		return outputs;
	}

	public List<Example> getExamples() {
		return examples;
	}

	public String getMetaString() {
		return metaString;
	}

	public boolean isSuccess() {
		return success;
	}
}
