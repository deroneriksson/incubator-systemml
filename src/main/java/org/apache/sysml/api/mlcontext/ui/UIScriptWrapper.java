package org.apache.sysml.api.mlcontext.ui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sysml.api.mlcontext.MLContextUtil;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptExecutor;
import org.apache.sysml.api.mlcontext.ScriptType;
import org.apache.sysml.api.mlcontext.ui.UIUtil.UIParseTree;
import org.apache.sysml.api.mlcontext.ui.UIUtil.UIProgram;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.common.CustomErrorListener.ParseIssue;

public class UIScriptWrapper {

	protected Script script;
	protected UIParseTree uiParseTree;
	protected List<ParseIssue> parseIssues;
	protected UIProgram uiProgram;

	public UIScriptWrapper(Script script) {
		this.script = script;
	}

	public UIScriptWrapper(Script script, List<ParseIssue> parseIssues) {
		this.script = script;
		this.parseIssues = parseIssues;
	}

	public String getName() {
		return escapeHtmlAndBrReplace(script.getName());
	}

	public String getScriptType() {
		ScriptType scriptType = script.getScriptType();
		if (scriptType == null) { // this shouldn't happen
			return null;
		}
		return scriptType.toString();
	}

	public String getScriptString() {
		return escapeHtml(script.getScriptString());
	}

	public String getScriptExecutionString() {
		return escapeHtmlAndBrReplace(script.getScriptExecutionString());
	}

	public int getScriptStringSize() {
		String str = script.getScriptString();
		if (StringUtils.isEmpty(str)) {
			return 0;
		}
		String lines[] = str.split("\\r?\\n");
		return lines.length;
	}

	public int getScriptExecutionStringSize() {
		String str = script.getScriptExecutionString();
		if (StringUtils.isEmpty(str)) {
			return 0;
		}
		String lines[] = str.split("\\r?\\n");
		return lines.length;
	}

	public String getScriptExecutionStringWithLineNumbers() {
		String ses = script.getScriptExecutionString();
		if (StringUtils.isEmpty(ses)) {
			return ses;
		}
		String lines[] = ses.split("\\r?\\n");
		StringBuilder sb = new StringBuilder();
		int numDigits = String.valueOf(lines.length).length();
		for (int i = 1; i <= lines.length; i++) {
			String line = String.format("%" + numDigits + "d: %s", i, lines[i - 1]);
			sb.append(escapeHtml(line));
			sb.append("<br/>");
		}
		return sb.toString();
	}

	public String getScriptExecutionStringWithErrorHighlighting() {
		String ses = script.getScriptExecutionString();
		if (StringUtils.isEmpty(ses)) {
			return ses;
		}
		String lines[] = ses.split("\\r?\\n");
		StringBuilder sb = new StringBuilder();
		int numDigits = String.valueOf(lines.length).length();
		for (int i = 1; i <= lines.length; i++) {

			List<ParseIssue> relevantIssues = new ArrayList<ParseIssue>();
			for (ParseIssue pi : parseIssues) {
				int piLine = pi.getLine();
				if (piLine == i) {
					relevantIssues.add(pi);
				}
			}
			String line = lines[i - 1];
			if (relevantIssues.size() > 0) {
				List<Integer> charPositions = new ArrayList<Integer>();
				for (ParseIssue pi : relevantIssues) {
					int charPositionInLine = pi.getCharPositionInLine();
					charPositions.add(charPositionInLine);
				}
				Collections.sort(charPositions);
				StringBuilder sb2 = new StringBuilder();
				int last = 0;
				for (int j = 0; j < charPositions.size(); j++) {
					int charPos = charPositions.get(j);
					String s = line.substring(last, charPos);
					s = s + "#####BEGIN#####";
					s = s + line.substring(charPos, charPos + 1);
					s = s + "#####END#####";
					sb2.append(s);
					last = charPos;
				}
				String s2 = line.substring(last + 1);
				sb2.append(s2);
				line = sb2.toString();
			}
			line = escapeHtml(line);
			line = line.replace("#####BEGIN#####", "<span style=\"background-color: yellow;\">");
			line = line.replace("#####END#####", "</span>");
			line = String.format("%" + numDigits + "d: %s", i, line);
			if (relevantIssues.size() > 0) {
				line = "<span style=\"font-weight: bold; background-color: red;\">" + line + "</span>";
			}
			sb.append(line);
			sb.append("<br/>");
		}
		return sb.toString();
	}

	public String getDisplayInputParameters() {
		return escapeHtmlAndBrReplace(MLContextUtil.displayMap(script.getInputParameters()));
	}

	public String getDisplayInputs() {
//		return escapeHtmlAndBrReplace(MLContextUtil.displayInputs(script.getInputs(), script.getSymbolTable()));
		String displayInputs = UIUtil.displayInputs(script);
		return escapeHtmlAndBrReplace(displayInputs);
	}

	public String getDisplayInputVariables() {
		return escapeHtmlAndBrReplace(MLContextUtil.displaySet(script.getInputVariables()));
	}

	public String getDisplayOutputs() {
//		return escapeHtmlAndBrReplace(
//				MLContextUtil.displayOutputs(script.getOutputVariables(), script.getSymbolTable()));
		String displayOutputs = UIUtil.displayOutputs(script);
		return escapeHtmlAndBrReplace(displayOutputs);
	}

	public String getDisplayOutputVariables() {
		return escapeHtmlAndBrReplace(MLContextUtil.displaySet(script.getOutputVariables()));
	}

	public String getDisplaySymbolTable() {
//		return escapeHtmlAndBrReplace(MLContextUtil.displaySymbolTable(script.getSymbolTable()));
		String displaySymbolTable = UIUtil.displaySymbolTable(script);
		return escapeHtmlAndBrReplace(displaySymbolTable);
	}

	private String escapeHtmlAndBrReplace(String str) {
		if (str == null) {
			return null;
		}
		return escapeHtml(str).replace("\n", "<br/>");
	}

	private String escapeHtml(String str) {
		return StringEscapeUtils.escapeHtml4(str);
	}

	public Script getScript() {
		return script;
	}

	public void setScript(Script script) {
		this.script = script;
	}

	public boolean isExists() {
		return (script != null) ? true : false;
	}

	public String getParseTreeLisp() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}
		DMLProgram dmlProgram = se.getDmlProgram();
		ParseTree parseTree = dmlProgram.getParseTree();
		return parseTree.toStringTree();
	}

	public String getParseTreeD3() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}
		if (uiParseTree == null) {
			DMLProgram dmlProgram = se.getDmlProgram();
			ParseTree parseTree = dmlProgram.getParseTree();
			Parser parser = dmlProgram.getParser();
			uiParseTree = UIUtil.parseTreeToUiParseTree(parseTree, parser);
		}
		String d3ParseTreeData = uiParseTree.d3ParseTreeData();
		return d3ParseTreeData;
	}

	public String getParseTreeHeight() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}
		if (uiParseTree == null) {
			DMLProgram dmlProgram = se.getDmlProgram();
			ParseTree parseTree = dmlProgram.getParseTree();
			Parser parser = dmlProgram.getParser();
			uiParseTree = UIUtil.parseTreeToUiParseTree(parseTree, parser);
		}
		int widestDepthNumNodes = uiParseTree.widestDepthNumNodes;
		int height = widestDepthNumNodes * 60;
		if (height < 500) {
			height = 500;
		}
		return "" + height;
	}

	public String getParseTreeWidth() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}
		if (uiParseTree == null) {
			DMLProgram dmlProgram = se.getDmlProgram();
			ParseTree parseTree = dmlProgram.getParseTree();
			Parser parser = dmlProgram.getParser();
			uiParseTree = UIUtil.parseTreeToUiParseTree(parseTree, parser);
		}
		int depth = uiParseTree.depth;
		int width = depth * 165;
		if (width < 1000) {
			return "1000";
		}
		return "" + width;
	}

	public String getScriptStringRaw() {
		return script.getScriptString();
	}

	public String getProgramToString() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}
		DMLProgram program = se.getDmlProgram();
		return program.toString();
	}

	public String getProgramDisplay() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		String displayProgram = uiProgram.displayProgram();
		return displayProgram;
	}

	public String getProgramD3() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		String d3ProgramData = uiProgram.d3ProgramData();
		return d3ProgramData;
	}

	public String getProgramDisplayDetails() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		String displayProgram = uiProgram.displayProgramDetails();
		return displayProgram;
	}

	public String getProgramDetailsD3() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		String d3ProgramDataDetails = uiProgram.d3ProgramDataDetails();
		return d3ProgramDataDetails;
	}

	public String getProgramBasicDetailsD3() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		String d3ProgramDataBasicDetails = uiProgram.d3ProgramDataBasicDetails();
		return d3ProgramDataBasicDetails;
	}
	
	public String getProgramHeight() {
		ScriptExecutor se = script.getScriptExecutor();
		if (se == null) {
			return "No Script Executor found.";
		}

		if (uiProgram == null) {
			DMLProgram program = se.getDmlProgram();
			uiProgram = UIUtil.programToUiProgram(program);
		}
		int widestDepthNumNodes = uiProgram.widestDepthNumNodes;
		int height = widestDepthNumNodes * 60;
		if (height < 500) {
			height = 500;
		}
		return "" + height;
	}

}
