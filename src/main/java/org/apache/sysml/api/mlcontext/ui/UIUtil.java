package org.apache.sysml.api.mlcontext.ui;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.sysml.api.mlcontext.MLContextUtil;
import org.apache.sysml.api.mlcontext.Matrix;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptExecutor;
import org.apache.sysml.parser.AssignmentStatement;
import org.apache.sysml.parser.BinaryExpression;
import org.apache.sysml.parser.BooleanExpression;
import org.apache.sysml.parser.BuiltinFunctionExpression;
import org.apache.sysml.parser.ConditionalPredicate;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.DataExpression;
import org.apache.sysml.parser.DataIdentifier;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.Expression.BinaryOp;
import org.apache.sysml.parser.Expression.BooleanOp;
import org.apache.sysml.parser.Expression.BuiltinFunctionOp;
import org.apache.sysml.parser.Expression.DataOp;
import org.apache.sysml.parser.Expression.DataType;
import org.apache.sysml.parser.Expression.FunctCallOp;
import org.apache.sysml.parser.Expression.ParameterizedBuiltinFunctionOp;
import org.apache.sysml.parser.Expression.RelationalOp;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.ForStatement;
import org.apache.sysml.parser.ForStatementBlock;
import org.apache.sysml.parser.FunctionCallIdentifier;
import org.apache.sysml.parser.FunctionStatement;
import org.apache.sysml.parser.FunctionStatementBlock;
import org.apache.sysml.parser.IfStatement;
import org.apache.sysml.parser.IfStatementBlock;
import org.apache.sysml.parser.IterablePredicate;
import org.apache.sysml.parser.OutputStatement;
import org.apache.sysml.parser.ParForStatement;
import org.apache.sysml.parser.ParForStatementBlock;
import org.apache.sysml.parser.ParameterExpression;
import org.apache.sysml.parser.ParameterizedBuiltinFunctionExpression;
import org.apache.sysml.parser.PrintStatement;
import org.apache.sysml.parser.PrintStatement.PRINTTYPE;
import org.apache.sysml.parser.RelationalExpression;
import org.apache.sysml.parser.Statement;
import org.apache.sysml.parser.StatementBlock;
import org.apache.sysml.parser.StringIdentifier;
import org.apache.sysml.parser.WhileStatement;
import org.apache.sysml.parser.WhileStatementBlock;
import org.apache.sysml.parser.common.CustomErrorListener.ParseIssue;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.cp.Data;

public class UIUtil {

	public static final String JS_PATH = "org/apache/sysml/api/mlcontext/ui/js/";
	public static final String JS_EXTENSION = ".js";
	public static final String JS_START = "js/";
	public static final String CSS_PATH = "org/apache/sysml/api/mlcontext/ui/css/";
	public static final String CSS_START = "css/";
	public static final String IMG_PATH = "org/apache/sysml/api/mlcontext/ui/img/";
	public static final String IMG_START = "img/";

	public static String fullPathToJs(String jsName) {
		return JS_PATH + jsName + JS_EXTENSION;
	}

	public static InputStream getJsAsInputStream(String jsName) {
		InputStream is = UIUtil.class.getClassLoader().getResourceAsStream(fullPathToJs(jsName));
		return is;
	}

	public static InputStream getCssAsInputStream(String cssFile) {
		InputStream is = UIUtil.class.getClassLoader().getResourceAsStream(fullPathToCss(cssFile));
		return is;
	}

	public static String fullPathToCss(String cssFile) {
		return CSS_PATH + cssFile;
	}

	public static InputStream getImgAsInputStream(String imgFile) {
		InputStream is = UIUtil.class.getClassLoader().getResourceAsStream(fullPathToImg(imgFile));
		return is;
	}

	public static String fullPathToImg(String imgFile) {
		return IMG_PATH + imgFile;
	}

	public static class UIParseTreeNode {
		int id;
		int depth;
		String value;
		List<UIParseTreeNode> children = new ArrayList<UIParseTreeNode>();
		UIParseTreeNode parent = null;

		public UIParseTreeNode(int id, int depth, String value, UIParseTreeNode parent) {
			this.id = id;
			this.depth = depth;
			this.value = value;
			this.parent = parent;
			if (parent != null) {
				parent.children.add(this);
			}
		}
	}

	public static class TreeStats {
		int numNodes = 0;
		int widestDepth = 0;
		int widestDepthNumNodes = 0;
		List<Integer> depthCounts = new ArrayList<Integer>();

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}

	public static class UIParseTree {
		UIParseTreeNode root = null;

		int numNodes = 0;
		int widestDepth = 0;
		int widestDepthNumNodes = 0;
		List<Integer> depthCounts = null;
		int depth = 0;

		public void determineTreeStats() {
			TreeStats treeStats = new TreeStats();
			doTreeStats(treeStats, root);

			for (int i = 0; i < treeStats.depthCounts.size(); i++) {
				int depthCount = treeStats.depthCounts.get(i);
				if (depthCount > treeStats.widestDepthNumNodes) {
					treeStats.widestDepthNumNodes = depthCount;
					treeStats.widestDepth = i;
				}
			}

			numNodes = treeStats.numNodes;
			widestDepth = treeStats.widestDepth;
			widestDepthNumNodes = treeStats.widestDepthNumNodes;
			depthCounts = new ArrayList<Integer>(treeStats.depthCounts);
			depth = depthCounts.size();
			// System.out.println("TREE STATS:" + treeStats);
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

		private void doTreeStats(TreeStats treeStats, UIParseTreeNode node) {
			treeStats.numNodes++;
			if (treeStats.depthCounts.size() <= node.depth) {
				treeStats.depthCounts.add(1);
			} else {
				int depthCount = treeStats.depthCounts.get(node.depth);
				treeStats.depthCounts.set(node.depth, ++depthCount);
			}
			for (UIParseTreeNode child : node.children) {
				doTreeStats(treeStats, child);
			}
		}

		// for debugging
		public String displayTreeText() {
			StringBuilder sb = new StringBuilder();
			displayNodeValue(sb, root);
			return sb.toString();
		}

		private void displayNodeValue(StringBuilder sb, UIParseTreeNode node) {
			for (int i = 0; i < node.depth; i++) {
				sb.append("  ");
			}
			sb.append("VALUE:" + node.value + ", ID:" + node.id + ", DEPTH:" + node.depth + "\n");
			for (UIParseTreeNode child : node.children) {
				displayNodeValue(sb, child);
			}
		}

		public String d3ParseTreeData() {
			StringBuilder sb = new StringBuilder();
			d3NodeData(sb, root);
			if (sb.length() > 0) {
				sb.replace(sb.length() - 1, sb.length(), "");
			}
			return sb.toString();
		}

		private void d3NodeData(StringBuilder sb, UIParseTreeNode node) {
			String spaces = "";
			for (int i = 0; i < node.depth; i++) {
				spaces = spaces + "  ";
			}
			sb.append(spaces + "{");
			sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + "\"");
			if (node.children.size() > 0) {
				sb.append(",\n");
				sb.append(spaces + "  \"children\": [\n");
			}
			for (int i = 0; i < node.children.size(); i++) {
				UIParseTreeNode child = node.children.get(i);
				d3NodeData(sb, child);
				if (i < node.children.size() - 1) {
					sb.replace(sb.length() - 1, sb.length(), ",\n");
				}
			}
			if (node.children.size() > 0) {
				sb.append(spaces + "  ]\n");
				sb.append(spaces + "}\n");
			} else {
				sb.append(" }\n");
			}

		}
	}

	public static UIParseTree parseTreeToUiParseTree(ParseTree tree, Parser parser) {
		final String[] ruleNames = parser.getRuleNames();
		final UIParseTree uiParseTree = new UIParseTree();
		ParseTreeWalker walker = new ParseTreeWalker();

		ParseTreeListener ptl = new ParseTreeListener() {
			int id = 0;
			int depth = 0;
			UIParseTreeNode parent = null;

			@Override
			public void enterEveryRule(ParserRuleContext context) {
				UIParseTreeNode node = new UIParseTreeNode(id, depth, ruleNames[context.getRuleIndex()], parent);
				if (id == 0) {
					uiParseTree.root = node;
				}
				parent = node;
				id++;
				depth++;
			}

			@Override
			public void exitEveryRule(ParserRuleContext context) {
				parent = parent.parent;
				depth--;
			}

			@Override
			public void visitErrorNode(ErrorNode context) {
			}

			@Override
			public void visitTerminal(TerminalNode context) {
				new UIParseTreeNode(id, depth, context.getText(), parent);
				id++;
			}
		};
		walker.walk(ptl, tree);

		uiParseTree.determineTreeStats();
		return uiParseTree;
	}

	/**
	 * Generate a UI message displaying information about the parse issues that
	 * occurred.
	 * 
	 * @param scriptString
	 *            The DML or PYDML script string.
	 * @param parseIssues
	 *            The list of parse issues.
	 * @return HTML string representing the list of parse issues.
	 */
	public static String generateUIParseIssuesMessage(String scriptString, List<ParseIssue> parseIssues) {
		if (scriptString == null) {
			return "No script string available.";
		}
		if (parseIssues == null) {
			return "No parse issues available.";
		}

		String[] scriptLines = scriptString.split("\\n");
		StringBuilder sb = new StringBuilder();
		sb.append("<span style=\"font-weight: bold; color: red; font-style: italic;\">");
		if (parseIssues.size() == 1) {
			sb.append("The following parse issue was encountered:");
		} else {
			sb.append("The following " + parseIssues.size() + " parse issues were encountered:");
		}
		sb.append("</span><br/>");
		sb.append("<ol style=\"margin-left: 20px;\">");
		for (ParseIssue parseIssue : parseIssues) {
			sb.append("<li style=\"font-style: normal; color: red;\">");

			int issueLineNum = parseIssue.getLine();
			boolean displayScriptLine = false;
			String scriptLine = null;
			if ((issueLineNum > 0) && (issueLineNum <= scriptLines.length)) {
				displayScriptLine = true;
				scriptLine = scriptLines[issueLineNum - 1];
			}

			String name = parseIssue.getFileName();
			if (name != null) {
				sb.append(name);
				sb.append(" ");
			}
			sb.append("<span style=\"font-weight: bold\">[line ");
			sb.append(issueLineNum);
			sb.append(":");
			sb.append(parseIssue.getCharPositionInLine());
			sb.append("]</span> [");
			sb.append(parseIssue.getParseIssueType().getText());
			sb.append("]");
			if (displayScriptLine) {
				sb.append(" -&gt; ");
				sb.append(
						"<span style=\"font-size: 14px; font-family: 'Courier New', Courier, monospace; font-weight: bold;\">");
				sb.append(scriptLine);
				sb.append("</span>");
			}
			sb.append("<br/>   ");
			sb.append(StringEscapeUtils.escapeHtml4(parseIssue.getMessage()));
			sb.append("<br/>");
			sb.append("</li>");
		}
		sb.append("</ol>");
		return sb.toString();
	}

	public static UIProgram programToUiProgram(DMLProgram program) {
		UIProgram uiProgram = new UIProgram();

		UIProgramNode root = uiProgram.addNode("Program", null, new String());

		UIProgramNode namespaces = uiProgram.addNode("Namespaces", root, new String());
		for (String nsKey : program.getNamespaces().keySet()) {
			DMLProgram nsProgram = program.getNamespaces().get(nsKey);
			UIProgramNode namespace = uiProgram.addNode(nsKey, namespaces, nsProgram);
			if (!nsProgram.getFunctionBlocks().isEmpty()) {
				UIProgramNode functions = uiProgram.addNode("Function Statement Blocks", namespace, new String());
				for (FunctionStatementBlock fsb : nsProgram.getFunctionBlocks().values()) {
					uiProgram.addNode(null, functions, fsb);
				}
			}
		}

		if (program.getStatementBlocks().size() > 0) {
			UIProgramNode statementBlocks = uiProgram.addNode("Statement Blocks", root, new String());
			for (StatementBlock sb : program.getStatementBlocks()) {
				uiProgram.addNode(sb.toString(), statementBlocks, sb);
			}
		}

		uiProgram.determineTreeStats();
		return uiProgram;
	}

	public static class UIProgramNode {
		int id;
		int depth;
		String value;
		Object nodeObject;

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

		List<UIProgramNode> children = new ArrayList<UIProgramNode>();
		UIProgramNode parent = null;

		public UIProgramNode(int id, int depth, String value, UIProgramNode parent, Object nodeObject) {
			this.id = id;
			this.depth = depth;
			this.value = value;
			this.parent = parent;
			if (parent != null) {
				parent.children.add(this);
			}
			this.nodeObject = nodeObject;
		}

	}

	public static class UIProgram {
		UIProgramNode root = null;

		int numNodes = 0;
		int widestDepth = 0;
		int widestDepthNumNodes = 0;
		List<Integer> depthCounts = null;
		int depth = 0;

		public void determineTreeStats() {
			TreeStats treeStats = new TreeStats();
			doTreeStats(treeStats, root);

			for (int i = 0; i < treeStats.depthCounts.size(); i++) {
				int depthCount = treeStats.depthCounts.get(i);
				if (depthCount > treeStats.widestDepthNumNodes) {
					treeStats.widestDepthNumNodes = depthCount;
					treeStats.widestDepth = i;
				}
			}

			numNodes = treeStats.numNodes;
			widestDepth = treeStats.widestDepth;
			widestDepthNumNodes = treeStats.widestDepthNumNodes;
			depthCounts = new ArrayList<Integer>(treeStats.depthCounts);
			depth = depthCounts.size();
		}

		@Override
		public String toString() {
			return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

		private void doTreeStats(TreeStats treeStats, UIProgramNode node) {
			treeStats.numNodes++;
			if (treeStats.depthCounts.size() <= node.depth) {
				treeStats.depthCounts.add(1);
			} else {
				int depthCount = treeStats.depthCounts.get(node.depth);
				treeStats.depthCounts.set(node.depth, ++depthCount);
			}
			for (UIProgramNode child : node.children) {
				doTreeStats(treeStats, child);
			}
		}

		public UIProgramNode addNode(String value, UIProgramNode parent, Object nodeObject) {
			if (parent == null) {
				root = new UIProgramNode(numNodes++, 0, value, null, nodeObject);
				return root;
			}
			if (nodeObject instanceof FunctionStatementBlock) {
				UIProgramNode fsbNode = new UIProgramNode(numNodes++, (parent.depth + 1), "Function Statement Block",
						parent, nodeObject);
				FunctionStatementBlock fsb = (FunctionStatementBlock) nodeObject;
				for (Statement statement : fsb.getStatements()) {
					addNode(statement.toString(), fsbNode, statement);
				}
				return fsbNode;
			} else if (nodeObject instanceof StatementBlock) {
				String name = "";
				if (nodeObject instanceof ParForStatementBlock) {
					name = "ParFor Statement Block";
				} else if (nodeObject instanceof ForStatementBlock) {
					name = "For Statement Block";
				} else if (nodeObject instanceof IfStatementBlock) {
					name = "If Statement Block";
				} else if (nodeObject instanceof WhileStatementBlock) {
					name = "While Statement Block";
				} else {
					name = "Statement Block";
				}
				UIProgramNode sbNode = new UIProgramNode(numNodes++, (parent.depth + 1), name, parent, nodeObject);
				StatementBlock sb = (StatementBlock) nodeObject;
				for (Statement statement : sb.getStatements()) {
					addNode(statement.toString(), sbNode, statement);
				}
				return sbNode;
			} else if (nodeObject instanceof FunctionStatement) {
				FunctionStatement fs = (FunctionStatement) nodeObject;
				UIProgramNode fsNode = new UIProgramNode(numNodes++, (parent.depth + 1), fs.getSignature(), parent,
						nodeObject);
				for (StatementBlock sb : fs.getBody()) {
					addNode(sb.toString(), fsNode, sb);
				}
				return fsNode;
			} else if (nodeObject instanceof IfStatement) {
				IfStatement is = (IfStatement) nodeObject;
				UIProgramNode ifNode = new UIProgramNode(numNodes++, (parent.depth + 1),
						"if " + is.getConditionalPredicate().toString(), parent, nodeObject);
				for (StatementBlock sb : is.getIfBody()) {
					addNode(sb.toString(), ifNode, sb);
				}
				if (!is.getElseBody().isEmpty()) {
					UIProgramNode elseNode = new UIProgramNode(numNodes++, (parent.depth + 1), "else", parent,
							nodeObject);
					for (StatementBlock sb : is.getElseBody()) {
						addNode(sb.toString(), elseNode, sb);
					}
					return elseNode;
				}
				return ifNode;
			} else if (nodeObject instanceof ParForStatement) {
				ParForStatement pfs = (ParForStatement) nodeObject;
				UIProgramNode parforNode = new UIProgramNode(numNodes++, (parent.depth + 1),
						"parfor " + pfs.getIterablePredicate().toString(), parent, nodeObject);
				for (StatementBlock sb : pfs.getBody()) {
					addNode(sb.toString(), parforNode, sb);
				}
				return parforNode;
			} else if (nodeObject instanceof ForStatement) {
				ForStatement fs = (ForStatement) nodeObject;
				UIProgramNode forNode = new UIProgramNode(numNodes++, (parent.depth + 1),
						"for " + fs.getIterablePredicate().toString(), parent, nodeObject);
				for (StatementBlock sb : fs.getBody()) {
					addNode(sb.toString(), forNode, sb);
				}
				return forNode;
			} else if (nodeObject instanceof WhileStatement) {
				WhileStatement ws = (WhileStatement) nodeObject;
				UIProgramNode whileNode = new UIProgramNode(numNodes++, (parent.depth + 1),
						"while " + ws.getConditionalPredicate().toString(), parent, nodeObject);
				for (StatementBlock sb : ws.getBody()) {
					addNode(sb.toString(), whileNode, sb);
				}
				return whileNode;
			}
			UIProgramNode node = new UIProgramNode(numNodes++, (parent.depth + 1), value, parent, nodeObject);
			return node;
		}

		public String displayProgram() {
			StringBuilder sb = new StringBuilder();
			displayNodeValue(sb, root);
			prevIfNodeObject = null;
			return sb.toString();
		}

		private void displayNodeValue(StringBuilder sb, UIProgramNode node) {
			for (int i = 0; i < node.depth; i++) {
				sb.append("  ");
			}
			sb.append(node.value);
			if (!(node.nodeObject instanceof String)) {
				sb.append(" (" + node.nodeObject.getClass().getSimpleName() + ")");
			}
			sb.append(" (id:" + node.id + ")");
			sb.append(" (depth: " + node.depth + ")\n");
			for (UIProgramNode child : node.children) {
				displayNodeValue(sb, child);
			}
		}

		public String displayProgramDetails() {
			StringBuilder sb = new StringBuilder();
			displayNodeValueDetails(sb, root);
			prevIfNodeObject = null;
			return sb.toString();
		}

		private void displayNodeValueDetails(StringBuilder sb, UIProgramNode node) {
			for (int i = 0; i < node.depth; i++) {
				sb.append("  ");
			}
			sb.append(StringEscapeUtils.escapeHtml4(generateNodeObjectDetails(node.value, node.nodeObject)));
			if (!(node.nodeObject instanceof String)) {
				sb.append(" (" + node.nodeObject.getClass().getSimpleName() + ")");
			}
			sb.append(" (id:" + node.id + ")");
			sb.append(" (depth: " + node.depth + ")\n");
			for (UIProgramNode child : node.children) {
				displayNodeValueDetails(sb, child);
			}
		}

		public String d3ProgramData() {
			StringBuilder sb = new StringBuilder();
			d3NodeData(sb, root);
			if (sb.length() > 0) {
				sb.replace(sb.length() - 1, sb.length(), "");
			}
			prevIfNodeObject = null;
			return sb.toString();
		}

		private void d3NodeData(StringBuilder sb, UIProgramNode node) {
			String spaces = "";
			for (int i = 0; i < node.depth; i++) {
				spaces = spaces + "  ";
			}
			sb.append(spaces + "{");
			// sb.append(" \"text\": \"" +
			// StringEscapeUtils.escapeJson(node.value) + "\"");

			boolean hasClass = false;
			if (!(node.nodeObject instanceof String)) {
				// sb.append(",\n");
				// sb.append(spaces);
				// sb.append(" \"class\": \"" +
				// node.nodeObject.getClass().getSimpleName() + "\"");
				hasClass = true;
			}

			// sb.append(",\n");
			// sb.append(spaces);
			if (hasClass) {
				sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + " ["
						+ node.nodeObject.getClass().getSimpleName() + "]\"");
			} else {
				sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + "\"");
			}

			if (node.children.size() > 0) {
				sb.append(",\n");
				sb.append(spaces + "  \"children\": [\n");
			}
			for (int i = 0; i < node.children.size(); i++) {
				UIProgramNode child = node.children.get(i);
				d3NodeData(sb, child);
				if (i < node.children.size() - 1) {
					sb.replace(sb.length() - 1, sb.length(), ",\n");
				}
			}
			if (node.children.size() > 0) {
				sb.append(spaces + "  ]\n");
				sb.append(spaces + "}\n");
			} else {
				sb.append(" }\n");
			}
		}

		public String d3ProgramDataDetails() {
			StringBuilder sb = new StringBuilder();
			d3NodeDataDetails(sb, root);
			if (sb.length() > 0) {
				sb.replace(sb.length() - 1, sb.length(), "");
			}
			prevIfNodeObject = null;
			return sb.toString();
		}

		private void d3NodeDataDetails(StringBuilder sb, UIProgramNode node) {
			String spaces = "";
			for (int i = 0; i < node.depth; i++) {
				spaces = spaces + "  ";
			}
			sb.append(spaces + "{");
			// sb.append(" \"text\": \"" +
			// StringEscapeUtils.escapeJson(node.value) + "\"");

			boolean hasClass = false;
			if (!(node.nodeObject instanceof String)) {
				// sb.append(",\n");
				// sb.append(spaces);
				// sb.append(" \"class\": \"" +
				// node.nodeObject.getClass().getSimpleName() + "\"");
				hasClass = true;
			}

			// sb.append(",\n");
			// sb.append(spaces);
			if (hasClass) {
				sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + " ["
						+ node.nodeObject.getClass().getSimpleName() + "]\"");
			} else {
				sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + "\"");
			}

			if (hasClass) {
				sb.append(",\n");
				sb.append(spaces);
				sb.append("  \"details\": \"");
				sb.append(StringEscapeUtils.escapeJson(generateNodeObjectDetails(node.value, node.nodeObject)));
				sb.append("\"");
			}

			if (node.children.size() > 0) {
				sb.append(",\n");
				sb.append(spaces + "  \"children\": [\n");
			}
			for (int i = 0; i < node.children.size(); i++) {
				UIProgramNode child = node.children.get(i);
				d3NodeDataDetails(sb, child);
				if (i < node.children.size() - 1) {
					sb.replace(sb.length() - 1, sb.length(), ",\n");
				}
			}
			if (node.children.size() > 0) {
				sb.append(spaces + "  ]\n");
				sb.append(spaces + "}\n");
			} else {
				sb.append(" }\n");
			}
		}

		public String d3ProgramDataBasicDetails() {
			StringBuilder sb = new StringBuilder();
			d3NodeDataBasicDetails(sb, root);
			if (sb.length() > 0) {
				sb.replace(sb.length() - 1, sb.length(), "");
			}
			prevIfNodeObject = null;
			return sb.toString();
		}

		private void d3NodeDataBasicDetails(StringBuilder sb, UIProgramNode node) {
			String spaces = "";
			for (int i = 0; i < node.depth; i++) {
				spaces = spaces + "  ";
			}
			sb.append(spaces + "{");

			boolean hasClass = false;
			if (!(node.nodeObject instanceof String)) {
				hasClass = true;
			}

			if (hasClass) {
				sb.append("  \"name\": \"");
				sb.append(StringEscapeUtils.escapeJson(generateNodeObjectDetails(node.value, node.nodeObject)));
				sb.append("\"");
			} else {
				sb.append(" \"name\": \"" + StringEscapeUtils.escapeJson(node.value) + "\"");
			}

			if (node.children.size() > 0) {
				sb.append(",\n");
				sb.append(spaces + "  \"children\": [\n");
			}
			for (int i = 0; i < node.children.size(); i++) {
				UIProgramNode child = node.children.get(i);
				d3NodeDataBasicDetails(sb, child);
				if (i < node.children.size() - 1) {
					sb.replace(sb.length() - 1, sb.length(), ",\n");
				}
			}
			if (node.children.size() > 0) {
				sb.append(spaces + "  ]\n");
				sb.append(spaces + "}\n");
			} else {
				sb.append(" }\n");
			}
		}

		private Object prevIfNodeObject = null;

		private String generateNodeObjectDetails(String value, Object nodeObject) {

			if (nodeObject instanceof IfStatement) {
				IfStatement is = (IfStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				if (nodeObject != prevIfNodeObject) {
					ConditionalPredicate cp = is.getConditionalPredicate();
					sb.append(sn(is));
					sb.append("  ");
					sb.append("if (" + sn(cp) + "(");
					Expression exp = cp.getPredicate();
					buildExpressionDetails(exp, sb);
					sb.append(")");
					sb.append(")");
				} else {
					sb.append(sn(is));
					sb.append("  ");
					sb.append("else");
				}
				prevIfNodeObject = nodeObject;
				return sb.toString();
			} else if (nodeObject instanceof ParForStatement) {
				ParForStatement pfs = (ParForStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				IterablePredicate ip = pfs.getIterablePredicate();
				sb.append(sn(pfs));
				sb.append("  ");
				sb.append("parfor (" + sn(ip) + "(");
				sb.append(sn(ip.getIterVar()) + ip.getIterVar().getName() + " ");
				sb.append("in ");
				sb.append(sn(ip.getFromExpr()) + ip.getFromExpr().toString());
				sb.append(":");
				sb.append(sn(ip.getToExpr()) + ip.getToExpr().toString());
				if (ip.getIncrementExpr() != null) {
					sb.append(",");
					sb.append(sn(ip.getIncrementExpr()) + ip.getIncrementExpr().toString());
				}
				HashMap<String, String> parForParams = ip.getParForParams();
				if ((parForParams != null) && (parForParams.size() > 0)) {
					for (String key : parForParams.keySet()) {
						sb.append(", ");
						sb.append(key);
						sb.append("=");
						sb.append(parForParams.get(key));
					}
				}
				sb.append(")");
				sb.append(")");
				return sb.toString();
			} else if (nodeObject instanceof ForStatement) {
				ForStatement fs = (ForStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				IterablePredicate ip = fs.getIterablePredicate();
				sb.append(sn(fs));
				sb.append("  ");
				sb.append("for (" + sn(ip) + "(");
				sb.append(sn(ip.getIterVar()) + ip.getIterVar().getName() + " ");
				sb.append("in ");
				sb.append(sn(ip.getFromExpr()) + ip.getFromExpr().toString());
				sb.append(":");
				sb.append(sn(ip.getToExpr()) + ip.getToExpr().toString());
				if (ip.getIncrementExpr() != null) {
					sb.append(",");
					sb.append(sn(ip.getIncrementExpr()) + ip.getIncrementExpr().toString());
				}
				HashMap<String, String> parForParams = ip.getParForParams();
				if ((parForParams != null) && (parForParams.size() > 0)) {
					for (String key : parForParams.keySet()) {
						sb.append(", ");
						sb.append(key);
						sb.append("=");
						sb.append(parForParams.get(key));
					}
				}
				sb.append(")");
				sb.append(")");
				return sb.toString();
			} else if (nodeObject instanceof WhileStatement) {
				WhileStatement ws = (WhileStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				ConditionalPredicate cp = ws.getConditionalPredicate();
				sb.append(sn(ws));
				sb.append("  ");
				sb.append("while (" + sn(cp) + "(");
				Expression exp = cp.getPredicate();
				if (exp instanceof RelationalExpression) {
					RelationalExpression rExp = (RelationalExpression) exp;
					Expression left = rExp.getLeft();
					RelationalOp opCode = rExp.getOpCode();
					Expression right = rExp.getRight();
					sb.append(sn(rExp) + "(");
					sb.append(sn(left) + left.toString());
					sb.append(" " + sn(opCode) + opCode.toString());
					sb.append(" " + sn(right) + right.toString());
					sb.append(")");
				} else {
					sb.append(exp.toString());
				}
				sb.append(")");
				return sb.toString();
			} else if (nodeObject instanceof AssignmentStatement) {
				AssignmentStatement as = (AssignmentStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(as));
				sb.append("  ");
				ArrayList<DataIdentifier> targetList = as.getTargetList();
				if (targetList.size() == 0) {
					sb.append("(NO TARGET)");
				} else if (targetList.size() > 1) {
					sb.append("(");
				}
				boolean first = true;
				for (DataIdentifier target : targetList) {
					if (!first) {
						sb.append(", ");
					} else {
						first = false;
					}
					sb.append(sn(target) + target.toString());
				}
				if (targetList.size() > 1) {
					sb.append(")");
				}
				sb.append(" = ");
				Expression source = as.getSource();
				buildExpressionDetails(source, sb);
				return sb.toString();
			} else if (nodeObject instanceof PrintStatement) {
				PrintStatement ps = (PrintStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(ps));
				sb.append("  ");
				PRINTTYPE type = ps.getType();
				List<Expression> expressions = ps.getExpressions();
				sb.append(type);
				sb.append("(");
				if ((type == PRINTTYPE.PRINT) || (type == PRINTTYPE.STOP)) {
					Expression exp = expressions.get(0);
					buildExpressionDetails(exp, sb);
				} else if (type == PRINTTYPE.PRINTF) {
					for (int i = 0; i < expressions.size(); i++) {
						if (i > 0) {
							sb.append(", ");
						}
						Expression exp = expressions.get(i);
						buildExpressionDetails(exp, sb);
					}
				}
				sb.append(")");
				return sb.toString();
			} else if (nodeObject instanceof OutputStatement) {
				OutputStatement os = (OutputStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(os));
				sb.append("  ");

				DataIdentifier identifier = os.getIdentifier();
				DataExpression source = os.getSource();
				sb.append(Statement.OUTPUTSTATEMENT + "(");
				sb.append("id=" + identifier);
				for (String key : source.getVarParams().keySet()) {
					sb.append(", ");
					sb.append(key);
					sb.append("=");
					Expression exp = source.getVarParam(key);
					if (exp instanceof StringIdentifier) {
						sb.append("\"");
						sb.append(exp.toString());
						sb.append("\"");
					} else {
						buildExpressionDetails(exp, sb);
					}
				}
				sb.append(");");
				return sb.toString();
			} else if (nodeObject instanceof FunctionStatementBlock) {
				FunctionStatementBlock fsb = (FunctionStatementBlock) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(fsb));
				sb.append("  ");
				sb.append(value);
				return sb.toString();
			} else if (nodeObject instanceof StatementBlock) {
				StatementBlock stbl = (StatementBlock) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(stbl));
				sb.append("  ");
				sb.append(value);
				return sb.toString();
			} else if (nodeObject instanceof FunctionStatement) {
				FunctionStatement fs = (FunctionStatement) nodeObject;
				StringBuilder sb = new StringBuilder();
				sb.append(sn(fs));
				sb.append("  ");
				// sb.append(value);
				// sb.append(sn(fs))
				sb.append(fs.getName());
				sb.append(" = function(");

				ArrayList<DataIdentifier> inputParams = fs.getInputParams();
				for (int i = 0; i < inputParams.size(); i++) {
					if (i > 0) {
						sb.append(", ");
					}
					DataIdentifier inputParam = inputParams.get(i);
					sb.append(sn(inputParam));
					sb.append(inputParam.getName());
					String defaultValue = inputParam.getDefaultValue();
					if (defaultValue != null) {
						sb.append("=" + defaultValue);
					}
				}
				sb.append(") return (");
				ArrayList<DataIdentifier> outputParams = fs.getOutputParams();
				for (int i = 0; i < outputParams.size(); i++) {
					if (i > 0) {
						sb.append(", ");
					}
					DataIdentifier outputParam = outputParams.get(i);
					sb.append(sn(outputParam));
					sb.append(outputParam.getName());
				}
				sb.append(")");

				return sb.toString();
			}

			return value;
		}

		private void buildExpressionDetails(Expression exp, StringBuilder sb) {
			sb.append(sn(exp));
			if (exp instanceof StringIdentifier) {
				sb.append("\"");
				sb.append(exp.toString());
				sb.append("\"");
			} else if (exp instanceof RelationalExpression) {
				RelationalExpression rExp = (RelationalExpression) exp;
				Expression left = rExp.getLeft();
				RelationalOp opCode = rExp.getOpCode();
				Expression right = rExp.getRight();
				sb.append("(");
				buildExpressionDetails(left, sb);
				sb.append(" " + sn(opCode) + opCode.toString() + " ");
				buildExpressionDetails(right, sb);
				sb.append(")");
			} else if (exp instanceof BuiltinFunctionExpression) {
				BuiltinFunctionExpression bfExp = (BuiltinFunctionExpression) exp;
				BuiltinFunctionOp opCode = bfExp.getOpCode();
				sb.append("(");
				sb.append(sn(opCode));
				sb.append(opCode);
				sb.append("(");
				Expression[] allExpr = bfExp.getAllExpr();
				for (int i = 0; i < allExpr.length; i++) {
					if (i > 0) {
						sb.append(",");
					}
					buildExpressionDetails(allExpr[i], sb);
				}
				sb.append(")");
				sb.append(")");
			} else if (exp instanceof ParameterizedBuiltinFunctionExpression) {
				ParameterizedBuiltinFunctionExpression pbfExp = (ParameterizedBuiltinFunctionExpression) exp;
				ParameterizedBuiltinFunctionOp opCode = pbfExp.getOpCode();
				sb.append("(");
				sb.append(sn(opCode) + opCode.toString() + "(");
				HashMap<String, Expression> varParams = pbfExp.getVarParams();
				boolean first = true;
				for (String key : varParams.keySet()) {
					Expression expression = varParams.get(key);
					if (!first) {
						sb.append(", ");
					} else {
						first = false;
					}
					if (key != null) {
						sb.append(key);
						sb.append("=");
					}
					buildExpressionDetails(expression, sb);
				}
				sb.append(")");
				sb.append(")");
			} else if (exp instanceof BinaryExpression) {
				BinaryExpression bExp = (BinaryExpression) exp;
				Expression left = bExp.getLeft();
				BinaryOp opCode = bExp.getOpCode();
				Expression right = bExp.getRight();
				sb.append("(");
				buildExpressionDetails(left, sb);
				sb.append(" " + sn(opCode) + opCode.toString() + " ");
				buildExpressionDetails(right, sb);
				sb.append(")");
			} else if (exp instanceof BooleanExpression) {
				BooleanExpression bExp = (BooleanExpression) exp;
				Expression left = bExp.getLeft();
				BooleanOp opCode = bExp.getOpCode();
				Expression right = bExp.getRight();
				sb.append("(");
				if (opCode == BooleanOp.NOT) {
					sb.append(sn(opCode) + opCode.toString() + " ");
					buildExpressionDetails(left, sb);
				} else {
					buildExpressionDetails(left, sb);
					sb.append(" " + sn(opCode) + opCode.toString() + " ");
					buildExpressionDetails(right, sb);
				}
				sb.append(")");
			} else if (exp instanceof DataExpression) {
				DataExpression dExp = (DataExpression) exp;

				DataOp opCode = dExp.getOpCode();
				sb.append("(");
				sb.append(sn(opCode) + opCode.toString() + "(");
				HashMap<String, Expression> varParams = dExp.getVarParams();
				boolean first = true;
				for (String key : varParams.keySet()) {
					Expression expression = varParams.get(key);
					if (!first) {
						sb.append(", ");
					} else {
						first = false;
					}
					sb.append(key);
					sb.append("=");
					buildExpressionDetails(expression, sb);
				}
				sb.append(")");
				sb.append(")");
			} else if (exp instanceof FunctionCallIdentifier) {
				FunctionCallIdentifier fci = (FunctionCallIdentifier) exp;

				FunctCallOp opCode = fci.getOpCode();
				if (opCode != null) {
					sb.append("(");
					sb.append(sn(opCode) + opCode.toString());
				}
				sb.append("(");
				String namespace = fci.getNamespace();
				// if (!DMLProgram.DEFAULT_NAMESPACE.equals(namespace)) {
				sb.append(namespace + "::");
				// }
				sb.append(fci.getName());
				sb.append("(");
				ArrayList<ParameterExpression> paramExprs = fci.getParamExprs();
				for (int i = 0; i < paramExprs.size(); i++) {
					if (i > 0) {
						sb.append(",");
					}
					ParameterExpression pe = paramExprs.get(i);
					String name = pe.getName();
					Expression expression = pe.getExpr();
					if (name != null) {
						sb.append(name);
						sb.append("=");
					}
					buildExpressionDetails(expression, sb);
				}
				sb.append(")");
				sb.append(")");
				if (opCode != null) {
					sb.append(")");
				}
			} else {
				sb.append(exp.toString());
			}
		}

		private String sn(Object o) {
			if (o instanceof DataIdentifier) {
				DataIdentifier di = (DataIdentifier) o;
				StringBuilder sb = new StringBuilder();
				sb.append("[" + di.getClass().getSimpleName());
				DataType dataType = di.getDataType();
				ValueType valueType = di.getValueType();
				if ((dataType != DataType.UNKNOWN) || (valueType != ValueType.UNKNOWN)) {
					sb.append("{");
					sb.append(di.getDataType());
					sb.append(",");
					sb.append(di.getValueType());
					sb.append("}");
				}
				sb.append("]");
				return sb.toString();
			} else {
				return "[" + o.getClass().getSimpleName() + "]";
			}
		}
	}

	public static String displaySymbolTable(Script script) {
		LocalVariableMap symbolTable = script.getSymbolTable();
		ScriptExecutor se = script.getScriptExecutor();
		SparkExecutionContext sec = null;
		if (se != null) {
			sec = (SparkExecutionContext) se.getExecutionContext();
		}
		StringBuilder sb = new StringBuilder();
		Set<String> keys = symbolTable.keySet();
		keys = new TreeSet<String>(keys);
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				sb.append("  [");
				sb.append(++count);
				sb.append("]");

				sb.append(" (");
				sb.append(MLContextUtil.determineOutputTypeAsString(symbolTable, key));
				sb.append(") ");

				sb.append(key);

				sb.append(": ");
				if (sec != null) {
					Data data = symbolTable.get(key);
					sb.append(StringUtils.abbreviate(data.toString(), 1000));
					if (data instanceof MatrixObject) {
						sb.append("\n");
						MatrixObject mo = (MatrixObject) data;
						Matrix m = new Matrix(mo, sec);
						String showString = m.toDF().sort("__INDEX").showString(20, true);
						String[] lines = showString.split("\\n");
						for (String line : lines) {
							sb.append("       ");
							sb.append(line);
							sb.append("\n");
						}
					}
				} else {
					sb.append(StringUtils.abbreviate(symbolTable.get(key).toString(), 1000));
				}
				sb.append("\n");
			}
		}
		return sb.toString();
	}

	public static String displayInputs(Script script) {
		Map<String, Object> inputs = script.getInputs();
		LocalVariableMap symbolTable = script.getSymbolTable();
		ScriptExecutor se = script.getScriptExecutor();
		SparkExecutionContext sec = null;
		if (se != null) {
			sec = (SparkExecutionContext) se.getExecutionContext();
		}
		StringBuilder sb = new StringBuilder();

		Set<String> keys = inputs.keySet();
		keys = new TreeSet<String>(keys);
		if (keys.isEmpty()) {
			sb.append("None\n");
		} else {
			int count = 0;
			for (String key : keys) {
				Object object = inputs.get(key);
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
				if (MLContextUtil.doesSymbolTableContainMatrixObject(symbolTable, key)) {
					sb.append(" as Matrix");
				} else if (MLContextUtil.doesSymbolTableContainFrameObject(symbolTable, key)) {
					sb.append(" as Frame");
				}
				sb.append(") ");

				sb.append(key);
				sb.append(": ");

				if (sec != null) {
					sb.append(StringUtils.abbreviate(object.toString(), 1000));
					if (object instanceof MatrixObject) {
						sb.append("\n");
						MatrixObject mo = (MatrixObject) object;
						Matrix m = new Matrix(mo, sec);
						String showString = m.toDF().sort("__INDEX").showString(20, true);
						String[] lines = showString.split("\\n");
						for (String line : lines) {
							sb.append("       ");
							sb.append(line);
							sb.append("\n");
						}
					} else if (object instanceof Dataset) {
						sb.append("\n");
						Dataset<?> ds = (Dataset<?>) object;
						String showString = ds.showString(20, true);
						String[] lines = showString.split("\\n");
						for (String line : lines) {
							sb.append("       ");
							sb.append(line);
							sb.append("\n");
						}
					}
				} else {
					sb.append(StringUtils.abbreviate(object.toString(), 1000));
				}

				sb.append("\n");
			}
		}
		return sb.toString();
	}

	public static String displayOutputs(Script script) {
		Set<String> outputNames = script.getOutputVariables();
		outputNames = new TreeSet<String>(outputNames);
		LocalVariableMap symbolTable = script.getSymbolTable();
		ScriptExecutor se = script.getScriptExecutor();
		SparkExecutionContext sec = null;
		if (se != null) {
			sec = (SparkExecutionContext) se.getExecutionContext();
		}
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
					sb.append(MLContextUtil.determineOutputTypeAsString(symbolTable, outputName));
					sb.append(") ");
				}

				sb.append(outputName);

				Data data = symbolTable.get(outputName);
				if (data != null) {
					sb.append(": ");
					if (sec != null) {
						sb.append(StringUtils.abbreviate(data.toString(), 1000));
						if (data instanceof MatrixObject) {
							sb.append("\n");
							MatrixObject mo = (MatrixObject) data;
							Matrix m = new Matrix(mo, sec);
							String showString = m.toDF().sort("__INDEX").showString(20, true);
							String[] lines = showString.split("\\n");
							for (String line : lines) {
								sb.append("       ");
								sb.append(line);
								sb.append("\n");
							}
						}
					} else {
						sb.append(StringUtils.abbreviate(data.toString(), 1000));
					}
				}

				sb.append("\n");
			}
		}
		return sb.toString();
	}

}
