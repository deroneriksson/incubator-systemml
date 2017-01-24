package org.apache.sysml.api.mlcontext.ui;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.sysml.api.mlcontext.MLContext;
import org.apache.sysml.api.mlcontext.MLContextException;
import org.apache.sysml.api.mlcontext.MLResults;
import org.apache.sysml.api.mlcontext.Script;
import org.apache.sysml.api.mlcontext.ScriptExecutor;
import org.apache.sysml.api.mlcontext.ui.UIUtil.UIProgram;
import org.apache.sysml.parser.DMLProgram;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.parser.common.CustomErrorListener.ParseIssue;
import org.apache.velocity.VelocityContext;

public class UIServlet extends HttpServlet {

	private static final long serialVersionUID = 3132339183656048496L;

	protected UI ui = null;
	protected MLContext mlContext = null;
	protected String docBaseUrl = "http://systemml.apache.org";

	public UIServlet(UI ui, MLContext mlContext) {
		this.ui = ui;
		this.mlContext = mlContext;
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String uri = request.getRequestURI();
		if (uri.startsWith("/")) {
			uri = uri.substring(1);
		}
		if ((uri.startsWith(UIUtil.JS_START)) && (uri.endsWith(UIUtil.JS_EXTENSION))) {
			renderJs(request, response, uri);
			return;
		} else if (uri.startsWith(UIUtil.CSS_START)) {
			renderCss(request, response, uri);
			return;
		} else if (uri.startsWith(UIUtil.IMG_START)) {
			renderImg(request, response, uri);
			return;
		}

		response.setContentType("text/html;charset=utf-8");
		PrintWriter out = response.getWriter();
		controller(request, response, out);
		response.setStatus(HttpServletResponse.SC_OK);
		out.close();
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("text/html;charset=utf-8");
		PrintWriter out = response.getWriter();
		controller(request, response, out);
		response.setStatus(HttpServletResponse.SC_OK);
		out.close();
	}

	protected void renderJs(HttpServletRequest request, HttpServletResponse response, String uri) throws IOException {
		String jsName = uri.substring(uri.lastIndexOf("/") + 1, uri.length() - UIUtil.JS_EXTENSION.length());
		InputStream is = UIUtil.getJsAsInputStream(jsName);
		if (is != null) {
			response.setContentType("text/javascript;charset=utf-8");
			IOUtils.copy(is, response.getOutputStream());
		} else {
			throw new MLContextException("Could not read " + jsName + " from " + uri);
		}
	}

	protected void renderCss(HttpServletRequest request, HttpServletResponse response, String uri) throws IOException {
		String cssFile = uri.substring(uri.lastIndexOf("/") + 1, uri.length());
		InputStream is = UIUtil.getCssAsInputStream(cssFile);
		if (is != null) {
			response.setContentType("text/css;charset=utf-8");
			IOUtils.copy(is, response.getOutputStream());
		} else {
			throw new MLContextException("Could not read " + cssFile + " from " + uri);
		}
	}

	protected void renderImg(HttpServletRequest request, HttpServletResponse response, String uri) throws IOException {
		String imgFile = uri.substring(uri.lastIndexOf("/") + 1, uri.length());
		InputStream is = UIUtil.getImgAsInputStream(imgFile);
		if (is != null) {
			response.setContentType("image/png");
			IOUtils.copy(is, response.getOutputStream());
		} else {
			throw new MLContextException("Could not read " + imgFile + " from " + uri);
		}
	}

	protected void controller(HttpServletRequest request, HttpServletResponse response, PrintWriter out)
			throws ServletException, IOException {
		UIPage uiPage = determinePage(request);
		VelocityContext model = new VelocityContext();
		model.put("docBaseUrl", docBaseUrl);
		model.put("title", uiPage.getTitle());

		String view = null;
		switch (uiPage) {
		case DEMO:
			view = renderDemoView(uiPage, model);
			break;
		case SCRIPT_EDIT:
			view = renderEditScriptView(uiPage, model, request);
			break;
		case SCRIPT:
			view = renderScriptView(uiPage, model);
			break;
		case SCRIPT_PARSETREE:
			view = renderParseTreeView(uiPage, model);
			break;
		case HISTORY:
			view = renderHistoryView(uiPage, model);
			break;
		case HOME:
			view = renderHomeView(uiPage, model);
			break;
		case PROGRAM:
			view = renderProgramView(uiPage, model);
			break;
		case PROGRAM_EXECUTION_DIFF:
			view = renderProgramExecutionDiffView(uiPage, model);
			break;
		case LIVE_VARIABLE_ANALYSIS:
			view = renderLiveVariableAnalysisView(uiPage, model);
			break;
		case SCRIPT_EXECUTE:
			view = renderScriptExecuteView(uiPage, model);
			if (view == null) {
				response.sendRedirect("../script");
				return;
			}
			break;
		default:
			view = renderHomeView(uiPage, model);
		}
		out.println(view);

	}

	protected String renderScriptExecuteView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script == null) {
			model.put("message", "No script is active or has been registered.");
			String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
			return view;
		} else {
			mlContext.execute(script);
			return null;
		}

	}

	protected String renderHomeView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script == null) {
			model.put("message", "No script is active or has been registered.");
		} else {
			model.put("script", new UIScriptWrapper(script));
		}
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderHistoryView(UIPage uiPage, VelocityContext model) {
		String history = mlContext.history();
		if (history == null) {
			model.put("message", "The MLContext script history is currently empty.");
		} else {
			model.put("message", history);
		}
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderScriptView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderEditScriptView(UIPage uiPage, VelocityContext model, HttpServletRequest request) {
		Script script = ui.getScript();

		String scriptString = request.getParameter("scriptString");
		if (scriptString != null) {
			script.setScriptString(scriptString);
			script.setScriptExecutor(null);
		}

		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderParseTreeView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script != null) {
			ScriptExecutor se = script.getScriptExecutor();
			if (se == null) {
				se = new ScriptExecutor() {
					public MLResults execute(Script script) {
						setup(script);
						parseScript();
						return null;
					}
				};
				try {
					// se.execute(script);
					mlContext.execute(script, se); // alternative
				} catch (MLContextException e) {
					script.setScriptExecutor(null);
					Throwable cause = e.getCause();
					if (cause instanceof ParseException) {
						ParseException pe = (ParseException) cause;
						List<ParseIssue> parseIssues = pe.getParseIssues();
						model.put("title", UIPage.SCRIPT_PARSE_ERROR.getTitle());
						model.put("script", new UIScriptWrapper(script, parseIssues));
						model.put("message",
								UIUtil.generateUIParseIssuesMessage(script.getScriptExecutionString(), parseIssues));
						String view = VelocityUtil
								.generateOutputWithHeaderAndFooter(UIPage.SCRIPT_PARSE_ERROR.getPageName(), model);
						return view;

					} else {
						throw e;
					}
				}
			}
		}
		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderProgramView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script != null) {
			ScriptExecutor se = script.getScriptExecutor();
			if (se == null) {
				se = new ScriptExecutor() {
					public MLResults execute(Script script) {
						setup(script);
						parseScript();
						// liveVariableAnalysis();
						// validateScript();

						// initialSetup(script);
						// parseScript();
						// liveVariableAnalysis();
						// validateScript();
						// constructHops();
						// rewriteHops();
						// rewritePersistentReadsAndWrites();
						// constructLops();
						// generateRuntimeProgram();
						// showExplanation();
						// globalDataFlowOptimization();
						// countCompiledMRJobsAndSparkInstructions();
						// initializeCachingAndScratchSpace();
						// cleanupRuntimeProgram();
						// createAndInitializeExecutionContext();
						// executeRuntimeProgram();
						// setExplainRuntimeProgramInSparkMonitor();
						// cleanupAfterExecution();

						return null;
					}
				};
				try {
					// se.execute(script);
					mlContext.execute(script, se); // alternative
				} catch (MLContextException e) {
					script.setScriptExecutor(null);
					Throwable cause = e.getCause();
					if (cause instanceof ParseException) {
						return generateParseErrorView((ParseException) cause, model, script);
					} else {
						throw e;
					}
				}
			}
		}
		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderProgramExecutionDiffView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();

		ScriptExecutor se = new ScriptExecutor() {
			public MLResults execute(Script script) {
				setup(script);
				parseScript();
				return null;
			}
		};
		try {
			// se.execute(script);
			mlContext.execute(script, se);
		} catch (MLContextException e) {
			script.setScriptExecutor(null);
			Throwable cause = e.getCause();
			if (cause instanceof ParseException) {
				return generateParseErrorView((ParseException) cause, model, script);
			} else {
				throw e;
			}
		}
		DMLProgram program = se.getDmlProgram();
		UIProgram uiProgram = UIUtil.programToUiProgram(program);
		String displayProgram1 = uiProgram.displayProgram();
		String displayProgramDetails1 = uiProgram.displayProgramDetails();

		ScriptExecutor se2 = new ScriptExecutor();
		// script.setScriptExecutor(se2);
		// mlContext.execute(script, new ScriptExecutor());
		// se2.execute(script);
		mlContext.execute(script, se2);
		DMLProgram program2 = se2.getDmlProgram();
		UIProgram uiProgram2 = UIUtil.programToUiProgram(program2);
		String displayProgram2 = uiProgram2.displayProgram();
		String displayProgramDetails2 = uiProgram2.displayProgramDetails();

		model.put("scriptdiff",
				new UIScriptDiff(displayProgram1, displayProgram2, displayProgramDetails1, displayProgramDetails2));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String renderLiveVariableAnalysisView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script != null) {
			ScriptExecutor se = script.getScriptExecutor();
			if (se == null) {
				se = new ScriptExecutor() {
					public MLResults execute(Script script) {
						setup(script);
						parseScript();
						liveVariableAnalysis();
						return null;
					}
				};
				try {
					// se.execute(script);
					mlContext.execute(script, se);
				} catch (MLContextException e) {
					script.setScriptExecutor(null);
					Throwable cause = e.getCause();
					if (cause instanceof ParseException) {
						return generateParseErrorView((ParseException) cause, model, script);
					} else {
						throw e;
					}
				}
			}
		}

		// Main$ main = org.apache.spark.repl.Main$.MODULE$;
		// SparkILoop interp = main.interp();
		// interp.printWelcome();
		// IMain imain = interp.intp();
		// imain.

		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	protected String generateParseErrorView(ParseException pe, VelocityContext model, Script script) {
		List<ParseIssue> parseIssues = pe.getParseIssues();
		model.put("title", UIPage.SCRIPT_PARSE_ERROR.getTitle());
		model.put("script", new UIScriptWrapper(script, parseIssues));
		model.put("message", UIUtil.generateUIParseIssuesMessage(script.getScriptExecutionString(), parseIssues));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(UIPage.SCRIPT_PARSE_ERROR.getPageName(), model);
		return view;
	}

	protected String renderDemoView(UIPage uiPage, VelocityContext model) {
		Script script = ui.getScript();
		if (script != null) {
			ScriptExecutor se = script.getScriptExecutor();
			if (se == null) {
				se = new ScriptExecutor() {
					public MLResults execute(Script script) {
						setup(script);
						parseScript();
						return null;
					}
				};
				// se.execute(script);
				mlContext.execute(script, se); // alternative
			}
		}
		model.put("script", new UIScriptWrapper(script));
		String view = VelocityUtil.generateOutputWithHeaderAndFooter(uiPage.getPageName(), model);
		return view;
	}

	public UIPage determinePage(HttpServletRequest request) {
		String uri = request.getRequestURI();
		if (uri.startsWith("/")) {
			uri = uri.substring(1);
		}
		return UIPage.getUIPage(uri);
	}

}
