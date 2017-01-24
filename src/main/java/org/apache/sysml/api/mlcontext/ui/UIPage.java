package org.apache.sysml.api.mlcontext.ui;

public enum UIPage {
	DEMO("demo", "demo", "Demo"),
	SCRIPT_EDIT("script/edit", "script-edit", "Edit Script"),
	SCRIPT("script", "script", "Script"),
	SCRIPT_EXECUTE("script/execute", "script-execute", "Execute Script"),
	SCRIPT_PARSETREE("script/parse-tree", "parse-tree", "Parse Tree"),
	SCRIPT_PARSE_ERROR("script/parse-error", "parse-error", "Parse Error"),
	PROGRAM("script/program", "program", "Program"),
	PROGRAM_EXECUTION_DIFF("script/program-execution-diff", "program-execution-diff", "Program Execution Diff"),
	LIVE_VARIABLE_ANALYSIS("script/live-variable-analysis", "live-variable-analysis", "Live Variable Analysis"),
	HISTORY("history", "history", "Execution History"),
	HOME("home", "home", "Apache SystemML UI");

	private final String path;
	private final String pageName;
	private final String title;

	UIPage(String path, String pageName, String title) {
		this.path = path;
		this.pageName = pageName;
		this.title = title;
	}

	public String getPath() {
		return path;
	}

	public String getPageName() {
		return pageName;
	}

	public String getTitle() {
		return title;
	}

	public static UIPage getUIPage(String path) {
		for (UIPage uiPage : UIPage.values()) {
			if (path.equalsIgnoreCase(uiPage.getPath())) {
				return uiPage;
			}
		}
		return UIPage.HOME;
	}
}
