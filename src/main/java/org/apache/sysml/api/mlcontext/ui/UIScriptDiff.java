package org.apache.sysml.api.mlcontext.ui;

import java.util.LinkedList;

import com.sksamuel.diffpatch.DiffMatchPatch;
import com.sksamuel.diffpatch.DiffMatchPatch.Diff;

public class UIScriptDiff {

	protected String beforeExecution;
	protected String afterExecution;
	protected String beforeExecutionDetails;
	protected String afterExecutionDetails;

	UIScriptDiff(String beforeExecution, String afterExecution, String beforeExecutionDetails,
			String afterExecutionDetails) {
		this.beforeExecution = beforeExecution;
		this.afterExecution = afterExecution;
		this.beforeExecutionDetails = beforeExecutionDetails;
		this.afterExecutionDetails = afterExecutionDetails;
	}

	public String getExecutionDiff() {
		DiffMatchPatch dmp = new DiffMatchPatch();
		LinkedList<Diff> diffs = dmp.diff_main(beforeExecution, afterExecution);
		dmp.diff_cleanupSemantic(diffs);
		String prettyHtml = dmp.diff_prettyHtml(diffs);
		return prettyHtml;
	}

	public String getExecutionDiffDetails() {
		DiffMatchPatch dmp = new DiffMatchPatch();
		LinkedList<Diff> diffs = dmp.diff_main(beforeExecutionDetails, afterExecutionDetails);
		dmp.diff_cleanupSemantic(diffs);
		String prettyHtml = dmp.diff_prettyHtml(diffs);
		return prettyHtml;
	}
}
