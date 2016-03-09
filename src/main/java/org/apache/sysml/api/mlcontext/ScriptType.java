package org.apache.sysml.api.mlcontext;

public enum ScriptType {
	DML, PYDML;

	public String lowerCase() {
		return super.toString().toLowerCase();
	}

	public boolean isDML() {
		if (this == ScriptType.DML) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isPYDML() {
		if (this == ScriptType.PYDML) {
			return true;
		} else {
			return false;
		}
	}

}
