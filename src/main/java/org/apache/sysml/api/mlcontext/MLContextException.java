package org.apache.sysml.api.mlcontext;

/**
 * RuntimeException representing exceptions that occur through the MLContext API
 *
 */
public class MLContextException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MLContextException() {
		super();
	}

	public MLContextException(String message, Throwable cause) {
		super(message, cause);
	}

	public MLContextException(String message) {
		super(message);
	}

	public MLContextException(Throwable cause) {
		super(cause);
	}

}
