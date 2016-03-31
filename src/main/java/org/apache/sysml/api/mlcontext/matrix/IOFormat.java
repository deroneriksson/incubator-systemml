package org.apache.sysml.api.mlcontext.matrix;

/**
 * SystemML currently supports 4 formats: CSV (delimited), Matrix Market, IJV, and Binary. CSV, Matrix Market, and IJV
 * are all text-based formats.
 *
 */
public enum IOFormat {
	CSV, MATRIX_MARKET, IJV, BINARY;
}
