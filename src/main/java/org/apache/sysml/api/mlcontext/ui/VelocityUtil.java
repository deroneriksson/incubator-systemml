package org.apache.sysml.api.mlcontext.ui;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

/**
 * Velocity utility class for user interface functionality.
 *
 */
public class VelocityUtil {

	public static final String TEMPLATE_PATH = "org/apache/sysml/api/mlcontext/ui/templates/";
	public static final String TEMPLATE_EXTENSION = ".vm";

	protected static String generateOutputWithHeaderAndFooter(String templateName, VelocityContext velocityContext) {
		StringBuilder sb = new StringBuilder();
		sb.append(generateHeader(velocityContext));
		sb.append(generateOutput(templateName, velocityContext));
		sb.append(generateFooter(velocityContext));
		return sb.toString();
	}

	protected static String generateHeader(VelocityContext velocityContext) {
		Template header = getTemplate("header");
		String output = generateOutput(header, velocityContext);
		return output;
	}

	protected static String generateFooter(VelocityContext velocityContext) {
		Template footer = getTemplate("footer");
		String output = generateOutput(footer, velocityContext);
		return output;
	}

	protected static Template getTemplate(String templateName) {
		Template template = Velocity.getTemplate(fullPathToTemplate(templateName));
		return template;
	}

	protected static String generateOutput(String templateName, VelocityContext velocityContext) {
		Template template = getTemplate(templateName);
		String output = generateOutput(template, velocityContext);
		return output;
	}

	protected static String generateOutput(Template template, VelocityContext velocityContext) {
		StringWriter sw = new StringWriter();
		template.merge(velocityContext, sw);
		String output = sw.toString();
		return output;
	}

	protected static String fullPathToTemplate(String templateName) {
		return TEMPLATE_PATH + templateName + TEMPLATE_EXTENSION;
	}

	// for debugging
	protected static InputStream getTemplateAsInputStream(String templateName) {
		InputStream is = VelocityUtil.class.getClassLoader().getResourceAsStream(fullPathToTemplate(templateName));
		return is;
	}

	// for debugging
	protected static String getTemplateAsString(InputStream is) throws IOException {
		String templateAsString = IOUtils.toString(is);
		return templateAsString;
	}

}
