/**
 * 
 */
package org.one.remote.common.enums;

/**
 * @author yangkunguo
 *
 */
public enum ServerRole {
	MASTER("MASTER"),
	SLAVE("SLAVE");
	
	private String value;

	private ServerRole(String value) {
		this.value=value;
	}
	
	public String getValue() {
		return value;
	}
	
	
}
