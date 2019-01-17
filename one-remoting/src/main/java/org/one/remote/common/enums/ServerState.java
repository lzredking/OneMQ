/**
 * 
 */
package org.one.remote.common.enums;

/**
 * @author yangkunguo
 *
 */
public enum ServerState {
	IM_OK("IM_OK");
	
	private String value;
	
	private ServerState(String value) {
		this.value=value;
	}
	
	public String getValue() {
		return value;
	}
	
	
	
}
