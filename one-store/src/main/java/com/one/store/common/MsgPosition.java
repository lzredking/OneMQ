/**
 * 
 */
package com.one.store.common;

/**
 * @author yangkunguo
 *
 */
public class MsgPosition {

	private Long position;
	private int length;
	
	
	
	public MsgPosition(long position, int length) {
		super();
		this.position = position;
		this.length = length;
	}
	public MsgPosition() {
		super();
	}
	public Long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	
}
