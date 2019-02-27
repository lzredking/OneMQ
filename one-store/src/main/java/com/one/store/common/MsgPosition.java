/**
 * 
 */
package com.one.store.common;

/**
 * @author yangkunguo
 *
 */
public class MsgPosition {

	private String id;
	private Long position;
	private int length;
	
	
	
	public MsgPosition(String id, Long position, int length) {
		super();
		this.id = id;
		this.position = position;
		this.length = length;
	}
	
	public MsgPosition() {
		super();
	}
	public Long getPosition() {
		return position;
	}
	
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	@Override
	public String toString() {
		return "MsgPosition [position=" + position + ", length=" + length + "]";
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setPosition(Long position) {
		this.position = position;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		MsgPosition other = (MsgPosition) obj;
		if (id == null) {
			if (other.id != null) return false;
		} else if (!id.equals(other.id)) return false;
		return true;
	}
	
}
