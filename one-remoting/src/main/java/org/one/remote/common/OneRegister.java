/**
 * 
 */
package org.one.remote.common;

import java.io.Serializable;

/**注册中心
 * @author yangkunguo
 *
 */
public class OneRegister implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4174229192634968640L;

	private String registerUrl;

	public OneRegister(String registerUrl) {
		super();
		this.registerUrl = registerUrl;
	}

	public String getRegisterUrl() {
		return registerUrl;
	}

	public void setRegisterUrl(String registerUrl) {
		this.registerUrl = registerUrl;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((registerUrl == null) ? 0 : registerUrl.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OneRegister other = (OneRegister) obj;
		if (registerUrl == null) {
			if (other.registerUrl != null)
				return false;
		} else if (!registerUrl.equals(other.registerUrl))
			return false;
		return true;
	}

	public OneRegister() {
		super();
	}
	
}
