/**
 * 
 */
package org.one.remote.common;

/**
 * @author yangkunguo
 *
 */
public class OneMQException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int code;
    private String message;
    
    
	public OneMQException() {
		super();
		// TODO Auto-generated constructor stub
	}

	public OneMQException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public OneMQException(String message, Throwable cause) {
		super(message, cause);
	}

	public OneMQException(String message) {
		super(message);
	}
	
	public OneMQException(int code,String message) {
		this.code=code;
		this.message=message;
		
	}

	public OneMQException(Throwable cause) {
		super(cause);
	}

}
