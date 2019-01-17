/**
 * 
 */
package org.one.remote.cmd;

import org.one.remote.common.enums.RequestType;
import org.tio.core.intf.Packet;

/**
 * @author yangkunguo
 *
 */
public class Command extends Packet{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final int HEADER_LENGHT = 5;//消息头的长度
    public static final String CHARSET = "utf-8";
    
    /**
     * 消息模式
     * byte REGISTER =0;
	byte BROKER=1;
	byte PRODUCER=2;
	byte COMSUMER=3;
	byte MESSAGE=4;
     */
    private Byte reqType;
    
    public Byte getReqType() {
    	if(reqType==null)
    		reqType=RequestType.HEART_BEAT;
		return reqType;
	}
	public void setReqType(Byte reqType) {
		this.reqType = reqType;
	}
	private byte[] body;
    
	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
}
