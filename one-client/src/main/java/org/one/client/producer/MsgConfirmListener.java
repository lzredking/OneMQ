/**
 * 
 */
package org.one.client.producer;

/**如果没有收到返回消息，说明消息发送失败
 * @author yangkunguo
 *
 */
public interface MsgConfirmListener {

	/**消息发送成功回调
	 * @param channelContext
	 * @param _id
	 */
	public void confirm(String _id) ;
	

}
