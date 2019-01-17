/**
 * 
 */
package org.one.remote.consumer;

import org.one.remote.client.RemotingClientHandler;
import org.one.remote.cmd.Command;
import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;

/**
 * @author yangkunguo
 *
 */
public class ConsumerClientHandler extends RemotingClientHandler {

	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		
		Command helloPacket = (Command) packet;
		byte[] body = helloPacket.getBody();
		if (body != null) {
			String str = new String(body, Command.CHARSET);
			System.out.println("收到消息：" + str);
		}
	}

}
