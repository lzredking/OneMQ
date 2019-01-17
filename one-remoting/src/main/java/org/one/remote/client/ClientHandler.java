/**
 * 
 */
package org.one.remote.client;

import java.nio.ByteBuffer;

import org.one.remote.cmd.Command;
import org.tio.core.ChannelContext;
import org.tio.core.GroupContext;
import org.tio.core.exception.AioDecodeException;
import org.tio.core.intf.Packet;

/**
 * @author yangkunguo
 *
 */
public interface ClientHandler {

	public Packet decode(ByteBuffer buffer, int limit, int position, int readableLength, ChannelContext channelContext) throws AioDecodeException;
		
	public ByteBuffer encode(Packet packet, GroupContext groupContext, ChannelContext channelContext) ;
	
	public void handler(Command packet, ChannelContext channelContext) throws Exception;
	
	Command heartbeatPacket();
}
