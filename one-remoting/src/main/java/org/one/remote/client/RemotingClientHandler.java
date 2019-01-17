/**
 * 
 */
package org.one.remote.client;

import java.nio.ByteBuffer;

import org.one.remote.cmd.Command;
import org.one.remote.server.RemotingServerHandler;
import org.tio.client.intf.ClientAioHandler;
import org.tio.core.ChannelContext;
import org.tio.core.GroupContext;
import org.tio.core.exception.AioDecodeException;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class RemotingClientHandler implements ClientAioHandler{

	private Command heartbeatPacket;
	
	@Override
	public Packet decode(ByteBuffer buffer, int limit, int position, int readableLength, ChannelContext channelContext) throws AioDecodeException {
		//提醒：buffer的开始位置并不一定是0，应用需要从buffer.position()开始读取数据
        //收到的数据组不了业务包，则返回null以告诉框架数据不够
        if (readableLength < Command.HEADER_LENGHT) {
            return null;
        }
        //先读get
        byte type = buffer.get();
        //读取消息体的长度
        int bodyLength = buffer.getInt();
        //数据不正确，则抛出AioDecodeException异常
        if (bodyLength < 0) {
            throw new AioDecodeException("bodyLength [" + bodyLength + "] is not right, remote:" + channelContext.getClientNode());
        }
        //计算本次需要的数据长度
        int neededLength = Command.HEADER_LENGHT + bodyLength;
        //收到的数据是否足够组包
        int isDataEnough = readableLength - neededLength;
        // 不够消息体长度(剩下的buffe组不了消息体)
        if (isDataEnough < 0) {
            return null;
        } else {//组包成功
        	Command packet = new Command();
        	packet.setReqType(type);
            if (bodyLength > 0) {
                byte[] dst = new byte[bodyLength];
                buffer.get(dst);
                packet.setBody(dst);
            }
            
            return packet;
        }
	}

	@Override
	public ByteBuffer encode(Packet packet, GroupContext groupContext, ChannelContext channelContext) {
		Command cmd = (Command) packet;
		byte[] body = cmd.getBody();
		int bodyLen = 0;
		if (body != null) {
			bodyLen = body.length;
		}

		//总长度是消息头的长度+消息体的长度
		int allLen = Command.HEADER_LENGHT + bodyLen;

		ByteBuffer buffer = ByteBuffer.allocate(allLen);
		buffer.order(groupContext.getByteOrder());

		//写入消息类型
		buffer.put(cmd.getReqType());
		//写入消息体长度
		buffer.putInt(bodyLen);

		//写入消息体
		if (body != null) {
			buffer.put(body);
		}
		
		
        return buffer;
	}
	
	
	/**
	 * 处理消息
	 */
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command helloPacket = (Command) packet;
		byte[] body = helloPacket.getBody();
		if (body != null) {
			String str = new String(body, Command.CHARSET);
			System.out.println("收到消息：" + str);
		}

		return;
	}

	/**
	 * 此方法如果返回null，框架层面则不会发心跳；如果返回非null，框架层面会定时发本方法返回的消息包
	 */
	@Override
	public Command heartbeatPacket() {
		return heartbeatPacket;
	}

	public void setHeartbeatPacket(Command heartbeatPacket) {
		this.heartbeatPacket = heartbeatPacket;
	}

//	@Override
	public Command heartbeatPacket(ChannelContext channelContext) {
		return heartbeatPacket;
	}

	public Command getHeartbeatPacket() {
		return heartbeatPacket;
	}

}
