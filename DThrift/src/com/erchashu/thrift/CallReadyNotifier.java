package com.erchashu.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.transport.TTransportException;

/**
 * 调用通知器。
 */
public interface CallReadyNotifier {

    /**
     * 请求调用。
     * @param buffer RPC 调用请求的数据帧。
     * @throws TTransportException 
     */
    public void requestCall(ByteBuffer buffer) throws TTransportException;

}