package com.erchashu.thrift;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;

/**
 * 客户端 Stub 工厂。
 */
public abstract class ClientFactory {
    public abstract TServiceClient getClient(TProtocol protocol);

}