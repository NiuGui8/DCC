package com.szlabsun.wqimc.ins.thrift;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;
import com.erchashu.thrift.ClientFactory;
import com.szlabsun.wqimc.api.agent.InstrumentAgent;


public class InstrumentAgentClientFactory extends ClientFactory {
	@Override
	public TServiceClient getClient(TProtocol protocol) {
		return new InstrumentAgent.Client(protocol);
	}

}