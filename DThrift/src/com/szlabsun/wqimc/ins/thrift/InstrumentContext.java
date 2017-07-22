package com.szlabsun.wqimc.ins.thrift;

import java.util.UUID;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.server.ServerContext;
import com.szlabsun.wqimc.api.agent.InstrumentAgent;

/**
 * 仪器上下文. 
 * 保存仪器相关信息
 */
public class InstrumentContext implements ServerContext {
    protected UUID id;
    protected InstrumentAgent.Client client;

    public InstrumentContext() {
        
    }
    
    public InstrumentContext(UUID id, InstrumentAgent.Client client) {
        this.id = id;
        this.client = client;
    }
    
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public InstrumentAgent.Client getClient() {
        return client;
    }

    public void setClient(TServiceClient client) {
        this.client = (InstrumentAgent.Client) client;
    }
}