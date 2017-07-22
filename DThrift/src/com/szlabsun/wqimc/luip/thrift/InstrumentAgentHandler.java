package com.szlabsun.wqimc.luip.thrift;

import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.szlabsun.wqimc.api.agent.*;

public class InstrumentAgentHandler implements InstrumentAgent.Iface {
    private final Logger LOGGER = LoggerFactory.getLogger("InstrumentAgent");

    @Override
    public String echo(String data) throws TException {
        LOGGER.debug("echo({})", data);
        return data + "-REPLY";
    }

    @Override
    public boolean upgrade(String type, String newVersion, String url) throws TException {
        LOGGER.info("upgrade(type={}, ver={}, url={})", type, newVersion, url);
        if (newVersion.equals("2.3.4"))
            return true;
        else
            return false;
    }

    @Override
    public List<Signal> getSignals() throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean setSignalUpdateCycle(double cycleSec) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean changeConfigs(List<Config> configs) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean changeSystemTime(long time) throws TException {
        System.out.println(time);
        return false;
    }

    @Override
    public boolean restore() throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reboot() throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(Operation op) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean diagnose(List<Diagnosis> diags) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean stop() throws TException {
        // TODO Auto-generated method stub
        return false;
    }

}
