package com.szlabsun.wqimc.ins.thrift;

import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erchashu.thrift.ThreadingClientManager;
import com.szlabsun.wqimc.api.manager.*;
import com.szlabsun.wqimc.api.manager.InstrumentManager.Iface;

public class InstrumentManagerHandler implements Iface {
    private final Logger LOGGER = LoggerFactory.getLogger("InstrumentManager");

    @Override
    public String echo(String data) throws TException {
        LOGGER.debug("echo({})", data);
        return data + "-REPLY";
    }
    
    @Override
    public boolean authenticate(String code) throws TException {
        return false;
    }

    @Override
    public long getSystemTime() throws TException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean upload(Instrument instrument) throws TException {
        UUID id = new UUID(instrument.uuid.getLong(), instrument.uuid.getLong());
        LOGGER.info("upload({},{},{})", instrument.type, instrument.name, id.toString());
         
        // 注册仪器
        ThreadingClientManager threadingClientManager = ThreadingClientManager.getInstance();
        InstrumentContext context = (InstrumentContext)threadingClientManager.getContext();
        if (context != null) {
            context.setId(id);
            context.setClient(threadingClientManager.getClient());
            InstrumentContextManager.getInstance().addContext(id, context);
            
        }
        
        if (instrument.type.equals("PT62-COD"))
            return true;
        else
            return false;
    }

    @Override
    public boolean alarm(Alarm alarm) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateStatus(Status status) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateAction(Status action) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadMeasureData(MeasureData data) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadMeasureCurve(MeasureCurve curve) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadSettingProfiles(List<Profile> profilesList) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateConfigs(List<Config> config) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadReagentProfile(List<ReagentConfigItem> configs) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadConsumableProfile(List<ConsumableConfigItem> configs) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateReagentRemain(List<ReagentRemain> remains) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadSignalProfile(Profile profile) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateSignals(List<Signal> signals) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadOperations(List<OperationSuit> ops) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean uploadDiagnoses(List<DiagnosisSuit> ops) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean notifyDiagnosisResult(DiagnosisResult result) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

}
