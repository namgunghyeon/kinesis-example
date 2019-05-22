package com.example.kinesis.demo;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import org.springframework.stereotype.Component;

@Component
public class ProcessorFactory implements IRecordProcessorFactory {
    public ProcessorFactory() {
        super();
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new Processor();
    }
}
