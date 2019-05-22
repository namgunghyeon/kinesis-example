package com.example.kinesis.demo;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

public class Processor implements IRecordProcessor {

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);

    @Override
    public void initialize(InitializationInput initializationInput) {
        System.out.println(initializationInput);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<Record> records = processRecordsInput.getRecords();
        IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

        for (Record r : records) {
            String json = null;
            try {
                json = decoder.decode(r.getData()).toString();
            } catch (CharacterCodingException cce) {
                System.out.println(cce);

            }

            try {
                ObjectNode root = (ObjectNode)mapper.readTree(json);
                System.out.println(root.toString());

            } catch (IOException e) {
                System.out.println(e);
            }

            try {
                checkpointer.checkpoint();
            } catch (ShutdownException se) {
                System.out.println(se);
                break;
            } catch (InvalidStateException e) {
                System.out.println(e);
                break;
            }
        }

        processRecordsInput.getCheckpointer();
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();

        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            System.out.println(se);
        } catch (InvalidStateException e) {
            System.out.println(e);
        }
    }
}
