package com.zeotap.data.io.helpers.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class BeamTestHelpers {

  public static ParDo.SingleOutput<GenericRecord, GenericRecord> printer = ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
      System.out.println(c.element().toString());
      c.output(c.element());
    }
  });

}
