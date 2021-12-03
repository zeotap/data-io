package com.zeotap.data.io.helpers.beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zeotap.data.io.common.types.DataType$;
import com.zeotap.data.io.common.types.OptionalColumn;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BeamHelpers {

  public static String addOptionalColumnsToSchema(String schema, List<OptionalColumn> columns) {
    Schema oldSchema = parseAvroSchema(schema);
    List<Schema.Field> schemaFields = oldSchema.getFields();
    ArrayList<Schema.Field> newSchemaFields = new ArrayList<>();

    for (Schema.Field f : schemaFields) {
      Schema.Field field = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal());
      newSchemaFields.add(field);
    }

    for (OptionalColumn column : columns) {
      if (oldSchema.getField(column.columnName()) == null) {
        Schema.Field newField = new Schema.Field(column.columnName(), SchemaBuilder.builder().type(column.getStringDataType()), column.columnName(), column.getConvertedValue());
        newSchemaFields.add(newField);
      }
    }

    return Schema.createRecord(oldSchema.getName(), oldSchema.getDoc(), oldSchema.getNamespace(), false, newSchemaFields).toString();
  }

  public static ParDo.SingleOutput<GenericRecord, GenericRecord> addOptionalColumnsToGenericRecord(String oldSchema, String newSchema) {
    return ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws IllegalArgumentException {
        Schema oldAvroSchema = parseAvroSchema(oldSchema);
        Schema newAvroSchema = parseAvroSchema(newSchema);
        GenericRecord oldRecord = context.element();
        GenericRecord newRecord = new GenericData.Record(newAvroSchema);
        List<Schema.Field> newFields = newAvroSchema.getFields();
        int oldFieldsSize = oldAvroSchema.getFields().size();
        for (int index = 0; index < newFields.size(); ++index) {
          Schema.Field field = newFields.get(index);
          Object fieldValue = index < oldFieldsSize ? oldRecord.get(field.name()) : field.defaultVal();
          newRecord.put(field.name(), getConvertedValue(field, fieldValue.toString()));
        }
        context.output(newRecord);
      }
    });
  }

  public static ParDo.SingleOutput<String, GenericRecord> convertStringRowToGenericRecord(String delimiter, String schema) {
    return ParDo.of(new DoFn<String, GenericRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws IllegalArgumentException {
        String value = context.element();
        if (value != null && !value.isEmpty()) {
          String[] rowValues = value.split(delimiter);
          Schema avroSchema = parseAvroSchema(schema);
          GenericRecord genericRecord = new GenericData.Record(avroSchema);
          List<Schema.Field> fields = avroSchema.getFields();
          for (int index = 0; index < fields.size(); ++index) {
            Schema.Field field = fields.get(index);
            String fieldValue = index < rowValues.length ? rowValues[index] : field.defaultVal().toString();
            genericRecord.put(field.name(), getConvertedValue(field, fieldValue));
          }
          context.output(genericRecord);
        }
      }
    });
  }

  public static ParDo.SingleOutput<String, GenericRecord> convertJsonStringRowToGenericRecord(String schema) {
    return ParDo.of(new DoFn<String, GenericRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws IllegalArgumentException, IOException {
        String value = context.element();
        if (value != null && !value.isEmpty()) {
          JsonNode json = new ObjectMapper().readTree(value);
          Schema avroSchema = parseAvroSchema(schema);
          GenericRecord genericRecord = new GenericData.Record(avroSchema);
          List<Schema.Field> fields = avroSchema.getFields();
          for (Schema.Field field : fields) {
            String fieldValue = json.has(field.name()) ? json.get(field.name()).asText() : field.defaultVal().toString();
            genericRecord.put(field.name(), getConvertedValue(field, fieldValue));
          }
          context.output(genericRecord);
        }
      }
    });
  }

  public static ParDo.SingleOutput<GenericRecord, Row> convertGenericRecordToBeamRow() {
    return ParDo.of(new DoFn<GenericRecord, Row>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws IllegalArgumentException {
        GenericRecord record = context.element();
        context.output(AvroUtils.toBeamRowStrict(record, AvroUtils.toBeamSchema(record.getSchema())));
      }
    });
  }

  public static JdbcIO.Read<GenericRecord> readRowAsGenericRecordFromJDBC(String schema, String driver, String url, String user, String password, String query) {
    return JdbcIO.<GenericRecord>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, url)
            .withUsername(user)
            .withPassword(password))
        .withQuery(query)
        .withRowMapper((JdbcIO.RowMapper<GenericRecord>) resultSet -> {
          Schema avroSchema = parseAvroSchema(schema);
          List<Schema.Field> fields = avroSchema.getFields();
          GenericRecord record = new GenericData.Record(avroSchema);
          ResultSetMetaData metadata = resultSet.getMetaData();
          int columnsNumber = metadata.getColumnCount();
          for (int index = 0; index < fields.size(); index++) {
            Schema.Field field = fields.get(index);
            String fieldValue = index < columnsNumber ? resultSet.getString(index + 1) : fields.get(index).defaultVal().toString();
            record.put(field.name(), getConvertedValue(field, fieldValue));
          }
          return record;
        }).withCoder(AvroCoder.of(new Schema.Parser().parse(schema)));
  }

  public static FileIO.Write<Void, GenericRecord> writeGenericRecordToParquet(String schema) {
    return FileIO.<GenericRecord> write().via(ParquetIO.sink(parseAvroSchema(schema)));
  }

  public static ParDo.SingleOutput<GenericRecord, String> convertGenericRecordToDelimitedString(String schema, String delimiter) {
    return ParDo.of(new DoFn<GenericRecord, String>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        GenericRecord record = context.element();
        Schema avroSchema = parseAvroSchema(schema);
        List<Schema.Field> fields = avroSchema.getFields();
        String output = "";
        for (int index = 0; index < fields.size(); index++) {
          Schema.Field field = fields.get(index);
          if (index > 0) {
            output = String.join(delimiter, output, record.get(field.name()).toString());
          } else {
            output = record.get(field.name()).toString();
          }
        }
        context.output(output);
      }
    });
  }

  public static ParDo.SingleOutput<GenericRecord, String> convertGenericRecordToJsonString(String schema) {
    return ParDo.of(new DoFn<GenericRecord, String>() {
      @ProcessElement
      public void processElement(ProcessContext context) throws JsonProcessingException {
        GenericRecord record = context.element();
        Schema avroSchema = parseAvroSchema(schema);
        List<Schema.Field> fields = avroSchema.getFields();
        HashMap<String, Object> outputMap = new HashMap<>();
        for (Schema.Field field : fields) {
          outputMap.put(field.name(), getConvertedValue(field, record.get(field.name()).toString()));
        }
        context.output(new ObjectMapper().writeValueAsString(outputMap));
      }
    });
  }

  public static JdbcIO.Write<GenericRecord> writeGenericRecordToJDBC(String schema, String driver, String url, String user, String password, String tableName) {
    String columnsString = "";
    String valueString = "";
    List<Schema.Field> schemaFields = parseAvroSchema(schema).getFields();
    for (int index = 0; index < schemaFields.size(); index++) {
      if (index > 0) {
        columnsString = String.join(",", columnsString, schemaFields.get(index).name());
        valueString = String.join(",", valueString, "?");
      } else {
        columnsString = schemaFields.get(index).name();
        valueString = "?";
      }
    }
    return JdbcIO.<GenericRecord>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driver, url)
            .withUsername(user)
            .withPassword(password))
        .withStatement(String.format("insert into %s(%s) values(%s)", tableName, columnsString, valueString))
        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<GenericRecord>) (record, query) -> {
          Schema avroSchema = parseAvroSchema(schema);
          List<Schema.Field> fields = avroSchema.getFields();
          for (int index = 0; index < fields.size(); index++) {
            Schema.Field field = fields.get(index);
            setConvertedValueInPreparedStatement(query, field, record.get(field.name()).toString(), index + 1);
          }
        });
  }

  public static Schema parseAvroSchema(String schema) {
    return new Schema.Parser().parse(schema);
  }

  private static Object getConvertedValue(Schema.Field field, String value) {
    return DataType$.MODULE$.convert(field.schema().getType().getName().toLowerCase(), value);
  }

  private static void setConvertedValueInPreparedStatement(PreparedStatement query, Schema.Field field, String value, int index) throws SQLException {
    String type = field.schema().getType().getName().toLowerCase();
    Object convertedValue = DataType$.MODULE$.convert(type, value);
    switch(type) {
      case "string": query.setString(index, convertedValue.toString()); break;
      case "int": query.setInt(index, (int) convertedValue); break;
      case "boolean": query.setBoolean(index, (boolean) convertedValue); break;
      case "byte": query.setByte(index, (byte) convertedValue); break;
      case "short": query.setShort(index, (short) convertedValue); break;
      case "long": query.setLong(index, (long) convertedValue); break;
      case "float": query.setFloat(index, (float) convertedValue); break;
      case "double": query.setDouble(index, (double) convertedValue); break;
      case "date": query.setDate(index, Date.valueOf(convertedValue.toString())); break;
      case "timestamp": query.setTimestamp(index, Timestamp.valueOf(convertedValue.toString())); break;
      default: throw new IllegalArgumentException(String.format("Format %s not supported!", type));
    }
  }

}
