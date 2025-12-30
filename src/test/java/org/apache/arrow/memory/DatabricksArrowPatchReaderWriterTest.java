package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test all Arrow supported data types can be read and written with the patched Arrow classes. */
public class DatabricksArrowPatchReaderWriterTest {
  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksArrowPatchReaderWriterTest.class);

  /** Provide different buffer allocators. */
  private static Stream<Arguments> getBufferAllocators() {
    int totalRows = 3_000_000;
    return Stream.of(
        Arguments.of(new RootAllocator(), new RootAllocator(), totalRows),
        Arguments.of(new RootAllocator(), new DatabricksBufferAllocator(), totalRows),
        Arguments.of(new DatabricksBufferAllocator(), new RootAllocator(), totalRows),
        Arguments.of(new DatabricksBufferAllocator(), new DatabricksBufferAllocator(), totalRows));
  }

  /** Test read and write of integer types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testIntegerTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestIntegers();
    byte[] date = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, date, readAllocator);
  }

  /** Test read and write of float types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testFloatTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestFloats();
    byte[] date = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, date, readAllocator);
  }

  /** Test read and write of decimal types . */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testDecimalTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestDecimal();
    byte[] date = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, date, readAllocator);
  }

  /** Test read and write of temporal types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testTemporalTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testTemporal = new TestTemporalTypes();
    byte[] date = writeData(testTemporal, totalRows, writeAllocator);
    readAndValidate(testTemporal, date, readAllocator);
  }

  /** Test read and write of binary types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testBinaryTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testBinary = new TestBinaryTypes();
    byte[] date = writeData(testBinary, totalRows, writeAllocator);
    readAndValidate(testBinary, date, readAllocator);
  }

  /** Test read and write of UTF-8 string types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testUtf8Types(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testUtf8 = new TestUtf8Types();
    byte[] date = writeData(testUtf8, totalRows, writeAllocator);
    readAndValidate(testUtf8, date, readAllocator);
  }

  /** Test read and write of list types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testListTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testList = new TestListTypes();
    byte[] date = writeData(testList, totalRows, writeAllocator);
    readAndValidate(testList, date, readAllocator);
  }

  /** Test read and write of struct types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testStructTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testStruct = new TestStructTypes();
    byte[] date = writeData(testStruct, totalRows, writeAllocator);
    readAndValidate(testStruct, date, readAllocator);
  }

  /** Test read and write of map types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testMapTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testMap = new TestMapTypes();
    byte[] date = writeData(testMap, totalRows, writeAllocator);
    readAndValidate(testMap, date, readAllocator);
  }

  /** Test read and write of union types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testUnionTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testUnion = new TestUnionTypes();
    byte[] date = writeData(testUnion, totalRows, writeAllocator);
    readAndValidate(testUnion, date, readAllocator);
  }

  /** Test read and write of dictionary types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testDictionaryTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testDictionary = new TestDictionaryTypes();
    byte[] date = writeData(testDictionary, totalRows, writeAllocator);
    readAndValidate(testDictionary, date, readAllocator);
  }

  /** Test read and write of run-end encoded types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testRunEndEncodedTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testRunEndEncoded = new TestRunEndEncodedTypes();
    byte[] date = writeData(testRunEndEncoded, totalRows, writeAllocator);
    readAndValidate(testRunEndEncoded, date, readAllocator);
  }

  private byte[] writeData(DataTester dataTester, int totalRowCount, BufferAllocator allocator)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(dataTester.getSchema(), allocator);
    ArrowStreamWriter streamWriter =
        new ArrowStreamWriter(vectorSchemaRoot, null, byteArrayOutputStream);

    streamWriter.start();
    for (int batchSize = 1; batchSize <= totalRowCount; batchSize *= 2) {
      dataTester.writeData(vectorSchemaRoot, batchSize);

      // Write batch.
      vectorSchemaRoot.setRowCount(batchSize);
      streamWriter.writeBatch();
      vectorSchemaRoot.clear();
    }

    streamWriter.end();
    streamWriter.close();
    vectorSchemaRoot.close();

    return byteArrayOutputStream.toByteArray();
  }

  private void readAndValidate(DataTester dataTester, byte[] data, BufferAllocator allocator)
      throws IOException {
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(data), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        logger.info("Validating {} rows", root.getRowCount());
        dataTester.validateData(root);
      }
    }
  }

  private interface DataTester {
    Schema getSchema();

    void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize);

    void validateData(VectorSchemaRoot vectorSchemaRoot);
  }

  /** Test integers */
  private class TestIntegers implements DataTester {
    private final Field signedByteField = newSignedByteIntField();
    private final Field signedShortField = newSignedShortIntField();
    private final Field signedIntField = newSignedIntField();
    private final Field signedLongField = newSignedLongField();
    private final Field unsignedByteField = newUnsignedByteIntField();
    private final Field unsignedShortField = newUnsignedShortIntField();
    private final Field unsignedIntField = newUnsignedIntField();
    private final Field unsignedLongField = newUnsignedLongField();
    private final Schema schema =
        new Schema(
            Arrays.asList(
                signedByteField,
                signedShortField,
                signedIntField,
                signedLongField,
                unsignedByteField,
                unsignedShortField,
                unsignedIntField,
                unsignedLongField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      TinyIntVector signedByteInt =
          (TinyIntVector) vectorSchemaRoot.getVector(signedByteField.getName());
      SmallIntVector signedShortInt =
          (SmallIntVector) vectorSchemaRoot.getVector(signedShortField.getName());
      IntVector signedInt = (IntVector) vectorSchemaRoot.getVector(signedIntField.getName());
      BigIntVector signedLong =
          (BigIntVector) vectorSchemaRoot.getVector(signedLongField.getName());
      UInt1Vector unsignedByteInt =
          (UInt1Vector) vectorSchemaRoot.getVector(unsignedByteField.getName());
      UInt2Vector unsignedShortInt =
          (UInt2Vector) vectorSchemaRoot.getVector(unsignedShortField.getName());
      UInt4Vector unsignedInt =
          (UInt4Vector) vectorSchemaRoot.getVector(unsignedIntField.getName());
      UInt8Vector unsignedLong =
          (UInt8Vector) vectorSchemaRoot.getVector(unsignedLongField.getName());
      // Set signed bytes.
      signedByteInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedByteInt.setNull(i);
        } else {
          signedByteInt.set(i, getSignedByte(i));
        }
      }

      // Set signed shorts.
      signedShortInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedShortInt.setNull(i);
        } else {
          signedShortInt.set(i, getSignedShort(i));
        }
      }

      // Set signed ints.
      signedInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedInt.setNull(i);
        } else {
          signedInt.set(i, getSignedInt(i));
        }
      }

      // Set signed longs.
      signedLong.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedLong.setNull(i);
        } else {
          signedLong.set(i, getSignedLong(i));
        }
      }

      // Set unsigned bytes.
      unsignedByteInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedByteInt.setNull(i);
        } else {
          unsignedByteInt.set(i, getUnsignedByte(i));
        }
      }

      // Set unsigned shorts.
      unsignedShortInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedShortInt.setNull(i);
        } else {
          unsignedShortInt.set(i, getUnsignedShort(i));
        }
      }

      // Set unsigned ints.
      unsignedInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedInt.setNull(i);
        } else {
          unsignedInt.set(i, getUnsignedInt(i));
        }
      }

      // Set unsigned longs.
      unsignedLong.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedLong.setNull(i);
        } else {
          unsignedLong.set(i, getUnsignedLong(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      TinyIntVector signedByteInt =
          (TinyIntVector) vectorSchemaRoot.getVector(signedByteField.getName());
      SmallIntVector signedShortInt =
          (SmallIntVector) vectorSchemaRoot.getVector(signedShortField.getName());
      IntVector signedInt = (IntVector) vectorSchemaRoot.getVector(signedIntField.getName());
      BigIntVector signedLong =
          (BigIntVector) vectorSchemaRoot.getVector(signedLongField.getName());
      UInt1Vector unsignedByteInt =
          (UInt1Vector) vectorSchemaRoot.getVector(unsignedByteField.getName());
      UInt2Vector unsignedShortInt =
          (UInt2Vector) vectorSchemaRoot.getVector(unsignedShortField.getName());
      UInt4Vector unsignedInt =
          (UInt4Vector) vectorSchemaRoot.getVector(unsignedIntField.getName());
      UInt8Vector unsignedLong =
          (UInt8Vector) vectorSchemaRoot.getVector(unsignedLongField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();
      // Validate all rows
      for (int i = 0; i < rowCount; i++) {
        // Validate signed byte
        if (i % 2 == 0) {
          assertTrue(signedByteInt.isNull(i), "Signed byte should be null at index " + i);
        } else {
          assertEquals(
              getSignedByte(i), signedByteInt.get(i), "Signed byte mismatch at index " + i);
        }

        // Validate signed short
        if (i % 2 == 0) {
          assertTrue(signedShortInt.isNull(i), "Signed short should be null at index " + i);
        } else {
          assertEquals(
              getSignedShort(i), signedShortInt.get(i), "Signed short mismatch at index " + i);
        }

        // Validate signed int
        if (i % 2 == 0) {
          assertTrue(signedInt.isNull(i), "Signed int should be null at index " + i);
        } else {
          assertEquals(getSignedInt(i), signedInt.get(i), "Signed int mismatch at index " + i);
        }

        // Validate signed long
        if (i % 2 == 0) {
          assertTrue(signedLong.isNull(i), "Signed long should be null at index " + i);
        } else {
          assertEquals(getSignedLong(i), signedLong.get(i), "Signed long mismatch at index " + i);
        }

        // Validate unsigned byte (convert to unsigned using Byte.toUnsignedInt)
        if (i % 2 == 0) {
          assertTrue(unsignedByteInt.isNull(i), "Unsigned byte should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedByte(i),
              Byte.toUnsignedInt(unsignedByteInt.get(i)),
              "Unsigned byte mismatch at index " + i);
        }

        // Validate unsigned short (char is already unsigned in Java)
        if (i % 2 == 0) {
          assertTrue(unsignedShortInt.isNull(i), "Unsigned short should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedShort(i),
              unsignedShortInt.get(i),
              "Unsigned short mismatch at index " + i);
        }

        // Validate unsigned int (convert to unsigned long for comparison)
        if (i % 2 == 0) {
          assertTrue(unsignedInt.isNull(i), "Unsigned int should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedInt(i), unsignedInt.get(i), "Unsigned int mismatch at index " + i);
        }

        // Validate unsigned long
        if (i % 2 == 0) {
          assertTrue(unsignedLong.isNull(i), "Unsigned long should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedLong(i), unsignedLong.get(i), "Unsigned long mismatch at index " + i);
        }
      }
    }
  }

  /** Test floats */
  private class TestFloats implements DataTester {
    Field floatField = newFloatField();
    Field doubleField = newDoubleField();
    Schema schema = new Schema(Arrays.asList(floatField, doubleField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      Float4Vector floatVector = (Float4Vector) vectorSchemaRoot.getVector(floatField.getName());
      Float8Vector doubleVector = (Float8Vector) vectorSchemaRoot.getVector(doubleField.getName());

      // Set floats.
      floatVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          floatVector.setNull(i);
        } else {
          floatVector.set(i, getFloat(i));
        }
      }

      // Set doubles.
      doubleVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          doubleVector.setNull(i);
        } else {
          doubleVector.set(i, getDouble(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      try (Float4Vector floatVector =
              (Float4Vector) vectorSchemaRoot.getVector(floatField.getName());
          Float8Vector doubleVector =
              (Float8Vector) vectorSchemaRoot.getVector(doubleField.getName())) {
        for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
          // Validate float
          if (i % 2 == 0) {
            assertTrue(floatVector.isNull(i), "Float should be null at index " + i);
          } else {
            assertEquals(getFloat(i), floatVector.get(i), 0.0001f, "Float mismatch at index " + i);
          }

          // Validate double
          if (i % 2 == 0) {
            assertTrue(doubleVector.isNull(i), "Double should be null at index " + i);
          } else {
            assertEquals(
                getDouble(i), doubleVector.get(i), 0.0001, "Double mismatch at index " + i);
          }
        }
      }
    }
  }

  /** Test decimals */
  private class TestDecimal implements DataTester {
    private final Field decimalField;
    private final Schema schema;

    TestDecimal() {
      this(16, 0, 128);
    }

    TestDecimal(int precision, int scale, int bitWidth) {
      decimalField = newDecimalField(precision, scale, bitWidth);
      //noinspection ArraysAsListWithZeroOrOneArgument
      schema = new Schema(Arrays.asList(decimalField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      DecimalVector decimalVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalField.getName());

      // Set decimals.
      decimalVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          decimalVector.setNull(i);
        } else {
          decimalVector.set(i, getDecimal(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      DecimalVector decimalVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        // Validate decimal
        BigDecimal bigDecimal = decimalVector.getObject(i);
        if (i % 2 == 0) {
          assertNull(bigDecimal, "Decimal should be null at index " + i);
        } else {
          assertNotNull(bigDecimal, "Decimal should not be null at index " + i);
          assertEquals(getDecimal(i), bigDecimal.longValue(), "Decimal mismatch at index " + i);
        }
      }
    }
  }

  /** Test temporal types */
  private class TestTemporalTypes implements DataTester {
    private final Field dateDayField;
    private final Field timestampField;
    private final Field timeSecField;
    private final Field durationSecField;
    private final Field intervalYearField;
    private final Field intervalDayField;
    private final Schema schema;

    TestTemporalTypes() {
      dateDayField = newDateDayField();
      timestampField = newTimestampMilliField();
      timeSecField = newTimeSecField();
      durationSecField = newDurationSecField();
      intervalYearField = newIntervalYearField();
      intervalDayField = newIntervalDayField();
      schema =
          new Schema(
              Arrays.asList(
                  dateDayField,
                  timestampField,
                  timeSecField,
                  durationSecField,
                  intervalYearField,
                  intervalDayField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      DateDayVector dateDayVector =
          (DateDayVector) vectorSchemaRoot.getVector(dateDayField.getName());
      TimeStampMilliTZVector timestampVector =
          (TimeStampMilliTZVector) vectorSchemaRoot.getVector(timestampField.getName());
      TimeSecVector timeSecVector =
          (TimeSecVector) vectorSchemaRoot.getVector(timeSecField.getName());
      DurationVector durationVector =
          (DurationVector) vectorSchemaRoot.getVector(durationSecField.getName());
      IntervalYearVector intervalYearVector =
          (IntervalYearVector) vectorSchemaRoot.getVector(intervalYearField.getName());
      IntervalDayVector intervalDayVector =
          (IntervalDayVector) vectorSchemaRoot.getVector(intervalDayField.getName());

      // Set dates (days since epoch).
      dateDayVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dateDayVector.setNull(i);
        } else {
          dateDayVector.set(i, getDateDay(i));
        }
      }

      // Set timestamps (milliseconds since epoch).
      timestampVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timestampVector.setNull(i);
        } else {
          timestampVector.set(i, getTimestampMilli(i));
        }
      }

      // Set times (seconds since midnight).
      timeSecVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timeSecVector.setNull(i);
        } else {
          timeSecVector.set(i, getTimeSec(i));
        }
      }

      // Set durations (seconds).
      durationVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          durationVector.setNull(i);
        } else {
          durationVector.set(i, getDurationSec(i));
        }
      }

      // Set interval year-months.
      intervalYearVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          intervalYearVector.setNull(i);
        } else {
          intervalYearVector.set(i, getIntervalYearMonth(i));
        }
      }

      // Set interval day-times.
      intervalDayVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          intervalDayVector.setNull(i);
        } else {
          intervalDayVector.set(i, getIntervalDayDays(i), getIntervalDayMillis(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      DateDayVector dateDayVector =
          (DateDayVector) vectorSchemaRoot.getVector(dateDayField.getName());
      TimeStampMilliTZVector timestampVector =
          (TimeStampMilliTZVector) vectorSchemaRoot.getVector(timestampField.getName());
      TimeSecVector timeSecVector =
          (TimeSecVector) vectorSchemaRoot.getVector(timeSecField.getName());
      DurationVector durationVector =
          (DurationVector) vectorSchemaRoot.getVector(durationSecField.getName());
      IntervalYearVector intervalYearVector =
          (IntervalYearVector) vectorSchemaRoot.getVector(intervalYearField.getName());
      IntervalDayVector intervalDayVector =
          (IntervalDayVector) vectorSchemaRoot.getVector(intervalDayField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate date (days since epoch)
        if (i % 2 == 0) {
          assertTrue(dateDayVector.isNull(i), "Date should be null at index " + i);
        } else {
          assertEquals(getDateDay(i), dateDayVector.get(i), "Date mismatch at index " + i);
        }

        // Validate timestamp (milliseconds since epoch)
        if (i % 2 == 0) {
          assertTrue(timestampVector.isNull(i), "Timestamp should be null at index " + i);
        } else {
          assertEquals(
              getTimestampMilli(i), timestampVector.get(i), "Timestamp mismatch at index " + i);
        }

        // Validate time (seconds since midnight)
        if (i % 2 == 0) {
          assertTrue(timeSecVector.isNull(i), "Time should be null at index " + i);
        } else {
          assertEquals(getTimeSec(i), timeSecVector.get(i), "Time mismatch at index " + i);
        }

        // Validate duration (seconds)
        if (i % 2 == 0) {
          assertTrue(durationVector.isNull(i), "Duration should be null at index " + i);
        } else {
          Duration durationValue = durationVector.getObject(i);
          assertNotNull(durationValue, "Duration should not be null at index " + i);
          assertEquals(
              getDurationSec(i), durationValue.getSeconds(), "Duration mismatch at index " + i);
        }

        // Validate interval year-month
        if (i % 2 == 0) {
          assertTrue(
              intervalYearVector.isNull(i), "Interval year-month should be null at index " + i);
        } else {
          assertEquals(
              getIntervalYearMonth(i),
              intervalYearVector.get(i),
              "Interval year-month mismatch at index " + i);
        }

        // Validate interval day-time
        if (i % 2 == 0) {
          assertTrue(intervalDayVector.isNull(i), "Interval day-time should be null at index " + i);
        } else {
          NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
          intervalDayVector.get(i, holder);
          assertEquals(getIntervalDayDays(i), holder.days, "Interval days mismatch at index " + i);
          assertEquals(
              getIntervalDayMillis(i),
              holder.milliseconds,
              "Interval milliseconds mismatch at index " + i);
        }
      }
    }
  }

  /** Test UTF-8 string types */
  private class TestUtf8Types implements DataTester {
    private final Field utf8Field;
    private final Field largeUtf8Field;
    private final Schema schema;

    TestUtf8Types() {
      utf8Field = newUtf8Field();
      largeUtf8Field = newLargeUtf8Field();
      schema = new Schema(Arrays.asList(utf8Field, largeUtf8Field));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      VarCharVector utf8Vector = (VarCharVector) vectorSchemaRoot.getVector(utf8Field.getName());
      LargeVarCharVector largeUtf8Vector =
          (LargeVarCharVector) vectorSchemaRoot.getVector(largeUtf8Field.getName());

      // Set UTF-8 strings.
      utf8Vector.allocateNew(batchSize * 50L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          utf8Vector.setNull(i);
        } else {
          utf8Vector.set(i, getUtf8String(i).getBytes(StandardCharsets.UTF_8));
        }
      }

      // Set large UTF-8 strings.
      largeUtf8Vector.clear();
      largeUtf8Vector.allocateNew(batchSize * 100L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          largeUtf8Vector.setNull(i);
        } else {
          largeUtf8Vector.set(i, getLargeUtf8String(i).getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      VarCharVector utf8Vector = (VarCharVector) vectorSchemaRoot.getVector(utf8Field.getName());
      LargeVarCharVector largeUtf8Vector =
          (LargeVarCharVector) vectorSchemaRoot.getVector(largeUtf8Field.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate UTF-8 string
        if (i % 2 == 0) {
          assertTrue(utf8Vector.isNull(i), "UTF-8 string should be null at index " + i);
        } else {
          String expected = getUtf8String(i);
          byte[] actualBytes = utf8Vector.get(i);
          assertNotNull(actualBytes, "UTF-8 string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "UTF-8 string mismatch at index " + i);
        }

        // Validate large UTF-8 string
        if (i % 2 == 0) {
          assertTrue(largeUtf8Vector.isNull(i), "Large UTF-8 string should be null at index " + i);
        } else {
          String expected = getLargeUtf8String(i);
          byte[] actualBytes = largeUtf8Vector.get(i);
          assertNotNull(actualBytes, "Large UTF-8 string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "Large UTF-8 string mismatch at index " + i);
        }
      }
    }
  }

  /** Test list types */
  private class TestListTypes implements DataTester {
    private final Field listIntField;
    private final Field largeListIntField;
    private final Schema schema;

    TestListTypes() {
      listIntField = newListIntField();
      largeListIntField = newLargeListIntField();
      schema = new Schema(Arrays.asList(listIntField, largeListIntField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      ListVector listIntVector = (ListVector) vectorSchemaRoot.getVector(listIntField.getName());
      LargeListVector largeListIntVector =
          (LargeListVector) vectorSchemaRoot.getVector(largeListIntField.getName());

      // Set list of integers.
      listIntVector.allocateNew();
      UnionListWriter listWriter = listIntVector.getWriter();
      for (int i = 0; i < batchSize; i++) {
        listWriter.setPosition(i);
        if (i % 2 == 0) {
          // Null list
          listWriter.writeNull();
        } else {
          // Write list with varying number of elements
          listWriter.startList();
          int listSize = getListSize(i);
          for (int j = 0; j < listSize; j++) {
            listWriter.integer().writeInt(getListElement(i, j));
          }
          listWriter.endList();
        }
      }
      listWriter.setValueCount(batchSize);

      // Set large list of integers.
      largeListIntVector.allocateNew();
      UnionLargeListWriter largeListWriter = largeListIntVector.getWriter();
      for (int i = 0; i < batchSize; i++) {
        largeListWriter.setPosition(i);
        if (i % 2 == 0) {
          // Null list
          largeListWriter.writeNull();
        } else {
          // Write list with varying number of elements
          largeListWriter.startList();
          int listSize = getLargeListSize(i);
          for (int j = 0; j < listSize; j++) {
            largeListWriter.integer().writeInt(getLargeListElement(i, j));
          }
          largeListWriter.endList();
        }
      }
      largeListWriter.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      ListVector listIntVector = (ListVector) vectorSchemaRoot.getVector(listIntField.getName());
      LargeListVector largeListIntVector =
          (LargeListVector) vectorSchemaRoot.getVector(largeListIntField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate list of integers
        if (i % 2 == 0) {
          assertTrue(listIntVector.isNull(i), "List should be null at index " + i);
        } else {
          Object listObj = listIntVector.getObject(i);
          assertNotNull(listObj, "List should not be null at index " + i);
          int expectedSize = getListSize(i);
          @SuppressWarnings("unchecked")
          java.util.List<Integer> list = (java.util.List<Integer>) listObj;
          assertEquals(expectedSize, list.size(), "List size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            Integer element = list.get(j);
            assertEquals(
                getListElement(i, j),
                element,
                "List element mismatch at index " + i + "[" + j + "]");
          }
        }

        // Validate large list of integers
        if (i % 2 == 0) {
          assertTrue(largeListIntVector.isNull(i), "Large list should be null at index " + i);
        } else {
          Object listObj = largeListIntVector.getObject(i);
          assertNotNull(listObj, "Large list should not be null at index " + i);
          int expectedSize = getLargeListSize(i);
          @SuppressWarnings("unchecked")
          java.util.List<Integer> list = (java.util.List<Integer>) listObj;
          assertEquals(expectedSize, list.size(), "Large list size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            Integer element = list.get(j);
            assertEquals(
                getLargeListElement(i, j),
                element,
                "Large list element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }
  }

  /** Test struct types */
  private class TestStructTypes implements DataTester {
    private final Field structField;
    private final Schema schema;
    private final int VAR_BINARY_MAX_LENGTH = 30;

    TestStructTypes() {
      structField = newStructField();
      //noinspection ArraysAsListWithZeroOrOneArgument
      schema = new Schema(Arrays.asList(structField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      StructVector structVector = (StructVector) vectorSchemaRoot.getVector(structField.getName());

      // Allocate struct vector
      structVector.allocateNew();

      // Get child vectors
      IntVector structIntVector =
          structVector.addOrGet(
              "s_int", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      BigIntVector structLongVector =
          structVector.addOrGet(
              "s_long", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      Float4Vector structFloatVector =
          structVector.addOrGet(
              "s_float",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
              Float4Vector.class);
      Float8Vector structDoubleVector =
          structVector.addOrGet(
              "s_double",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Float8Vector.class);
      DecimalVector structDecimalVector =
          structVector.addOrGet(
              "s_decimal",
              FieldType.nullable(new ArrowType.Decimal(16, 0, 128)),
              DecimalVector.class);
      DateDayVector structDateVector =
          structVector.addOrGet(
              "s_date",
              FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
              DateDayVector.class);
      TimeStampMilliTZVector structTimestampVector =
          structVector.addOrGet(
              "s_timestamp",
              FieldType.nullable(
                  new ArrowType.Timestamp(
                      org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
              TimeStampMilliTZVector.class);
      DurationVector structDurationVector =
          structVector.addOrGet(
              "s_duration",
              FieldType.nullable(
                  new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)),
              DurationVector.class);
      IntervalYearVector structIntervalVector =
          structVector.addOrGet(
              "s_interval",
              FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
              IntervalYearVector.class);
      VarBinaryVector structBinaryVector =
          structVector.addOrGet(
              "s_binary", FieldType.nullable(new ArrowType.Binary()), VarBinaryVector.class);
      VarCharVector structUtf8Vector =
          structVector.addOrGet(
              "s_utf8", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      ListVector structListVector =
          structVector.addOrGet(
              "s_list", FieldType.nullable(ArrowType.List.INSTANCE), ListVector.class);

      // Allocate child vectors
      structIntVector.allocateNew(batchSize);
      structLongVector.allocateNew(batchSize);
      structFloatVector.allocateNew(batchSize);
      structDoubleVector.allocateNew(batchSize);
      structDecimalVector.allocateNew(batchSize);
      structDateVector.allocateNew(batchSize);
      structTimestampVector.allocateNew(batchSize);
      structDurationVector.allocateNew(batchSize);
      structIntervalVector.allocateNew(batchSize);
      structBinaryVector.allocateNew(batchSize * 20L, batchSize);
      structUtf8Vector.allocateNew(batchSize * 50L, batchSize);
      structListVector.allocateNew();

      // Set struct values
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          structVector.setNull(i);
        } else {
          structVector.setIndexDefined(i);
          structIntVector.set(i, getSignedInt(i));
          structLongVector.set(i, getSignedLong(i));
          structFloatVector.set(i, getFloat(i));
          structDoubleVector.set(i, getDouble(i));
          structDecimalVector.set(i, getDecimal(i));
          structDateVector.set(i, getDateDay(i));
          structTimestampVector.set(i, getTimestampMilli(i));
          structDurationVector.set(i, getDurationSec(i));
          structIntervalVector.set(i, getIntervalYearMonth(i));
          structBinaryVector.set(i, getVarBinary(i, VAR_BINARY_MAX_LENGTH));
          structUtf8Vector.set(i, getUtf8String(i).getBytes(StandardCharsets.UTF_8));

          // Set list in struct
          UnionListWriter listWriter = structListVector.getWriter();
          listWriter.setPosition(i);
          listWriter.startList();
          int listSize = getListSize(i);
          for (int j = 0; j < listSize; j++) {
            listWriter.integer().writeInt(getListElement(i, j));
          }
          listWriter.endList();
        }
      }
      structListVector.getWriter().setValueCount(batchSize);
      structVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      StructVector structVector = (StructVector) vectorSchemaRoot.getVector(structField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate struct
        if (i % 2 == 0) {
          assertTrue(structVector.isNull(i), "Struct should be null at index " + i);
        } else {
          java.util.Map<String, ?> structMap = structVector.getObject(i);
          assertNotNull(structMap, "Struct should not be null at index " + i);

          // Validate int
          assertEquals(
              getSignedInt(i),
              ((Integer) structMap.get("s_int")).intValue(),
              "Struct int mismatch at index " + i);

          // Validate long
          assertEquals(
              getSignedLong(i),
              ((Long) structMap.get("s_long")).longValue(),
              "Struct long mismatch at index " + i);

          // Validate float
          assertEquals(
              getFloat(i),
              (Float) structMap.get("s_float"),
              0.0001f,
              "Struct float mismatch at index " + i);

          // Validate double
          assertEquals(
              getDouble(i),
              (Double) structMap.get("s_double"),
              0.0001,
              "Struct double mismatch at index " + i);

          // Validate decimal
          BigDecimal decimalValue = (BigDecimal) structMap.get("s_decimal");
          assertEquals(
              getDecimal(i), decimalValue.longValue(), "Struct decimal mismatch at index " + i);

          // Validate date
          assertEquals(
              getDateDay(i),
              ((Integer) structMap.get("s_date")).intValue(),
              "Struct date mismatch at index " + i);

          // Validate timestamp
          assertEquals(
              getTimestampMilli(i),
              ((Long) structMap.get("s_timestamp")).longValue(),
              "Struct timestamp mismatch at index " + i);

          // Validate duration
          Duration durationValue = (Duration) structMap.get("s_duration");
          assertEquals(
              getDurationSec(i),
              durationValue.getSeconds(),
              "Struct duration mismatch at index " + i);

          // Validate interval
          assertEquals(
              getIntervalYearMonth(i),
              ((Period) structMap.get("s_interval")).getMonths(),
              "Struct interval mismatch at index " + i);

          // Validate binary
          byte[] binaryValue = (byte[]) structMap.get("s_binary");
          assertArrayEquals(
              getVarBinary(i, VAR_BINARY_MAX_LENGTH),
              binaryValue,
              "Struct binary mismatch at index " + i);

          // Validate utf8
          String utf8Value =
              new String(((Text) structMap.get("s_utf8")).getBytes(), StandardCharsets.UTF_8);
          assertEquals(getUtf8String(i), utf8Value, "Struct utf8 mismatch at index " + i);

          // Validate list
          @SuppressWarnings("unchecked")
          List<Integer> listValue = (List<Integer>) structMap.get("s_list");
          int expectedSize = getListSize(i);
          assertEquals(expectedSize, listValue.size(), "Struct list size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            assertEquals(
                getListElement(i, j),
                listValue.get(j),
                "Struct list element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }

    private Field newStructField() {
      return new Field(
          "struct-all-types",
          FieldType.nullable(ArrowType.Struct.INSTANCE),
          Arrays.asList(
              new Field("s_int", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("s_long", FieldType.nullable(new ArrowType.Int(64, true)), null),
              new Field(
                  "s_float",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null),
              new Field(
                  "s_double",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  null),
              new Field("s_decimal", FieldType.nullable(new ArrowType.Decimal(16, 0, 128)), null),
              new Field(
                  "s_date",
                  FieldType.nullable(
                      new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                  null),
              new Field(
                  "s_timestamp",
                  FieldType.nullable(
                      new ArrowType.Timestamp(
                          org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
                  null),
              new Field(
                  "s_duration",
                  FieldType.nullable(
                      new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)),
                  null),
              new Field(
                  "s_interval",
                  FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                  null),
              new Field("s_binary", FieldType.nullable(new ArrowType.Binary()), null),
              new Field("s_utf8", FieldType.nullable(new ArrowType.Utf8()), null),
              new Field(
                  "s_list",
                  FieldType.nullable(ArrowType.List.INSTANCE),
                  java.util.Collections.singletonList(
                      new Field(
                          "$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
    }
  }

  /** Test map types with all value types from struct test */
  private class TestMapTypes implements DataTester {
    private final Field mapStringIntField;
    private final Field mapStringLongField;
    private final Field mapStringFloatField;
    private final Field mapStringDoubleField;
    private final Field mapStringDecimalField;
    private final Field mapStringDateField;
    private final Field mapStringTimestampField;
    private final Field mapStringDurationField;
    private final Field mapStringIntervalField;
    private final Field mapStringBinaryField;
    private final Field mapStringUtf8Field;
    private final Field mapStringListField;
    private final Schema schema;
    private final int VAR_BINARY_MAX_LENGTH = 32;

    TestMapTypes() {
      mapStringIntField = newMapStringIntField();
      mapStringLongField = newMapStringLongField();
      mapStringFloatField = newMapStringFloatField();
      mapStringDoubleField = newMapStringDoubleField();
      mapStringDecimalField = newMapStringDecimalField();
      mapStringDateField = newMapStringDateField();
      mapStringTimestampField = newMapStringTimestampField();
      mapStringDurationField = newMapStringDurationField();
      mapStringIntervalField = newMapStringIntervalField();
      mapStringBinaryField = newMapStringBinaryField();
      mapStringUtf8Field = newMapStringUtf8Field();
      mapStringListField = newMapStringListField();
      schema =
          new Schema(
              Arrays.asList(
                  mapStringIntField,
                  mapStringLongField,
                  mapStringFloatField,
                  mapStringDoubleField,
                  mapStringDecimalField,
                  mapStringDateField,
                  mapStringTimestampField,
                  mapStringDurationField,
                  mapStringIntervalField,
                  mapStringBinaryField,
                  mapStringUtf8Field,
                  mapStringListField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntField.getName()), batchSize, "int");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringLongField.getName()), batchSize, "long");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringFloatField.getName()),
          batchSize,
          "float");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDoubleField.getName()),
          batchSize,
          "double");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDecimalField.getName()),
          batchSize,
          "decimal");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDateField.getName()), batchSize, "date");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringTimestampField.getName()),
          batchSize,
          "timestamp");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDurationField.getName()),
          batchSize,
          "duration");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntervalField.getName()),
          batchSize,
          "interval");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringBinaryField.getName()),
          batchSize,
          "binary");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringUtf8Field.getName()), batchSize, "utf8");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringListField.getName()), batchSize, "list");
    }

    private void writeMapVector(MapVector mapVector, int batchSize, String valueType) {
      mapVector.allocateNew();
      StructVector structVector = (StructVector) mapVector.getDataVector();

      VarCharVector keyVector =
          structVector.addOrGet(
              "key", FieldType.notNullable(new ArrowType.Utf8()), VarCharVector.class);

      int maxMapEntries = (batchSize / 2) * 3 + 10;
      keyVector.allocateNew((long) batchSize * 30, maxMapEntries);

      switch (valueType) {
        case "int":
          {
            IntVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getSignedInt(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "long":
          {
            BigIntVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getSignedLong(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "float":
          {
            Float4Vector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    Float4Vector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getFloat(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "double":
          {
            Float8Vector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    Float8Vector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDouble(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "decimal":
          {
            DecimalVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Decimal(16, 0, 128)),
                    DecimalVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDecimal(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "date":
          {
            DateDayVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(
                        new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                    DateDayVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDateDay(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "timestamp":
          {
            TimeStampMilliTZVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(
                        new ArrowType.Timestamp(
                            org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
                    TimeStampMilliTZVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getTimestampMilli(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "duration":
          {
            DurationVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(
                        new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)),
                    DurationVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDurationSec(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "interval":
          {
            IntervalYearVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                    IntervalYearVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getIntervalYearMonth(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "binary":
          {
            VarBinaryVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Binary()), VarBinaryVector.class);
            valueVector.allocateNew((long) batchSize * 20, maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getVarBinary(i * 10 + j, VAR_BINARY_MAX_LENGTH));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "utf8":
          {
            VarCharVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
            valueVector.allocateNew((long) batchSize * 50, maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(
                      start + j, getUtf8String(i * 10 + j).getBytes(StandardCharsets.UTF_8));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "list":
          {
            ListVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(ArrowType.List.INSTANCE), ListVector.class);
            valueVector.allocateNew();
            UnionListWriter listWriter = valueVector.getWriter();

            int entryIndex = 0;
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));

                  listWriter.setPosition(start + j);
                  listWriter.startList();
                  int listSize = getListSize(i * 10 + j);
                  for (int k = 0; k < listSize; k++) {
                    listWriter.integer().writeInt(getListElement(i * 10 + j, k));
                  }
                  listWriter.endList();
                  entryIndex++;
                }
                mapVector.endValue(i, mapSize);
              }
            }
            listWriter.setValueCount(entryIndex);
            break;
          }
      }
      mapVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntField.getName()),
          vectorSchemaRoot.getRowCount(),
          "int");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringLongField.getName()),
          vectorSchemaRoot.getRowCount(),
          "long");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringFloatField.getName()),
          vectorSchemaRoot.getRowCount(),
          "float");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDoubleField.getName()),
          vectorSchemaRoot.getRowCount(),
          "double");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDecimalField.getName()),
          vectorSchemaRoot.getRowCount(),
          "decimal");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDateField.getName()),
          vectorSchemaRoot.getRowCount(),
          "date");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringTimestampField.getName()),
          vectorSchemaRoot.getRowCount(),
          "timestamp");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDurationField.getName()),
          vectorSchemaRoot.getRowCount(),
          "duration");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntervalField.getName()),
          vectorSchemaRoot.getRowCount(),
          "interval");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringBinaryField.getName()),
          vectorSchemaRoot.getRowCount(),
          "binary");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringUtf8Field.getName()),
          vectorSchemaRoot.getRowCount(),
          "utf8");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringListField.getName()),
          vectorSchemaRoot.getRowCount(),
          "list");
    }

    @SuppressWarnings("unchecked")
    private void validateMapVector(MapVector mapVector, int rowCount, String valueType) {
      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(
              mapVector.isNull(i), "Map<String, " + valueType + "> should be null at index " + i);
        } else {
          List<?> mapList = mapVector.getObject(i);
          assertNotNull(mapList, "Map<String, " + valueType + "> should not be null at index " + i);
          int expectedSize = getMapSize(i);
          assertEquals(
              expectedSize,
              mapList.size(),
              "Map<String, " + valueType + "> size mismatch at index " + i);

          for (int j = 0; j < expectedSize; j++) {
            java.util.Map<String, ?> entry = (java.util.Map<String, ?>) mapList.get(j);
            String expectedKey = getMapStringKey(i, j);
            assertEquals(
                expectedKey,
                new String(((Text) entry.get("key")).getBytes(), StandardCharsets.UTF_8),
                "Map key mismatch at index " + i + "[" + j + "]");

            Object value = entry.get("value");
            switch (valueType) {
              case "int":
                assertEquals(
                    getSignedInt(i * 10 + j),
                    ((Integer) value).intValue(),
                    "Map int value mismatch at index " + i + "[" + j + "]");
                break;
              case "long":
                assertEquals(
                    getSignedLong(i * 10 + j),
                    ((Long) value).longValue(),
                    "Map long value mismatch at index " + i + "[" + j + "]");
                break;
              case "float":
                assertEquals(
                    getFloat(i * 10 + j),
                    (Float) value,
                    0.0001f,
                    "Map float value mismatch at index " + i + "[" + j + "]");
                break;
              case "double":
                assertEquals(
                    getDouble(i * 10 + j),
                    (Double) value,
                    0.0001,
                    "Map double value mismatch at index " + i + "[" + j + "]");
                break;
              case "decimal":
                assertEquals(
                    getDecimal(i * 10 + j),
                    ((BigDecimal) value).longValue(),
                    "Map decimal value mismatch at index " + i + "[" + j + "]");
                break;
              case "date":
                assertEquals(
                    getDateDay(i * 10 + j),
                    ((Integer) value).intValue(),
                    "Map date value mismatch at index " + i + "[" + j + "]");
                break;
              case "timestamp":
                assertEquals(
                    getTimestampMilli(i * 10 + j),
                    ((Long) value).longValue(),
                    "Map timestamp value mismatch at index " + i + "[" + j + "]");
                break;
              case "duration":
                assertEquals(
                    getDurationSec(i * 10 + j),
                    ((Duration) value).getSeconds(),
                    "Map duration value mismatch at index " + i + "[" + j + "]");
                break;
              case "interval":
                assertEquals(
                    getIntervalYearMonth(i * 10 + j),
                    ((Period) value).getMonths(),
                    "Map interval value mismatch at index " + i + "[" + j + "]");
                break;
              case "binary":
                assertArrayEquals(
                    getVarBinary(i * 10 + j, VAR_BINARY_MAX_LENGTH),
                    (byte[]) value,
                    "Map binary value mismatch at index " + i + "[" + j + "]");
                break;
              case "utf8":
                assertEquals(
                    getUtf8String(i * 10 + j),
                    new String(((Text) value).getBytes(), StandardCharsets.UTF_8),
                    "Map utf8 value mismatch at index " + i + "[" + j + "]");
                break;
              case "list":
                java.util.List<Integer> listValue = (java.util.List<Integer>) value;
                int expectedListSize = getListSize(i * 10 + j);
                assertEquals(
                    expectedListSize,
                    listValue.size(),
                    "Map list value size mismatch at index " + i + "[" + j + "]");
                for (int k = 0; k < expectedListSize; k++) {
                  assertEquals(
                      getListElement(i * 10 + j, k),
                      listValue.get(k),
                      "Map list element mismatch at index " + i + "[" + j + "][" + k + "]");
                }
                break;
            }
          }
        }
      }
    }

    private Field newMapStringIntField() {
      return new Field(
          "map-string-int",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
    }

    private Field newMapStringLongField() {
      return new Field(
          "map-string-long",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)))));
    }

    private Field newMapStringFloatField() {
      return new Field(
          "map-string-float",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                          null)))));
    }

    private Field newMapStringDoubleField() {
      return new Field(
          "map-string-double",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                          null)))));
    }

    private Field newMapStringDecimalField() {
      return new Field(
          "map-string-decimal",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value", FieldType.nullable(new ArrowType.Decimal(16, 0, 128)), null)))));
    }

    private Field newMapStringDateField() {
      return new Field(
          "map-string-date",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                          null)))));
    }

    private Field newMapStringTimestampField() {
      return new Field(
          "map-string-timestamp",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.Timestamp(
                                  org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
                          null)))));
    }

    private Field newMapStringDurationField() {
      return new Field(
          "map-string-duration",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.Duration(
                                  org.apache.arrow.vector.types.TimeUnit.SECOND)),
                          null)))));
    }

    private Field newMapStringIntervalField() {
      return new Field(
          "map-string-interval",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                          null)))));
    }

    private Field newMapStringBinaryField() {
      return new Field(
          "map-string-binary",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Binary()), null)))));
    }

    private Field newMapStringUtf8Field() {
      return new Field(
          "map-string-utf8",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)))));
    }

    private Field newMapStringListField() {
      return new Field(
          "map-string-list",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(ArrowType.List.INSTANCE),
                          java.util.Collections.singletonList(
                              new Field(
                                  "$data$",
                                  FieldType.nullable(new ArrowType.Int(32, true)),
                                  null)))))));
    }

    private int getMapSize(int index) {
      return (index % 3) + 1;
    }

    private String getMapStringKey(int rowIndex, int entryIndex) {
      return "key_" + rowIndex + "_" + entryIndex;
    }
  }

  /** Test binary types */
  private class TestBinaryTypes implements DataTester {
    private final Field fixedSizeBinaryField;
    private final Field varBinaryField;
    private final Field largeVarBinaryField;
    private final Schema schema;
    private final int FIXED_SIZE_BINARY_LENGTH = 16;
    private final int VAR_BINARY_LENGTH = 32;
    private final int LARGE_VAR_BINARY_LENGTH = 50;

    TestBinaryTypes() {
      fixedSizeBinaryField = newFixedSizeBinaryField();
      varBinaryField = newVarBinaryField();
      largeVarBinaryField = newLargeVarBinaryField();
      schema = new Schema(Arrays.asList(fixedSizeBinaryField, varBinaryField, largeVarBinaryField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      FixedSizeBinaryVector fixedSizeBinaryVector =
          (FixedSizeBinaryVector) vectorSchemaRoot.getVector(fixedSizeBinaryField.getName());
      VarBinaryVector varBinaryVector =
          (VarBinaryVector) vectorSchemaRoot.getVector(varBinaryField.getName());
      LargeVarBinaryVector largeVarBinaryVector =
          (LargeVarBinaryVector) vectorSchemaRoot.getVector(largeVarBinaryField.getName());

      // Set fixed-size binary (16 bytes).
      fixedSizeBinaryVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          fixedSizeBinaryVector.setNull(i);
        } else {
          fixedSizeBinaryVector.set(i, getFixedSizeBinary(i, FIXED_SIZE_BINARY_LENGTH));
        }
      }

      // Set variable binary.
      varBinaryVector.allocateNew(batchSize * 20L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          varBinaryVector.setNull(i);
        } else {
          varBinaryVector.set(i, getVarBinary(i, VAR_BINARY_LENGTH));
        }
      }

      // Set large variable binary.
      largeVarBinaryVector.clear();
      largeVarBinaryVector.allocateNew(batchSize * 50L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          largeVarBinaryVector.setNull(i);
        } else {
          largeVarBinaryVector.set(i, getLargeVarBinary(i, LARGE_VAR_BINARY_LENGTH));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      FixedSizeBinaryVector fixedSizeBinaryVector =
          (FixedSizeBinaryVector) vectorSchemaRoot.getVector(fixedSizeBinaryField.getName());
      VarBinaryVector varBinaryVector =
          (VarBinaryVector) vectorSchemaRoot.getVector(varBinaryField.getName());
      LargeVarBinaryVector largeVarBinaryVector =
          (LargeVarBinaryVector) vectorSchemaRoot.getVector(largeVarBinaryField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate fixed-size binary (16 bytes)
        if (i % 2 == 0) {
          assertTrue(
              fixedSizeBinaryVector.isNull(i), "Fixed-size binary should be null at index " + i);
        } else {
          byte[] expected = getFixedSizeBinary(i, FIXED_SIZE_BINARY_LENGTH);
          byte[] actual = fixedSizeBinaryVector.get(i);
          assertNotNull(actual, "Fixed-size binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Fixed-size binary mismatch at index " + i);
        }

        // Validate variable binary
        if (i % 2 == 0) {
          assertTrue(varBinaryVector.isNull(i), "Variable binary should be null at index " + i);
        } else {
          byte[] expected = getVarBinary(i, VAR_BINARY_LENGTH);
          byte[] actual = varBinaryVector.get(i);
          assertNotNull(actual, "Variable binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Variable binary mismatch at index " + i);
        }

        // Validate large variable binary
        if (i % 2 == 0) {
          assertTrue(
              largeVarBinaryVector.isNull(i), "Large variable binary should be null at index " + i);
        } else {
          byte[] expected = getLargeVarBinary(i, LARGE_VAR_BINARY_LENGTH);
          byte[] actual = largeVarBinaryVector.get(i);
          assertNotNull(actual, "Large variable binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Large variable binary mismatch at index " + i);
        }
      }
    }
  }

  private class TestUnionTypes implements DataTester {
    private final Field unionField;
    private final Schema schema;
    private final int VAR_BINARY_LENGTH = 64;

    TestUnionTypes() {
      unionField = newUnionField();
      schema = new Schema(java.util.Collections.singletonList(unionField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot root, int totalRows) {
      DenseUnionVector unionVector = (DenseUnionVector) root.getVector(unionField.getName());

      // Set Union - cycles through all types
      unionVector.allocateNew();

      // Get child vectors by type ID (0-10)
      IntVector unionIntVector = (IntVector) unionVector.getVectorByType((byte) 0);
      BigIntVector unionLongVector = (BigIntVector) unionVector.getVectorByType((byte) 1);
      Float4Vector unionFloatVector = (Float4Vector) unionVector.getVectorByType((byte) 2);
      Float8Vector unionDoubleVector = (Float8Vector) unionVector.getVectorByType((byte) 3);
      DecimalVector unionDecimalVector = (DecimalVector) unionVector.getVectorByType((byte) 4);
      DateDayVector unionDateVector = (DateDayVector) unionVector.getVectorByType((byte) 5);
      TimeStampMilliTZVector unionTimestampVector =
          (TimeStampMilliTZVector) unionVector.getVectorByType((byte) 6);
      DurationVector unionDurationVector = (DurationVector) unionVector.getVectorByType((byte) 7);
      IntervalYearVector unionIntervalVector =
          (IntervalYearVector) unionVector.getVectorByType((byte) 8);
      VarBinaryVector unionBinaryVector = (VarBinaryVector) unionVector.getVectorByType((byte) 9);
      VarCharVector unionUtf8Vector = (VarCharVector) unionVector.getVectorByType((byte) 10);

      // Track offsets for each type (11 types: 0-10)
      int[] typeOffsets = new int[11];

      for (int i = 0; i < totalRows; i++) {
        // Cycle through different types based on index mod 11
        int typeIndex = i % 11;
        byte typeId = (byte) typeIndex;

        // Get the current offset for this type
        int offset = typeOffsets[typeIndex];

        // Set type ID and offset for this position
        unionVector.setTypeId(i, typeId);
        unionVector.setOffset(i, offset);

        switch (typeIndex) {
          case 0: // Int
            unionIntVector.setSafe(offset, getSignedInt(i));
            break;
          case 1: // Long
            unionLongVector.setSafe(offset, getSignedLong(i));
            break;
          case 2: // Float
            unionFloatVector.setSafe(offset, getFloat(i));
            break;
          case 3: // Double
            unionDoubleVector.setSafe(offset, getDouble(i));
            break;
          case 4: // Decimal
            unionDecimalVector.setSafe(offset, getDecimal(i));
            break;
          case 5: // Date
            unionDateVector.setSafe(offset, getDateDay(i));
            break;
          case 6: // Timestamp
            unionTimestampVector.setSafe(offset, getTimestampMilli(i));
            break;
          case 7: // Duration
            unionDurationVector.setSafe(offset, getDurationSec(i));
            break;
          case 8: // Interval Year-Month
            unionIntervalVector.setSafe(offset, getIntervalYearMonth(i));
            break;
          case 9: // Binary
            unionBinaryVector.setSafe(offset, getVarBinary(i, VAR_BINARY_LENGTH));
            break;
          case 10: // UTF8
            unionUtf8Vector.setSafe(offset, getUtf8String(i).getBytes(StandardCharsets.UTF_8));
            break;
        }

        // Increment the offset for this type
        typeOffsets[typeIndex]++;
      }
      unionVector.setValueCount(totalRows);
    }

    @Override
    public void validateData(VectorSchemaRoot root) {
      DenseUnionVector unionVector = (DenseUnionVector) root.getVector(unionField.getName());
      int rowCount = root.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate Union
        assertFalse(unionVector.isNull(i), "Union should not be null at index " + i);
        int typeIndex = i % 11;
        Object unionValue = unionVector.getObject(i);
        assertNotNull(unionValue, "Union value should not be null at index " + i);

        switch (typeIndex) {
          case 0: // Int
            assertEquals(
                getSignedInt(i),
                ((Integer) unionValue).intValue(),
                "Union int mismatch at index " + i);
            break;
          case 1: // Long
            assertEquals(
                getSignedLong(i),
                ((Long) unionValue).longValue(),
                "Union long mismatch at index " + i);
            break;
          case 2: // Float
            assertEquals(
                getFloat(i), (Float) unionValue, 0.0001f, "Union float mismatch at index " + i);
            break;
          case 3: // Double
            assertEquals(
                getDouble(i), (Double) unionValue, 0.0001, "Union double mismatch at index " + i);
            break;
          case 4: // Decimal
            assertEquals(
                getDecimal(i),
                ((BigDecimal) unionValue).longValue(),
                "Union decimal mismatch at index " + i);
            break;
          case 5: // Date
            assertEquals(
                getDateDay(i),
                ((Integer) unionValue).intValue(),
                "Union date mismatch at index " + i);
            break;
          case 6: // Timestamp
            assertEquals(
                getTimestampMilli(i),
                ((Long) unionValue).longValue(),
                "Union timestamp mismatch at index " + i);
            break;
          case 7: // Duration
            assertEquals(
                getDurationSec(i),
                ((java.time.Duration) unionValue).getSeconds(),
                "Union duration mismatch at index " + i);
            break;
          case 8: // Interval
            assertEquals(
                getIntervalYearMonth(i),
                ((Period) unionValue).getMonths(),
                "Union interval mismatch at index " + i);
            break;
          case 9: // Binary
            assertArrayEquals(
                getVarBinary(i, VAR_BINARY_LENGTH),
                (byte[]) unionValue,
                "Union binary mismatch at index " + i);
            break;
          case 10: // UTF8
            assertEquals(
                getUtf8String(i),
                new String(((Text) unionValue).getBytes(), StandardCharsets.UTF_8),
                "Union utf8 mismatch at index " + i);
            break;
        }
      }
    }

    private Field newUnionField() {
      return new Field(
          "union-all-types",
          FieldType.nullable(
              new ArrowType.Union(UnionMode.Dense, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})),
          Arrays.asList(
              new Field("u_int", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("u_long", FieldType.nullable(new ArrowType.Int(64, true)), null),
              new Field(
                  "u_float",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null),
              new Field(
                  "u_double",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  null),
              new Field("u_decimal", FieldType.nullable(new ArrowType.Decimal(16, 0, 128)), null),
              new Field(
                  "u_date",
                  FieldType.nullable(
                      new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                  null),
              new Field(
                  "u_timestamp",
                  FieldType.nullable(
                      new ArrowType.Timestamp(
                          org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
                  null),
              new Field(
                  "u_duration",
                  FieldType.nullable(
                      new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)),
                  null),
              new Field(
                  "u_interval",
                  FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                  null),
              new Field("u_binary", FieldType.nullable(new ArrowType.Binary()), null),
              new Field("u_utf8", FieldType.nullable(new ArrowType.Utf8()), null)));
    }
  }

  /** Test dictionary types */
  private class TestDictionaryTypes implements DataTester {
    private final Field dictStringField;
    private final Field dictIntField;
    private final Schema schema;

    TestDictionaryTypes() {
      dictStringField = newDictStringField();
      dictIntField = newDictIntField();
      schema = new Schema(Arrays.asList(dictStringField, dictIntField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      VarCharVector dictStringVector =
          (VarCharVector) vectorSchemaRoot.getVector(dictStringField.getName());
      IntVector dictIntVector = (IntVector) vectorSchemaRoot.getVector(dictIntField.getName());

      // Set dictionary-encoded strings (repeating pattern of values)
      dictStringVector.allocateNew(batchSize * 20L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dictStringVector.setNull(i);
        } else {
          dictStringVector.set(i, getDictString(i).getBytes(StandardCharsets.UTF_8));
        }
      }

      // Set dictionary-encoded integers (repeating pattern of values)
      dictIntVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dictIntVector.setNull(i);
        } else {
          dictIntVector.set(i, getDictInt(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      VarCharVector dictStringVector =
          (VarCharVector) vectorSchemaRoot.getVector(dictStringField.getName());
      IntVector dictIntVector = (IntVector) vectorSchemaRoot.getVector(dictIntField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate dictionary string
        if (i % 2 == 0) {
          assertTrue(dictStringVector.isNull(i), "Dictionary string should be null at index " + i);
        } else {
          String expected = getDictString(i);
          byte[] actualBytes = dictStringVector.get(i);
          assertNotNull(actualBytes, "Dictionary string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "Dictionary string mismatch at index " + i);
        }

        // Validate dictionary int
        if (i % 2 == 0) {
          assertTrue(dictIntVector.isNull(i), "Dictionary int should be null at index " + i);
        } else {
          assertEquals(
              getDictInt(i), dictIntVector.get(i), "Dictionary int mismatch at index " + i);
        }
      }
    }
  }

  /** Test run-end encoded types */
  private class TestRunEndEncodedTypes implements DataTester {
    private final Field reeIntField;
    private final Field reeLongField;
    private final Schema schema;

    TestRunEndEncodedTypes() {
      reeIntField = newRunEndEncodedIntField();
      reeLongField = newRunEndEncodedLongField();
      schema = new Schema(Arrays.asList(reeIntField, reeLongField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      RunEndEncodedVector reeIntVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeIntField.getName());
      RunEndEncodedVector reeLongVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeLongField.getName());

      // Set run-end encoded integers
      // Calculate required capacity: runs every 3 rows means we need (batchSize + 2) / 3 runs
      int reeIntCapacity = (batchSize + 2) / 3;
      reeIntVector.allocateNew();
      IntVector reeIntRunEnds = (IntVector) reeIntVector.getRunEndsVector();
      IntVector reeIntValues = (IntVector) reeIntVector.getValuesVector();
      reeIntRunEnds.allocateNew(reeIntCapacity);
      reeIntValues.allocateNew(reeIntCapacity);

      // Create runs with repeating values (every 3 rows have the same value)
      int runCount = 0;
      for (int i = 0; i < batchSize; i += 3) {
        int runEnd = Math.min(i + 3, batchSize);
        reeIntRunEnds.set(runCount, runEnd);
        reeIntValues.set(runCount, getSignedInt(i));
        runCount++;
      }
      reeIntRunEnds.setValueCount(runCount);
      reeIntValues.setValueCount(runCount);
      reeIntVector.setValueCount(batchSize);

      // Set run-end encoded longs
      // Calculate required capacity: runs every 5 rows means we need (batchSize + 4) / 5 runs
      int reeLongCapacity = (batchSize + 4) / 5;
      reeLongVector.allocateNew();
      IntVector reeLongRunEnds = (IntVector) reeLongVector.getRunEndsVector();
      BigIntVector reeLongValues = (BigIntVector) reeLongVector.getValuesVector();
      reeLongRunEnds.allocateNew(reeLongCapacity);
      reeLongValues.allocateNew(reeLongCapacity);

      // Create runs with repeating values (every 5 rows have the same value)
      runCount = 0;
      for (int i = 0; i < batchSize; i += 5) {
        int runEnd = Math.min(i + 5, batchSize);
        reeLongRunEnds.set(runCount, runEnd);
        reeLongValues.set(runCount, getSignedLong(i));
        runCount++;
      }
      reeLongRunEnds.setValueCount(runCount);
      reeLongValues.setValueCount(runCount);
      reeLongVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      RunEndEncodedVector reeIntVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeIntField.getName());
      RunEndEncodedVector reeLongVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeLongField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate run-end encoded int
        Object reeIntValue = reeIntVector.getObject(i);
        assertNotNull(reeIntValue, "REE int should not be null at index " + i);
        int expectedInt = getSignedInt((i / 3) * 3);
        assertEquals(
            expectedInt, ((Integer) reeIntValue).intValue(), "REE int mismatch at index " + i);

        // Validate run-end encoded long
        Object reeLongValue = reeLongVector.getObject(i);
        assertNotNull(reeLongValue, "REE long should not be null at index " + i);
        long expectedLong = getSignedLong((i / 5) * 5);
        assertEquals(
            expectedLong, ((Long) reeLongValue).longValue(), "REE long mismatch at index " + i);
      }
    }

    private Field newRunEndEncodedIntField() {
      Field runEndField =
          new Field("run_ends", FieldType.notNullable(new ArrowType.Int(32, true)), null);
      Field valueField = new Field("values", FieldType.nullable(new ArrowType.Int(32, true)), null);
      return new Field(
          "ree-int",
          FieldType.nullable(new ArrowType.RunEndEncoded()),
          Arrays.asList(runEndField, valueField));
    }

    private Field newRunEndEncodedLongField() {
      Field runEndField =
          new Field("run_ends", FieldType.notNullable(new ArrowType.Int(32, true)), null);
      Field valueField = new Field("values", FieldType.nullable(new ArrowType.Int(64, true)), null);
      return new Field(
          "ree-long",
          FieldType.nullable(new ArrowType.RunEndEncoded()),
          Arrays.asList(runEndField, valueField));
    }
  }

  private Field newDictStringField() {
    return new Field("dict-string", FieldType.nullable(new ArrowType.Utf8()), null);
  }

  private Field newDictIntField() {
    return new Field("dict-int", FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private String getDictString(int index) {
    // Return repeating values to simulate dictionary-encoded data
    String[] dictValues = {"Red", "Green", "Blue", "Yellow", "Orange"};
    return dictValues[index % dictValues.length];
  }

  private int getDictInt(int index) {
    // Return repeating values to simulate dictionary-encoded data
    return (index % 10) * 100;
  }

  private Field newSignedByteIntField() {
    return new Field("signed-byte-int", FieldType.nullable(new ArrowType.Int(8, true)), null);
  }

  private Field newSignedShortIntField() {
    return new Field("signed-short-int", FieldType.nullable(new ArrowType.Int(16, true)), null);
  }

  private Field newSignedIntField() {
    return new Field("signed-int", FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  private Field newSignedLongField() {
    return new Field("signed-long", FieldType.nullable(new ArrowType.Int(64, true)), null);
  }

  // Unsigned integers.
  private Field newUnsignedByteIntField() {
    return new Field("unsigned-byte-int", FieldType.nullable(new ArrowType.Int(8, false)), null);
  }

  private Field newUnsignedShortIntField() {
    return new Field("unsigned-short-int", FieldType.nullable(new ArrowType.Int(16, false)), null);
  }

  private Field newUnsignedIntField() {
    return new Field("unsigned-int", FieldType.nullable(new ArrowType.Int(32, false)), null);
  }

  private Field newUnsignedLongField() {
    return new Field("unsigned-long", FieldType.nullable(new ArrowType.Int(64, false)), null);
  }

  private Field newFloatField() {
    return new Field(
        "float",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        null);
  }

  private Field newDoubleField() {
    return new Field(
        "double",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        null);
  }

  private Field newDecimalField(int precision, int scale, int bitWidth) {
    String name = "decimal-" + precision + "-" + scale + "-" + bitWidth;
    return new Field(
        name, FieldType.nullable(new ArrowType.Decimal(precision, scale, bitWidth)), null);
  }

  private int getSignedByte(int index) {
    return (index % 256) - 128;
  }

  private int getSignedShort(int index) {
    return (index % 65536) - 32768;
  }

  private int getSignedInt(int index) {
    return index * (index % 3 == 0 ? -1 : 1);
  }

  private long getSignedLong(int index) {
    return (long) index + Integer.MAX_VALUE * (index % 3 == 0 ? -1 : 1);
  }

  private int getUnsignedByte(int index) {
    return index % 256;
  }

  private int getUnsignedShort(int index) {
    return index % 65536;
  }

  private int getUnsignedInt(int index) {
    return index;
  }

  private long getUnsignedLong(int index) {
    return (long) index * 2;
  }

  private float getFloat(int index) {
    return (float) index * (float) Math.PI * (index % 3 == 0 ? -1 : 1);
  }

  private double getDouble(int index) {
    return index * Math.PI * (index % 3 == 0 ? -1 : 1);
  }

  private long getDecimal(int index) {
    return (long) index + Integer.MAX_VALUE * (index % 3 == 0 ? -1 : 1);
  }

  private Field newDateDayField() {
    return new Field(
        "date-day",
        FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
        null);
  }

  private Field newTimestampMilliField() {
    return new Field(
        "timestamp-milli",
        FieldType.nullable(
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC")),
        null);
  }

  private Field newTimeSecField() {
    return new Field(
        "time-sec",
        FieldType.nullable(new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.SECOND, 32)),
        null);
  }

  private Field newDurationSecField() {
    return new Field(
        "duration-sec",
        FieldType.nullable(new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.SECOND)),
        null);
  }

  private Field newIntervalYearField() {
    return new Field(
        "interval-year", FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)), null);
  }

  private Field newIntervalDayField() {
    return new Field(
        "interval-day", FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)), null);
  }

  private int getDateDay(int index) {
    return 18000 + (index % 10000);
  }

  private long getTimestampMilli(int index) {
    return 1577836800000L + ((long) index * 1000L);
  }

  private int getTimeSec(int index) {
    return index % 86400;
  }

  private long getDurationSec(int index) {
    return (long) index * 3600L * (index % 3 == 0 ? -1 : 1);
  }

  private int getIntervalYearMonth(int index) {
    return index % 600;
  }

  private int getIntervalDayDays(int index) {
    return index % 365;
  }

  private int getIntervalDayMillis(int index) {
    return (index * 1000) % 86400000;
  }

  private Field newFixedSizeBinaryField() {
    return new Field(
        "fixed-size-binary", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
  }

  private Field newVarBinaryField() {
    return new Field("variable-binary", FieldType.nullable(new ArrowType.Binary()), null);
  }

  private Field newLargeVarBinaryField() {
    return new Field(
        "large-variable-binary", FieldType.nullable(new ArrowType.LargeBinary()), null);
  }

  private byte[] getFixedSizeBinary(int index, int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) ((index + i) % 256);
    }
    return data;
  }

  private byte[] getVarBinary(int index, int maxLength) {
    int length = (index % maxLength) + 1;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) ((index * 3 + i) % 256);
    }
    return data;
  }

  private byte[] getLargeVarBinary(int index, int maxLength) {
    int length = (index % maxLength) + 1;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) ((index * 7 + i) % 256);
    }
    return data;
  }

  private Field newUtf8Field() {
    return new Field("utf8-string", FieldType.nullable(new ArrowType.Utf8()), null);
  }

  private Field newLargeUtf8Field() {
    return new Field("large-utf8-string", FieldType.nullable(new ArrowType.LargeUtf8()), null);
  }

  private String getUtf8String(int index) {
    return "UTF8-String-" + index + "-Data";
  }

  private String getLargeUtf8String(int index) {
    return "LargeUTF8-String-" + index + "-DataWithMoreContent-" + (index * 3);
  }

  private Field newListIntField() {
    return new Field(
        "list-int",
        FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(
            new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
  }

  private Field newLargeListIntField() {
    return new Field(
        "large-list-int",
        FieldType.nullable(ArrowType.LargeList.INSTANCE),
        Collections.singletonList(
            new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
  }

  private int getListSize(int index) {
    return index % 32 + 1;
  }

  private int getListElement(int rowIndex, int elementIndex) {
    return rowIndex * 100 + elementIndex;
  }

  private int getLargeListSize(int index) {
    return (index % 128) + 1;
  }

  private int getLargeListElement(int rowIndex, int elementIndex) {
    return rowIndex * 1000 + elementIndex;
  }
}
