use std::any::Any;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::array::as_string_array;
use arrow::datatypes::Date32Type;
use arrow::datatypes::Date64Type;
use arrow::datatypes::Decimal128Type;
use arrow::datatypes::Decimal256Type;
use arrow::datatypes::DurationMicrosecondType;
use arrow::datatypes::DurationMillisecondType;
use arrow::datatypes::DurationNanosecondType;
use arrow::datatypes::DurationSecondType;
use arrow::datatypes::Float16Type;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::IntervalDayTimeType;
use arrow::datatypes::IntervalMonthDayNanoType;
use arrow::datatypes::IntervalYearMonthType;
use arrow::datatypes::Time32MillisecondType;
use arrow::datatypes::Time32SecondType;
use arrow::datatypes::Time64MicrosecondType;
use arrow::datatypes::Time64NanosecondType;
use arrow::datatypes::TimestampMicrosecondType;
use arrow::datatypes::TimestampMillisecondType;
use arrow::datatypes::TimestampNanosecondType;
use arrow::datatypes::TimestampSecondType;
use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow_array::ArrowPrimitiveType;
use arrow_array::BinaryArray;
use arrow_array::BinaryArrayType;
use arrow_array::LargeBinaryArray;
use arrow_array::RecordBatch;
use arrow_array::record_batch;
use arrow_schema;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::SchemaRef;
use arrow_schema::TimeUnit;
use arrow_schema::UnionFields;
use arrow_schema::UnionMode;
use arrow_schema::{Schema, SchemaBuilder};

// Need a row encoding of datafusion arrays that use the same native
// datafusion types.

use arrow::datatypes::{DataType, IntervalUnit};
use bincode::BorrowDecode;
use bincode::Decode;
use bincode::Encode;
use sled::Tree;

// TODO: Wrap all the interval times!
struct WIntervalDayTime {}

#[derive(Encode, Decode, Debug, Clone)]
pub enum ArrowPrimitiveValue {
    Null,

    Boolean(bool),

    Date32(<Date32Type as ArrowPrimitiveType>::Native),
    Date64(<Date64Type as ArrowPrimitiveType>::Native),
    Decimal128(<Decimal128Type as ArrowPrimitiveType>::Native),

    // TODO: Handle this with encoding / decoding?
    // Decimal256(<Decimal256Type as ArrowPrimitiveType>::Native),
    DurationMicrosecond(<DurationMicrosecondType as ArrowPrimitiveType>::Native),
    DurationMillisecond(<DurationMillisecondType as ArrowPrimitiveType>::Native),
    DurationNanosecond(<DurationNanosecondType as ArrowPrimitiveType>::Native),
    DurationSecond(<DurationSecondType as ArrowPrimitiveType>::Native),
    // Float16(<Float16Type as ArrowPrimitiveType>::Native),
    Float32(<Float32Type as ArrowPrimitiveType>::Native),
    Float64(<Float64Type as ArrowPrimitiveType>::Native),
    Int8(<Int8Type as ArrowPrimitiveType>::Native),
    Int16(<Int16Type as ArrowPrimitiveType>::Native),
    Int32(<Int32Type as ArrowPrimitiveType>::Native),
    Int64(<Int64Type as ArrowPrimitiveType>::Native),
    // TODO: Create wrapped versions of these for encoding / decoding!
    // IntervalDayTime(<IntervalDayTimeType as ArrowPrimitiveType>::Native),
    // IntervalMonthDayNano(<IntervalMonthDayNanoType as ArrowPrimitiveType>::Native),
    // IntervalYearMonth(<IntervalYearMonthType as ArrowPrimitiveType>::Native),
    Time32Millisecond(<Time32MillisecondType as ArrowPrimitiveType>::Native),
    Time32Second(<Time32SecondType as ArrowPrimitiveType>::Native),
    Time64Microsecond(<Time64MicrosecondType as ArrowPrimitiveType>::Native),
    Time64Nanosecond(<Time64NanosecondType as ArrowPrimitiveType>::Native),
    TimestampMicrosecond(<TimestampMicrosecondType as ArrowPrimitiveType>::Native),
    TimestampMillisecond(<TimestampMillisecondType as ArrowPrimitiveType>::Native),
    TimestampNanosecond(<TimestampNanosecondType as ArrowPrimitiveType>::Native),
    TimestampSecond(<TimestampSecondType as ArrowPrimitiveType>::Native),
    UInt8(<UInt8Type as ArrowPrimitiveType>::Native),
    UInt16(<UInt16Type as ArrowPrimitiveType>::Native),
    UInt32(<UInt32Type as ArrowPrimitiveType>::Native),
    UInt64(<UInt64Type as ArrowPrimitiveType>::Native),

    Binary(Vec<u8>),

    String(String),
}

// Table State, this is morally just understanding the row counts based
// on how much has been written.
struct TableState {
    schema: SchemaRef,
    row_count: usize,
}

pub struct TableGuard<'a> {
    pub schema: SchemaRef,
    pub tree: &'a mut Tree<1024>,
}

pub static ROW_COUNT_KEY: std::sync::LazyLock<Vec<u8>> =
    std::sync::LazyLock::new(|| KeyKind::RowCount { name: "#%rowcount" }.encode());

impl<'a> TableGuard<'a> {
    // The table needs to be locked for this whole duration.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        // Check that the schemas are correct
        debug_assert!(batch.schema_ref() == &self.schema);

        // TODO: Insert #%rowcount into table first!
        let row_count: usize = self
            .tree
            .get(ROW_COUNT_KEY.as_slice())
            .unwrap()
            .map(|row_count| {
                bincode::decode_from_slice(&row_count, bincode::config::standard())
                    .unwrap()
                    .0
            })
            .unwrap_or(0);

        let mut sled_batch = sled::Batch::default();

        let batch_row_count = batch.num_rows();

        // This is our new offset
        let batches = split_record_batch(batch)?;

        for batch in batches {
            let key = KeyKind::Column {
                name: batch.schema().fields().first().unwrap().name().as_str(),
                // Offset from the beginning
                row_offset: row_count,
            }
            .encode();

            let value = encode_record_batch(&batch).unwrap();

            sled_batch.insert(key, value);
        }

        sled_batch.insert(
            ROW_COUNT_KEY.as_slice(),
            bincode::encode_to_vec(row_count + batch_row_count, bincode::config::standard())
                .unwrap(),
        );

        self.tree.apply_batch(sled_batch).unwrap();

        Ok(())
    }

    pub fn read_batch<'b>(&mut self, cols: Vec<&'b str>) -> Result<Vec<RecordBatch>, ArrowError> {
        // Go through and build up some batches
        let mut batches = Vec::new();

        let mut col_map: HashMap<_, Vec<RecordBatch>> =
            cols.iter().map(|x| (x, Vec::new())).collect();

        for name in cols.iter() {
            let name = *name;
            let batches = self
                .tree
                .scan_prefix(KeyKindQuery::Column { name }.encode());

            for batch in batches {
                let (_, batch) = batch.unwrap();
                let decoded = decode_record_batch(&batch).unwrap();
                // Unfurl the record batch. Could also eagerly combine them?
                col_map.get_mut(&name).unwrap().extend(decoded);
            }
        }

        let batch_length = col_map.iter().next().unwrap().1.len();

        for i in 0..batch_length {
            let mut local_batch = Vec::new();

            for col in &cols {
                local_batch.push(col_map.get(col).unwrap()[i].clone());
            }

            let merged = merge_batches(local_batch).unwrap();

            batches.push(merged);
        }

        Ok(batches)
    }
}

// How to figure out the keys when writing record batches in?
//
// Lock the database from the writer. The database itself will be locked
// via a mutex so that we can guarantee that we're the only ones
// accessing it. At the start of our lock, we get all of the things that
// we want to, meaning the row offsets and counts that we need for book keeping.
//
// We then constructs a batch which holds all of the things that we want.
// in this case, this will be:
//
// Splitting up the record batches into their respective columns, and then
// writing them back to the store. We'll atomically write everything, with
// reads up front to be reasonably quick.
#[repr(C)]
#[derive(Encode, BorrowDecode, Debug)]
enum KeyKind<'a> {
    // Column will have some name. We want to get to everything
    // that matches a specific column.
    Column { name: &'a str, row_offset: usize },

    // Row will be indexed via primary key.
    Row { key: &'a [u8] },

    // Primary key for a record batch. Optionally can
    // index into a record batch in order to tell us
    // which offset will be there.
    RecordBatchIndex { name: &'a str },

    RowCount { name: &'a str },
}

impl<'a> KeyKind<'a> {
    // Encode the key kind
    pub fn encode(&self) -> Vec<u8> {
        bincode::encode_to_vec(&self, bincode::config::standard()).unwrap()
    }
}

#[repr(C)]
#[derive(Encode, BorrowDecode)]
enum KeyKindQuery<'a> {
    Column { name: &'a str },
    Row { key: &'a [u8] },
    RecordBatchIndex { name: &'a str },
}

impl<'a> KeyKindQuery<'a> {
    // Encode the key kind
    pub fn encode(&self) -> Vec<u8> {
        bincode::encode_to_vec(&self, bincode::config::standard()).unwrap()
    }
}

#[test]
fn test_keys() {
    let db = sled::open("test.db").unwrap();
    db.insert(
        bincode::encode_to_vec(
            KeyKind::Column {
                name: "foo",
                row_offset: 0,
            },
            bincode::config::standard(),
        )
        .unwrap(),
        bincode::encode_to_vec(
            ValueKind::Row(ArrowRow {
                values: vec![ArrowPrimitiveValue::Int32(10)],
            }),
            bincode::config::standard(),
        )
        .unwrap(),
    )
    .unwrap();

    db.insert(
        bincode::encode_to_vec(
            KeyKind::Column {
                name: "foo",
                row_offset: 100,
            },
            bincode::config::standard(),
        )
        .unwrap(),
        bincode::encode_to_vec(
            ValueKind::Row(ArrowRow {
                values: vec![ArrowPrimitiveValue::Int32(100)],
            }),
            bincode::config::standard(),
        )
        .unwrap(),
    )
    .unwrap();

    db.insert(
        bincode::encode_to_vec(
            KeyKind::Column {
                name: "foo",
                row_offset: 200,
            },
            bincode::config::standard(),
        )
        .unwrap(),
        bincode::encode_to_vec(
            ValueKind::Row(ArrowRow {
                values: vec![ArrowPrimitiveValue::Int32(1000)],
            }),
            bincode::config::standard(),
        )
        .unwrap(),
    )
    .unwrap();

    let prefix = bincode::encode_to_vec(
        KeyKindQuery::Column { name: "foo" },
        bincode::config::standard(),
    )
    .unwrap();

    for value in db.scan_prefix(&prefix) {
        let (key, value) = value.unwrap();

        let decode_key: (KeyKind<'_>, _) =
            bincode::borrow_decode_from_slice(&key, bincode::config::standard()).unwrap();

        let decode_value: ValueKind =
            bincode::decode_from_slice(&value, bincode::config::standard())
                .unwrap()
                .0;

        println!("{:?} -> {:?}", decode_key, decode_value);
    }
}

// Split up a record batch of smaller record batches.
// Each column will have its own thing.
pub fn split_record_batch(batch: &RecordBatch) -> Result<Vec<RecordBatch>, ArrowError> {
    (0..batch.columns().len())
        .into_iter()
        .map(|col| batch.project(&[col]))
        .collect()
}

pub fn merge_batches(batches: Vec<RecordBatch>) -> Result<RecordBatch, ArrowError> {
    let mut schema = SchemaBuilder::new();

    for batch in &batches {
        // I'd expect that the batches here are only of length 1
        // given the way we're splitting things up above.
        debug_assert!(batch.columns().len() == 1);

        let col_schema = batch.schema();

        for field in col_schema.fields() {
            schema.try_merge(field)?;
        }

        let metadata = schema.metadata_mut();
        for (key, value) in col_schema.metadata() {
            metadata.insert(key.to_owned(), value.to_owned());
        }
    }

    // Merge all the columns
    let columns = batches
        .into_iter()
        .flat_map(|x| x.columns().to_vec())
        .collect();

    RecordBatch::try_new(Arc::new(schema.finish()), columns)
}

// Encode the individual record batch into something that we can use
fn encode_record_batch_into(batch: &RecordBatch, buffer: &mut Vec<u8>) -> Result<(), ArrowError> {
    let schema = batch.schema();
    let mut writer = FileWriter::try_new(buffer, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buffer = Vec::new();
    encode_record_batch_into(batch, &mut buffer)?;
    Ok(buffer)
}

fn decode_record_batch(batch: &[u8]) -> Result<Vec<RecordBatch>, ArrowError> {
    let cursor = Cursor::new(batch);
    let mut record_batches = Vec::new();

    let reader = FileReader::try_new(cursor, None).unwrap();
    for batch in reader {
        let batch = batch.unwrap();
        record_batches.push(batch);
    }

    Ok(record_batches)
}

#[derive(Debug)]
struct ColumnRecord {
    batch: RecordBatch,
}

impl Encode for ColumnRecord {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let batch = encode_record_batch(&self.batch).unwrap();
        Encode::encode(&batch, encoder)
    }
}

impl<Context> Decode<Context> for ColumnRecord {
    fn decode<D: bincode::de::Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        let batch = decode_record_batch(&bytes)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        Ok(ColumnRecord { batch })
    }
}

// Why is this the same as the above?
impl<'de, Context> BorrowDecode<'de, Context> for ColumnRecord {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        let batch = decode_record_batch(&bytes)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        Ok(ColumnRecord { batch })
    }
}

#[derive(Encode, Decode, Debug)]
enum ValueKind {
    // TODO: Implement encoding logic here!
    // Each column will be encoded in its own record batch
    Column(ColumnRecord),

    // Separately encode a row, if the row was inserted on its own
    Row(ArrowRow),

    // Global counter for the number of rows in the table
    Counter(usize),
}

pub fn add_column_to_record_batch(batch: RecordBatch) -> Result<RecordBatch, ArrowError> {
    let rows = batch_to_rows(&batch);
    let (mut schema, mut items, _) = batch.into_parts();

    let mut schema_builder = SchemaBuilder::from(schema.fields());
    schema_builder.push(Field::new("__row", DataType::LargeBinary, false));
    schema = Arc::new(schema_builder.finish());

    let bytes = rows
        .into_iter()
        .map(|x| bincode::encode_to_vec(&x, bincode::config::standard()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let array = Arc::new(LargeBinaryArray::from_iter_values(bytes));
    items.push(array);
    RecordBatch::try_new(schema, items)
}

// Create a column of encoded columnar values:
pub fn batch_to_rows(batch: &RecordBatch) -> Vec<ArrowRow> {
    let mut rows: Vec<ArrowRow> = Vec::with_capacity(batch.num_rows());

    let col_count = batch.num_columns();

    for _ in 0..batch.num_rows() {
        rows.push(ArrowRow {
            values: Vec::with_capacity(col_count),
        });
    }

    for (field, col) in batch
        .schema_ref()
        .fields()
        .iter()
        .zip(batch.columns().iter())
    {
        macro_rules! explode_primitive {
            ($primitive:expr, $arrow:ty) => {{
                let array = col.as_primitive_opt::<$arrow>().unwrap();

                let int_iter = array
                    .iter()
                    .map(|x| x.map($primitive).unwrap_or(ArrowPrimitiveValue::Null));

                for (i, row) in int_iter.zip(rows.iter_mut()) {
                    row.push(i);
                }
            }};
        }

        match field.data_type() {
            DataType::Null => todo!(),
            DataType::Boolean => {
                let array = col.as_boolean_opt().unwrap();
                let bool_iter = array.iter().map(|x| {
                    x.map(ArrowPrimitiveValue::Boolean)
                        .unwrap_or(ArrowPrimitiveValue::Null)
                });

                for (b, row) in bool_iter.zip(rows.iter_mut()) {
                    row.push(b);
                }
            }
            // TODO: Macro this?
            DataType::Int8 => {
                explode_primitive!(ArrowPrimitiveValue::Int8, Int8Type)
            }
            DataType::Int16 => {
                explode_primitive!(ArrowPrimitiveValue::Int16, Int16Type)
            }
            DataType::Int32 => {
                explode_primitive!(ArrowPrimitiveValue::Int32, Int32Type)
            }
            DataType::Int64 => {
                explode_primitive!(ArrowPrimitiveValue::Int64, Int64Type)
            }
            DataType::UInt8 => {
                explode_primitive!(ArrowPrimitiveValue::UInt8, UInt8Type)
            }
            DataType::UInt16 => {
                explode_primitive!(ArrowPrimitiveValue::UInt16, UInt16Type)
            }
            DataType::UInt32 => {
                explode_primitive!(ArrowPrimitiveValue::UInt32, UInt32Type)
            }
            DataType::UInt64 => {
                explode_primitive!(ArrowPrimitiveValue::UInt64, UInt64Type)
            }
            DataType::Float16 => {
                todo!()
            }
            DataType::Float32 => {
                explode_primitive!(ArrowPrimitiveValue::Float32, Float32Type)
            }
            DataType::Float64 => {
                explode_primitive!(ArrowPrimitiveValue::Float64, Float64Type)
            }
            DataType::Timestamp(time_unit, _) => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time32(time_unit) => todo!(),
            DataType::Time64(time_unit) => todo!(),
            DataType::Duration(time_unit) => todo!(),
            DataType::Interval(interval_unit) => todo!(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let array = col.as_binary_view();
                let bool_iter = array.iter().map(|x| {
                    x.map(|val| ArrowPrimitiveValue::Binary(val.to_vec()))
                        .unwrap_or(ArrowPrimitiveValue::Null)
                });

                for (b, row) in bool_iter.zip(rows.iter_mut()) {
                    row.push(b);
                }
            }
            DataType::FixedSizeBinary(b) => {
                let array = col.as_fixed_size_binary_opt().unwrap();
                let bool_iter = array.iter().map(|x| {
                    x.map(|val| ArrowPrimitiveValue::Binary(val.to_vec()))
                        .unwrap_or(ArrowPrimitiveValue::Null)
                });

                for (b, row) in bool_iter.zip(rows.iter_mut()) {
                    row.push(b);
                }
            }

            DataType::Utf8 | DataType::LargeUtf8 => {
                let array = as_string_array(col);
                let bool_iter = array.iter().map(|x| {
                    x.map(|val| ArrowPrimitiveValue::String(val.to_owned()))
                        .unwrap_or(ArrowPrimitiveValue::Null)
                });

                for (b, row) in bool_iter.zip(rows.iter_mut()) {
                    row.push(b);
                }
            }

            DataType::Utf8View => {
                let array = col.as_string_view_opt().unwrap();
                let bool_iter = array.iter().map(|x| {
                    x.map(|val| ArrowPrimitiveValue::String(val.to_owned()))
                        .unwrap_or(ArrowPrimitiveValue::Null)
                });

                for (b, row) in bool_iter.zip(rows.iter_mut()) {
                    row.push(b);
                }
            }

            DataType::List(field) => todo!(),
            DataType::ListView(field) => todo!(),
            DataType::FixedSizeList(field, _) => todo!(),
            DataType::LargeList(field) => todo!(),
            DataType::LargeListView(field) => todo!(),
            DataType::Struct(fields) => todo!(),
            DataType::Union(union_fields, union_mode) => todo!(),
            DataType::Dictionary(data_type, data_type1) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(field, _) => todo!(),
            DataType::RunEndEncoded(field, field1) => todo!(),
        }
    }

    rows
}

// Convert columnar to arrow
#[derive(Encode, Decode, Debug)]
pub struct ArrowRow {
    values: Vec<ArrowPrimitiveValue>,
}

impl ArrowRow {
    pub fn push(&mut self, value: ArrowPrimitiveValue) {
        self.values.push(value);
    }
}

#[test]
pub fn test() {
    // let serializer = CsvSerializer::new();
    // serializer.
    let mut row = Vec::new();

    // Just... convert a record batch directly into a CSV, and then
    // yoink the values out as we go.
    let builder = arrow_csv::WriterBuilder::new().with_header(false);
    let mut writer = builder.build(&mut row);

    use arrow_array::record_batch;
    use arrow_schema;

    let batch = record_batch!(
        ("a", Int32, [1, 2, 3]),
        ("b", Float64, [Some(4.0), None, Some(5.0)]),
        ("c", Utf8, ["alpha", "beta", "gamma"])
    )
    .unwrap();

    let with_rows = add_column_to_record_batch(batch);

    dbg!(with_rows);

    // // Encode a whole column specifically for the row wise
    // // fetching. And then, we will just store column sets individually
    // // along the column of the encoded row wise data. Each batch will
    // // also build an index of the primary key along the way, so that
    // // selecting an individual batch of rows is reasonable
    // writer.write(&batch).unwrap();

    // drop(writer);

    // let mut row_cursor = Cursor::new(row);

    // // Fetch each batch individually
    // let mut reader = arrow_csv::ReaderBuilder::new(Arc::new(Schema::new(vec![
    //     Field::new("foo", arrow_schema::DataType::Int32, false),
    //     Field::new("bar", arrow_schema::DataType::Float64, true),
    //     Field::new("baz", arrow_schema::DataType::Utf8, false),
    // ])))
    // .with_header(false)
    // .build(&mut row_cursor)
    // .unwrap();

    // let batches = reader.collect::<Vec<_>>();

    // // Serialize the rows themselves into a new column:

    // // todo!()
}
