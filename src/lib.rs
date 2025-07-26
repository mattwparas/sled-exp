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
use bincode::Decode;
use bincode::Encode;
use sled::Tree;

// TODO: Wrap all the interval times!
struct WIntervalDayTime {}

#[derive(Encode, Decode)]
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

struct TableGuard<'a> {
    schema: SchemaRef,
    tree: &'a mut Tree<1024>,
}

// These will be stored per leaf. Key -> Value
// should make it easier to get the information
enum BatchKind {
    // Insert a batch when pushing down a full table.
    RecordBatch(RecordBatch),
    // Separately, alongside that record batch, we just need
    // to store indexed values against the records.
    RowSet(Vec<ArrowRow>),

    // Index into the row set
    Index(Vec<u8>, usize),
}

// Prefix... with the proper values?
impl Encode for BatchKind {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        todo!()
    }
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
#[derive(Encode, Decode)]
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
