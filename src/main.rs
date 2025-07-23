use std::{
    any::Any,
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{Field, Schema, SchemaRef},
    },
    catalog::{Session, TableProvider},
    common::project_schema,
    datasource::TableType,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::dml::InsertOp,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_input_stream,
        memory::MemoryStream, stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use futures::StreamExt;
use sled::{Db, Tree};

#[derive(Debug, Clone)]
struct SledDb {
    db: Arc<Mutex<Db<1024>>>,
    schemas: Arc<Mutex<Tree<1024>>>,
    // TODO: Cache these tables?
    open_tables: HashMap<String, Arc<SledTable>>,
}

impl SledDb {
    pub fn new(config: &sled::Config) -> std::io::Result<Self> {
        let db = Db::open_with_config(config)?;
        let schemas = db.open_tree("schemas")?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
            schemas: Arc::new(Mutex::new(schemas)),
            open_tables: HashMap::new(),
        })
    }

    fn get_schema_for_table(&self, table: &str) -> std::io::Result<Option<SchemaRef>> {
        let guard = self.schemas.lock().unwrap();
        let schema = guard.get(table)?;

        if let Some(schema) = schema {
            let (schema, _) = bincode::serde::decode_from_slice::<Schema, _>(
                &schema,
                bincode::config::standard(),
            )
            .unwrap();

            Ok(Some(Arc::new(schema)))
        } else {
            Ok(None)
        }
    }

    // Create a new table
    fn register_table(&self, table: &str, schema: SchemaRef) -> std::io::Result<()> {
        let guard = self.schemas.lock().unwrap();
        let encoded = bincode::serde::encode_to_vec(schema, bincode::config::standard()).unwrap();
        guard.insert(table, encoded)?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> std::io::Result<Option<SledTable>> {
        let guard = self.db.lock().unwrap();

        let schema = self.get_schema_for_table(name)?;
        if let Some(schema) = schema {
            guard.open_tree(name).map(|db| {
                Some(SledTable {
                    db: Arc::new(Mutex::new(db)),
                    schema,
                })
            })
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
struct SledTable {
    db: Arc<Mutex<Tree<1024>>>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SledTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        return self.create_physical_plan(projection, self.schema()).await;
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.create_physical_plan_insert(self.schema(), input, insert_op)
            .await
    }
}

impl CustomExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: SledTable) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();

        let properties = {
            let eq_properties = datafusion::physical_expr::EquivalenceProperties::new(schema);
            let partitioning = datafusion::physical_plan::Partitioning::UnknownPartitioning(1);
            let emission_type =
                datafusion::physical_plan::execution_plan::EmissionType::Incremental;
            let boundedness = datafusion::physical_plan::execution_plan::Boundedness::Bounded;
            datafusion::physical_plan::PlanProperties::new(
                eq_properties,
                partitioning,
                emission_type,
                boundedness,
            )
        };

        Self {
            db,
            projected_schema,
            properties,
        }
    }
}

impl SledTable {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }

    pub(crate) async fn create_physical_plan_insert(
        &self,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        insert: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CustomInsertIntoExec::new(
            schema,
            self.clone(),
            input,
            insert,
        )))
    }
}

#[derive(Debug)]
struct CustomInsertIntoExec {
    db: SledTable,
    schema: SchemaRef,
    properties: PlanProperties,
    insert_op: InsertOp,
    input: Arc<dyn ExecutionPlan>,
    count_schema: SchemaRef,
}

impl CustomInsertIntoExec {
    fn new(
        schema: SchemaRef,
        db: SledTable,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Self {
        let properties = {
            let eq_properties =
                datafusion::physical_expr::EquivalenceProperties::new(schema.clone());
            let partitioning = datafusion::physical_plan::Partitioning::UnknownPartitioning(1);
            let emission_type =
                datafusion::physical_plan::execution_plan::EmissionType::Incremental;
            let boundedness = datafusion::physical_plan::execution_plan::Boundedness::Bounded;
            datafusion::physical_plan::PlanProperties::new(
                eq_properties,
                partitioning,
                emission_type,
                boundedness,
            )
        };

        Self {
            db,
            schema,
            properties,
            insert_op,
            input,
            count_schema: make_count_schema(),
        }
    }
}

impl DisplayAs for CustomInsertIntoExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomInsertIntoExec")
    }
}

impl ExecutionPlan for CustomInsertIntoExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let mut data =
            execute_input_stream(self.input.clone(), self.schema.clone(), partition, context)?;

        let count_schema = Arc::clone(&self.count_schema);

        let db = self.db.clone();
        let schema = self.schema.clone();

        let stream = futures::stream::once(async move {
            let mut buffer = Vec::new();

            let mut count = 0;

            while let Some(batch) = data.next().await {
                let batch = batch?;

                let guard = db.db.lock().unwrap();
                let mut writer = FileWriter::try_new(&mut buffer, &schema)?;

                writer.write(&batch)?;
                writer.finish()?;

                guard.insert("test", buffer.as_slice()).unwrap();

                count += batch.num_rows();

                buffer.clear();
            }

            Ok(make_count_batch(count as _))
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).unwrap()
}

fn make_count_schema() -> SchemaRef {
    // Define a schema.
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[derive(Debug)]
struct CustomExec {
    db: SledTable,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let guard = self.db.db.lock().unwrap();

        let values = guard.iter();

        let mut record_batches = Vec::new();
        let mut num_rows = 0;

        for value in values.values() {
            let value = value.unwrap();
            let slice: &[u8] = &value;

            let cursor = Cursor::new(slice);

            let reader = FileReader::try_new(cursor, None).unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                num_rows += batch.num_rows();
                record_batches.push(batch);
            }
        }

        eprintln!("Read: {}", num_rows);

        Ok(Box::pin(MemoryStream::try_new(
            record_batches,
            self.schema(),
            None,
        )?))
    }
}

use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "foo",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    )]));

    let config = sled::Config::new().path("test.db");

    let sled = SledDb::new(&config).unwrap();
    sled.register_table("foo", schema.clone()).unwrap();

    // Register tables into their own namespace:
    let ctx = SessionContext::new();

    let table = sled.get_table("foo").unwrap().unwrap();

    ctx.register_table("foo", Arc::new(table)).unwrap();

    let df = ctx
        .sql(
            r#"
INSERT INTO foo
VALUES
(10),
(20),
(30),
(40),
(50);
"#,
        )
        .await
        .unwrap();

    df.show().await.unwrap();

    let df = ctx.sql("SELECT * FROM foo").await.unwrap();

    df.show().await.unwrap();

    // Ok(())
}
