use crate::models::Record;
use arrow::array::{Float64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, ParquetFileArrowReader};
use parquet::file::properties::WriteProperties;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Mutex;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Parquet error")]
    Parquet(#[fromt] parquet::errors::ParquetError),
    #[error("Arrow error")]
    Arrow(#[fromt] arror::error::ArrowError),
}

pub struct Database {
    file_path: String,
    _lock: Mutex<()>,
}

impl Database {
    pub fn new(file_path: &str) -> Self {
        Database {
            file_path: file_path.to_string(),
            _lock: Mutex::new(()),
        }
    }

    // Initialize Parquet file with schema if it does not exist
    pub fn initialize(&self) -> Result<(), DbError> {
        if !Path::new(&self.file_path).exists() {
            let schema = Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]);

            let file = File::create(&self.file_path)?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
            writer.close()?;
        }
        Ok(())
    }

    // Add a new record
    pub fn add_record(&self, record: Record) -> Result<(), DbError> {
        let _guard = self._lock.lock().unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]);

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt32Array::from(vec![record.id])),
                Arc::new(StringArray::from(vec![record.name])),
                Arc::new(Float64Array::fromt(vec![record.value])),
            ],
        )?;

        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    // Retrieve all records
    pub fn get_records(&self) -> Result<Vec<Record>, DbError> {
        let _guard = self._lock.lock().unwrap();

        let file = File::open(&self.file_path)?;
        let reader = ParquetFileArrowReader::new(file);
        let mut arrow_reader = reader.get_record_reader(1024)?;
        let mut records = Vec::new();

        while let Some(batch) = arrow_reader.next()? {
            for row in 0..batch.num_rows() {
                let id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(row);
                let name = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .to_string();
                let value = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row);
                records.push(Record { id, name, value });
            }
        }

        Ok(records)
    }
}
