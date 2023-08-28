use std::sync::Arc;

use crate::collector::{Collector, SegmentCollector};
use crate::columnar::DynamicColumn;
use crate::fastfield::Column;
use crate::{DocId, Score, SegmentOrdinal, SegmentReader};

use arrow::array::{self, Array};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use columnar::ColumnType;
use itertools::Itertools;

// TODO support all types

// TODO collect arrays of docs and process batches ()

enum ColumnCopyBuilder {
    U64(String, Column<u64>, array::UInt64Builder),
    U64List(
        String,
        Column<u64>,
        array::ListBuilder<array::UInt64Builder>,
    ),
    // TODO other types
}

impl ColumnCopyBuilder {
    fn col2field<T>(col: &Column<T>, name: impl Into<String>, tgt_array: &dyn Array) -> Field {
        Field::new(
            name.into(),
            tgt_array.data_type().clone(),
            col.get_cardinality().is_optional(),
        )
    }

    fn to_arrow_field(&self) -> Field {
        match self {
            Self::U64(n, c, b) => Self::col2field(c, n, &b.finish_cloned()),
            Self::U64List(n, c, b) => Self::col2field(c, n, &b.finish_cloned()),
        }
    }

    /// Copies the docs one by one from the Tantivy fast col to an Arrow array
    ///
    /// Note that this row-wise copying might be ineficient because of the enum
    /// switching + vtables in the Column type. See also collect_block() in
    /// SegmentCollector and ColumnBlockAccessor columnar struct
    fn copy(&mut self, doc_id: DocId) {
        match self {
            ColumnCopyBuilder::U64(_, c, b) => b.append_option(c.values_for_doc(doc_id).next()),
            ColumnCopyBuilder::U64List(_, c, b) => {
                b.append_value(c.values_for_doc(doc_id).map(|v| Some(v)))
            }
        };
    }
}

pub struct ArrowSegmentCollector {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    transfers: Vec<ColumnCopyBuilder>,
    batch_size: usize,
    transfer_idx: usize,
}

impl ArrowSegmentCollector {
    pub fn new(columns: Vec<(String, DynamicColumn)>, batch_size: usize) -> Self {
        let transfers = columns
            .into_iter()
            .map(|(name, col)| match col {
                DynamicColumn::U64(c) => {
                    let inner_builder = array::UInt64Builder::with_capacity(batch_size);
                    if c.index.get_cardinality().is_multivalue() {
                        ColumnCopyBuilder::U64List(name, c, array::ListBuilder::new(inner_builder))
                    } else {
                        ColumnCopyBuilder::U64(name, c, inner_builder)
                    }
                }
                _ => unimplemented!("unknown type"),
            })
            .collect::<Vec<_>>();
        let fields = transfers
            .iter()
            .map(ColumnCopyBuilder::to_arrow_field)
            .collect::<Vec<_>>();
        Self {
            batches: Vec::new(),
            schema: Arc::new(Schema::new(fields)),
            transfers,
            batch_size,
            transfer_idx: 0,
        }
    }

    fn build_batch(&mut self) -> crate::Result<()> {
        let mut cols = Vec::with_capacity(self.transfers.len());
        for transfer in &mut self.transfers {
            match transfer {
                ColumnCopyBuilder::U64(_, _, b) => {
                    cols.push(Arc::new(b.finish()) as Arc<dyn array::Array>);
                }
                ColumnCopyBuilder::U64List(_, _, b) => {
                    cols.push(Arc::new(b.finish()) as Arc<dyn array::Array>)
                }
            }
        }
        let batch = RecordBatch::try_new(self.schema.clone(), cols).map_err(|e| {
            crate::error::TantivyError::SchemaError(format!(
                "mapping to Arrow RecordBatch failed: {}",
                e.to_string()
            ))
        })?;
        self.batches.push(batch);
        Ok(())
    }
}

impl SegmentCollector for ArrowSegmentCollector {
    type Fruit = crate::Result<Vec<RecordBatch>>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        if self.transfer_idx == self.batch_size {
            // TODO cannot propagate error here, which hints that it might be
            // better to the batch building at harvest time. Or panic?
            self.build_batch().map_err(|e| error!("{}", e)).ok();
            self.transfer_idx = 0;
        }
        for transfer in &mut self.transfers {
            transfer.copy(doc_id)
        }
    }

    fn harvest(mut self) -> crate::Result<Vec<RecordBatch>> {
        if self.transfer_idx > 0 {
            self.build_batch()?;
        }
        Ok(self.batches)
    }
}

/// A collector for exporting FAST fields as Arrow record batches
#[derive(Clone)]
pub struct ArrowCollector {
    /// The list of fast fields we want to collact as Arrow arrays. The Arrow
    /// schema field names will be the same as these provided names.
    pub fast_fields_to_collect: Vec<(String, ColumnType)>,
    /// The maximum Arrow batch sizes. The last batch of each segment will
    /// usually be smaller.
    pub batch_size: usize,
}

impl Collector for ArrowCollector {
    type Child = ArrowSegmentCollector;
    type Fruit = Vec<RecordBatch>;

    fn for_segment(
        &self,
        _segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let columns = self
            .fast_fields_to_collect
            .iter()
            .map(|(cname, ctype)| -> crate::Result<(String, DynamicColumn)> {
                let col_res = segment_reader.fast_fields().column_opt(cname);
                let col = col_res?.ok_or_else(|| {
                    // TODO: should error here? the return type seems to be an
                    // Option for future support of schema evolution
                    crate::error::TantivyError::FieldNotFound(format!(
                        "fastfield {} of type {} not found",
                        cname, ctype
                    ))
                })?;
                Ok((cname.clone(), col.into()))
            })
            .collect::<crate::Result<_>>()?;

        Ok(ArrowSegmentCollector::new(columns, self.batch_size))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<crate::Result<Vec<RecordBatch>>>,
    ) -> crate::Result<Self::Fruit> {
        segment_fruits.into_iter().flatten_ok().collect()
    }
}
