use crate::indexable::IndexableCollection;
use crate::wrappers::{BatchIndexer, TantivyIndexWrapper};
use anyhow::Result;
use indicatif::ProgressIterator;
use polars::datatypes::DataType;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;

pub(crate) fn index_df_rows(tantivy_index: &TantivyIndexWrapper, df: &DataFrame) -> Result<()> {
    // Initialize an empty vector of hashmaps
    // Get the number of rows
    let num_rows = df.height();

    let index_fields = tantivy_index.field_names();
    let columns = get_columns(df, index_fields);

    // HOW TO MAKE THIS FASTER?
    // [x] parallel iterator?
    // iterating over chunks?

    let mut batch_indexer = BatchIndexer::new(tantivy_index);

    let parallel_range = (0..num_rows).into_par_iter();
    parallel_range.for_each(|row_index| {
        let row_hashmap = get_row_hashmap(df, &columns, row_index);
        batch_indexer.add_document(row_hashmap);
    });
    batch_indexer.commit()?;
    Ok(())
}

type Columns<'a> = Vec<(&'a ChunkedArray<StringType>, String)>;

fn get_columns<'a>(df: &'a DataFrame, index_fields: Vec<&str>) -> Columns<'a> {
    df.get_columns()
        .iter()
        .filter(|col| col.dtype() == &DataType::String && index_fields.contains(&col.name()))
        .map(|col| (col.str().unwrap(), col.name().to_string()))
        .collect::<Vec<_>>()
}

fn get_row_hashmap<'a>(
    df: &'a DataFrame,
    columns: &'a Columns<'a>,
    row_index: usize,
) -> HashMap<&'a str, String> {
    let mut row_hashmap: HashMap<&str, String> = HashMap::new();
    columns.iter().for_each(|(col, name)| match col.dtype() {
        DataType::String => {
            let val = col.get(row_index);
            let str = val.unwrap_or("None").clone();
            row_hashmap.insert(&name, str.to_string());
        }
        _ => (),
    });
    row_hashmap
}

pub fn df_rows_foreach(
    df: &DataFrame,
    function: &dyn Fn(HashMap<String, String>) -> Result<()>,
) -> Result<()> {
    // Initialize an empty vector of hashmaps
    // Get the number of rows
    let num_rows = df.height();

    let columns = df
        .get_columns()
        .iter()
        .filter(|col| col.dtype() == &DataType::String)
        .map(|col| col.str().unwrap())
        .collect::<Vec<_>>();
    // Iterate over each row
    for row_index in 0..num_rows {
        // Create a new hashmap for each row
        let mut row_hashmap: HashMap<String, String> = HashMap::new();

        // Iterate over each of the columns, add name-value entries to the hashmap
        columns.iter().for_each(|col| match col.dtype() {
            DataType::String => {
                let name = col.name().to_string();
                let val = col.get(row_index);
                let str = val.unwrap_or("None").clone();
                row_hashmap.insert(name, str.to_string());
            }
            _ => (),
        });

        // Push the hashmap into the vector
        function(row_hashmap)?;
    }
    Ok(())
}

#[test]
fn test_indexing_df() {
    let index = TantivyIndexWrapper::new(
        "test_index".to_string(),
        "title".to_string(),
        vec!["body".to_string()],
    );

    let df = load_test_df();
    df.index_collection(&index).unwrap();

    let num_docs = index.num_docs().unwrap();
    assert_eq!(num_docs, 2);
}

fn load_test_df() -> DataFrame {
    let df = DataFrame::new(vec![
        Series::new("title", &["test title", "test title 2"]),
        Series::new("body", &["test body", "test body 2"]),
    ])
    .unwrap();
    df
}
