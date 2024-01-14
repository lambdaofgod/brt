use crate::polars_documents::index_df_rows;
use crate::wrappers::{BatchIndexer, TantivyIndexWrapper};
use anyhow::Result;
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::fs::read_to_string;
use std::path::PathBuf;

pub trait IndexableCollection {
    fn index_collection(&self, index: &TantivyIndexWrapper) -> Result<()>;
}

fn load_file_doc(path: &str) -> Result<HashMap<&str, String>> {
    let mut doc = HashMap::new();
    doc.insert("name", path.to_string());
    let contents = read_to_string(path).unwrap();
    doc.insert("contents", contents);
    Ok(doc)
}

impl IndexableCollection for PathBuf {
    fn index_collection(&self, index: &TantivyIndexWrapper) -> Result<()> {
        let mut indexer = BatchIndexer::new(index);
        for p in self {
            let doc_path_str = p.to_str().unwrap();
            let doc = load_file_doc(doc_path_str)?;
            indexer.add_document(doc)?;
        }
        Ok(indexer.commit()?)
    }
}

impl IndexableCollection for DataFrame {
    fn index_collection(&self, tantivy_index: &TantivyIndexWrapper) -> Result<()> {
        index_df_rows(tantivy_index, self);
        Ok(())
    }
}
