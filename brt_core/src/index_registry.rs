use crate::indexable::IndexableCollection;
use crate::polars_documents::*;
use crate::wrappers::TantivyIndexWrapper;
use anyhow::Result;
use polars::prelude::DataFrame;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct IndexRegistry {
    pub indices: RwLock<HashMap<String, Box<TantivyIndexWrapper>>>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        IndexRegistry {
            indices: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_index_names(&self) -> Result<Vec<String>, String> {
        let binding = self.indices.read();
        match binding {
            Ok(binding) => Ok(binding.keys().cloned().collect()),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn initialize_index(
        &self,
        name: String,
        name_field: String,
        fields: Vec<String>,
    ) -> Result<(), String> {
        let index_wrapper = TantivyIndexWrapper::new(name.clone(), name_field, fields);
        self.indices
            .write()
            .unwrap()
            .insert(name, Box::new(index_wrapper));
        Ok(())
    }

    pub fn index_document(&self, name: String, document_map: HashMap<&str, String>) -> Result<()> {
        let binding = self.indices.read().unwrap();
        let index_wrapper = binding.get(&name).unwrap();

        Ok(index_wrapper.add_document(document_map)?)
    }

    pub fn search(&self, name: String, query: String) -> Result<Vec<String>> {
        let binding = self.indices.read().unwrap();
        let index_wrapper = binding.get(&name.to_string()).unwrap();

        Ok(index_wrapper.search(&query.to_string())?)
    }

    pub fn index_df(&self, name: String, df: &DataFrame) -> Result<()> {
        let binding = self.indices.read().unwrap();
        let index_wrapper = binding.get(&name.to_string()).unwrap();

        df.index_collection(index_wrapper)
    }
}
