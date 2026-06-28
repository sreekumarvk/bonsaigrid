use std::collections::HashMap;

/// Result type for MapStore operations.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// SPI for loading map entries from an external data store (e.g. database).
pub trait MapLoader: Send + Sync {
    /// Loads the value of a given key. If not found, returns None.
    fn load(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Loads multiple keys at once.
    fn load_all(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let mut map = HashMap::new();
        for key in keys {
            if let Some(val) = self.load(key)? {
                map.insert(key.to_vec(), val);
            }
        }
        Ok(map)
    }

    /// Loads all keys available in the external data store.
    fn load_all_keys(&self) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }
}

/// SPI for storing map entries to an external data store.
pub trait MapStore: MapLoader {
    /// Stores a single key-value pair.
    fn store(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Stores multiple key-value pairs.
    fn store_all(&self, entries: HashMap<&[u8], &[u8]>) -> Result<()> {
        for (k, v) in entries {
            self.store(k, v)?;
        }
        Ok(())
    }

    /// Deletes a key from the data store.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Deletes multiple keys from the data store.
    fn delete_all(&self, keys: &[&[u8]]) -> Result<()> {
        for k in keys {
            self.delete(k)?;
        }
        Ok(())
    }
}
