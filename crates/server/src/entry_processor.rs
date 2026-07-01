use std::fmt;

/// An error that occurs during entry processor execution.
#[derive(Debug)]
pub struct ProcessorError(pub String);

impl fmt::Display for ProcessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A server-side mutator that applies to a map entry.
pub trait EntryProcessor {
    /// Applies the processor to the entry.
    /// Returns `(new_value, result)` where `new_value` is the updated value (or None if unchanged/removed),
    /// and `result` is the object to send back to the client.
    fn process(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), ProcessorError>;
}

/// Tries to parse a built-in IdentifiedDataSerializable entry processor.
pub fn parse_processor(_data: &[u8]) -> Result<Box<dyn EntryProcessor>, ProcessorError> {
    // Basic structure of Hazelcast IDS: factory_id (i32), class_id (i32), ...
    // If it's a generic Java Serializable, we won't be able to run it natively without a Java environment.
    Err(ProcessorError(
        "EntryProcessor execution is not fully supported for this processor type in BonsaiGrid"
            .to_string(),
    ))
}
