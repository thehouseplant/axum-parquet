use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Record {
    pub id: u32,
    pub name: String,
    pub value: f64,
}
