use std::path::{Path};
use std::fs::create_dir_all;
use rusqlite::{Connection};

pub fn connect_to_nx_db<P>(cache_dir: P) -> anyhow::Result<Connection> where P: AsRef<Path>{
    let machine_id = machine_uid::get().unwrap_or(String::from("machine"));
    let db_path = cache_dir.as_ref().join(format!("{}.db", machine_id));
    create_dir_all(cache_dir)?;
    let c = Connection::open(db_path).map_err(anyhow::Error::from)?;

    c.pragma_update(None, "journal_mode", &"WAL")?;

    Ok(c)
}
