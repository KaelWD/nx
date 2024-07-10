use std::fs::{create_dir_all, read_to_string, write};
use std::path::{Path, PathBuf};

use fs_extra::remove_items;
use rusqlite::{params, Connection, OptionalExtension};

use crate::native::cache::expand_outputs::_expand_outputs;
use crate::native::cache::file_ops::_copy;
use crate::native::db::connect_to_nx_db;
use crate::native::utils::Normalize;

#[napi(object)]
#[derive(Default, Clone)]
pub struct TaskResult {
    pub hash: String,
    pub project: String,
    pub target: String,
    pub configuration: Option<String>,
}

#[napi(object)]
#[derive(Default, Clone, Debug)]
pub struct CachedResult {
    pub code: i16,
    pub terminal_output: String,
    pub outputs_path: String,
}

#[napi]
pub struct NxCache {
    pub cache_directory: String,
    workspace_root: PathBuf,
    cache_path: PathBuf,
    db: Connection,
}

#[napi]
impl NxCache {
    #[napi(constructor)]
    pub fn new(
        workspace_root: String,
        cache_path: String,
        workspace_data_path: String,
    ) -> anyhow::Result<Self> {
        let machine_id = machine_uid::get().unwrap_or(String::from("machine"));
        let cache_path = PathBuf::from(&cache_path).join(&machine_id);
        let r = Self {
            db: connect_to_nx_db(&workspace_data_path)?,
            workspace_root: PathBuf::from(workspace_root),
            cache_directory: cache_path.to_normalized_string(),
            cache_path,
        };

        r.setup()?;

        Ok(r)
    }

    fn setup(&self) -> anyhow::Result<()> {
        self.db
            .execute_batch(
                "BEGIN;
                CREATE TABLE IF NOT EXISTS cache_outputs (
                    hash    TEXT PRIMARY KEY NOT NULL,
                    code   INTEGER NOT NULL,
                    project  TEXT NOT NULL,
                    target  TEXT NOT NULL,
                    configuration  TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE VIEW IF NOT EXISTS outdated AS
                    SELECT * FROM cache_outputs WHERE accessed_at < datetime('now', '-7 days');
            COMMIT;
            ",
            )
            .map_err(anyhow::Error::from)
    }

    #[napi]
    pub fn get(&mut self, task: TaskResult) -> anyhow::Result<Option<CachedResult>> {
        let task_dir = self.cache_path.join(&task.hash);

        let t = self.db.transaction()?;

        let r = t
            .query_row(
                "SELECT code FROM cache_outputs WHERE hash = ?1",
                params![task.hash],
                |row| {
                    let code: i16 = row.get(0)?;

                    let terminal_output_path = task_dir.join("terminalOutput");
                    let terminal_output =
                        read_to_string(terminal_output_path).unwrap_or(String::from(""));

                    Ok(CachedResult {
                        code,
                        terminal_output,
                        outputs_path: task_dir.join("outputs").to_normalized_string(),
                    })
                },
            )
            .optional()
            .map_err(anyhow::Error::new)?;

        if r.is_some() {
            t.execute(
                "UPDATE cache_outputs
                    SET accessed_at = CURRENT_TIMESTAMP WHERE hash = ?1",
                params![task.hash],
            )?;
        }
        t.commit()?;

        Ok(r)
    }

    #[napi]
    pub fn put(
        &mut self,
        task: TaskResult,
        terminal_output: String,
        outputs: Vec<String>,
        code: i16,
    ) -> anyhow::Result<()> {
        let task_dir = self.cache_path.join(&task.hash);

        // Remove the task directory
        remove_items(&[&task_dir])?;
        // Create the task directory again
        create_dir_all(&task_dir)?;

        // Write the terminal outputs into a file
        write(task_dir.join("terminalOutput"), terminal_output)?;

        // Create a directory for the outputs
        create_dir_all(task_dir.join("outputs"))?;

        // Expand the outputs
        let expanded_outputs = _expand_outputs(&self.workspace_root, outputs)?;

        // Copy the outputs to the cache
        for expanded_output in expanded_outputs.iter() {
            let p = self.workspace_root.join(expanded_output);
            if p.exists() {
                let cached_outputs_dir = task_dir.join("outputs").join(expanded_output);
                _copy(p, cached_outputs_dir)?;
            }
        }

        self.record_from_cache(task, code)?;
        Ok(())
    }

    #[napi]
    pub fn get_from_cache_directory(
        &self,
        task: TaskResult,
    ) -> anyhow::Result<Option<CachedResult>> {
        let task_dir = self.cache_path.join(&task.hash);

        if task_dir.exists() {
            let terminal_output_path = task_dir.join("terminalOutput");
            let terminal_output = read_to_string(terminal_output_path).unwrap_or(String::from(""));

            let code_path = task_dir.join("code");
            let code: i16 = read_to_string(code_path)
                .unwrap_or(String::from("0"))
                .parse()
                .unwrap_or_else(|_| 0);

            self.record_from_cache(task, code);

            Ok(Some(CachedResult {
                code,
                terminal_output,
                outputs_path: task_dir.join("outputs").to_normalized_string(),
            }))
        } else {
            Ok(None)
        }
    }

    #[napi]
    pub fn get_task_outputs_path(&self, hash: String) -> String {
        self.cache_path
            .join(hash)
            .join("terminalOutput")
            .to_normalized_string()
    }

    fn record_from_cache(&self, task: TaskResult, code: i16) -> anyhow::Result<()> {
        self.db.execute(
            "INSERT INTO cache_outputs
                (hash, code, project, target, configuration)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                task.hash,
                code,
                task.project,
                task.target,
                task.configuration
            ],
        )?;
        Ok(())
    }

    #[napi]
    pub fn copy_files_from_cache(
        &self,
        cached_result: CachedResult,
        outputs: Vec<String>,
    ) -> anyhow::Result<()> {
        let outputs_path = Path::new(&cached_result.outputs_path);

        let expanded_outputs = _expand_outputs(outputs_path, outputs)?;

        remove_items(
            expanded_outputs
                .iter()
                .map(|p| outputs_path.join(p))
                .collect::<Vec<_>>()
                .as_slice(),
        )?;

        for expanded_output in expanded_outputs.iter() {
            let cached_path = outputs_path.join(expanded_output);
            if cached_path.exists() {
                let source_path = self.workspace_root.join(expanded_output);
                _copy(cached_path, source_path)?;
            }
        }

        Ok(())
    }

    #[napi]
    pub fn remove_old_cache_records(&self) -> anyhow::Result<()> {
        let outdated_cache = self
            .db
            .prepare("SELECT hash FROM outdated")?
            .query_map(params![], |row| {
                let hash: String = row.get(0)?;

                Ok(self.cache_path.join(&hash))
            })?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();

        dbg!(&outdated_cache);
        remove_items(&outdated_cache);

        // remove_items(&r)?;
        self.db.execute(
            "DELETE FROM cache_outputs WHERE hash IN (SELECT hash FROM outdated)",
            params![],
        );

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use assert_fs::prelude::*;
    use assert_fs::TempDir;

    use super::*;

    #[test]
    fn test_setup() {
        let temp = TempDir::new().unwrap();

        dbg!(&temp);
        temp.child("cache").create_dir_all().unwrap();
        let workspace_root = temp.to_normalized_string();
        let cache_path = temp.join("cache");
        let mut cache = NxCache::new(
            workspace_root,
            cache_path.clone().to_normalized_string(),
            cache_path.clone().to_normalized_string(),
        )
        .unwrap();

        let output = temp.child("dist/output.txt");
        output.touch().unwrap();

        cache
            .put(
                TaskResult {
                    hash: "123".to_string(),
                    project: "proj".to_string(),
                    target: "build".to_string(),
                    configuration: Some("build".to_string()),
                },
                "terminal output".to_string(),
                vec!["outputs".to_string()],
                0,
            )
            .unwrap();
        cache
            .put(
                TaskResult {
                    hash: "234".to_string(),
                    project: "proj".to_string(),
                    target: "build2".to_string(),
                    configuration: None,
                },
                "terminal output".to_string(),
                vec!["outputs".to_string()],
                0,
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(2));

        dbg!(cache.get(TaskResult {
            hash: "234".into(),
            project: "proj".to_string(),
            target: "build2".to_string(),
            configuration: None,
        }));
        temp.into_persistent();
    }
}
