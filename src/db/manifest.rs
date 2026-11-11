use std::{
    fs, io,
    sync::atomic::{AtomicU64, Ordering},
};

pub struct Manifest {
    file_name: AtomicU64,
}

impl Manifest {
    pub fn new(start_num: u64) -> Self {
        Manifest {
            file_name: AtomicU64::new(start_num),
        }
    }

    pub fn generate_file_name(&mut self) -> String {
        let file = self.file_name.load(Ordering::SeqCst);
        format!("{file:06}_level{file:03}.sst")
    }

    pub fn increment_count(&mut self) -> std::result::Result<(), io::Error> {
        let file_path = "MANIFEST/current.sst";
        let content = fs::read_to_string(file_path)?;
        self.file_name.fetch_add(1, Ordering::SeqCst);

        let update_content: Vec<String> = content
            .lines()
            .map(|line| {
                if let Some((key, mut value)) = line.split_once('=') {
                    if key.trim() == "current_num" {
                        value = value.trim();

                        if let Ok(num) = value.trim().parse::<u64>() {
                            return format!("current_num = {}", num + 1);
                        }
                    }
                }
                line.to_string()
            })
            .collect();

        fs::write(file_path, update_content.join("\n"))?;
        Ok(())
    }

    pub fn load_count(&mut self) -> std::result::Result<(), io::Error> {
        let file_path = "MANIFEST/current.sst";
        let content = fs::read_to_string(file_path)?;
        let mut found = false;

        let mut update_content: Vec<String> = content
            .lines()
            .map(|line| {
                if let Some((key, mut value)) = line.split_once('=') {
                    if key.trim() == "current_num" {
                        found = true;

                        value = value.trim();

                        if let Ok(num) = value.trim().parse::<u64>() {
                            self.file_name.store(num, Ordering::Relaxed);
                        }
                    }
                }
                line.to_string()
            })
            .collect();

        if !found {
            update_content.push("current_num = 0;".to_string());
            fs::write(file_path, update_content.join("").trim())?;
        }

        Ok(())
    }
}
