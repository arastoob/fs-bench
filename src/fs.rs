use crate::error::Error;
use crate::progress::Progress;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{create_dir, create_dir_all, remove_dir_all, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

///
/// The fs operations
///
pub struct Fs;

impl Fs {
    pub fn make_dir<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        create_dir(path)
    }

    pub fn make_dir_all<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        create_dir_all(path)
    }

    pub fn make_file<P: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>>(
        path: P,
    ) -> Result<File, std::io::Error> {
        // create the parent directory hierarchy if needed
        let path = Path::new(&path);
        let path = PathBuf::from(path);
        let mut parents = path.clone();
        parents.pop();
        if !parents.exists() {
            Fs::make_dir_all(&parents)?;
        }

        File::create(path)
    }

    pub fn open_file<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        OpenOptions::new()
            .write(true)
            .read(true)
            .append(false)
            .open(path)
    }

    pub fn open_dir<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        OpenOptions::new().read(true).open(path)
    }

    pub fn open_write<P: AsRef<Path>>(
        path: P,
        content: &mut Vec<u8>,
    ) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(false)
            .open(path)?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn open_write_at<P: AsRef<Path>>(
        path: P,
        content: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .append(false)
            .open(path)?;
        file.seek(SeekFrom::Start(offset))?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn write(file: &mut File, content: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        file.write_all(&content)?;
        file.flush()?;
        Ok(content.len())
    }

    pub fn write_at(
        file: &mut File,
        content: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        file.seek(SeekFrom::Start(offset))?;

        let size = file.write(&content)?;
        file.flush()?;
        Ok(size)
    }

    pub fn open_read<P: AsRef<Path>>(
        path: P,
        read_buffer: &mut Vec<u8>,
    ) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.read(read_buffer)
    }

    pub fn open_read_at<P: AsRef<Path>>(
        path: P,
        read_buffer: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read(read_buffer)
    }

    pub fn read(file: &mut File, read_buffer: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        file.read_exact(read_buffer)?;
        Ok(read_buffer.len())
    }

    pub fn read_at(
        file: &mut File,
        read_buffer: &mut Vec<u8>,
        offset: u64,
    ) -> Result<usize, std::io::Error> {
        file.seek(SeekFrom::Start(offset))?;
        file.read(read_buffer)
    }

    pub fn remove_file<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        remove_file(path)
    }

    pub fn remove_dir<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        remove_dir_all(path)
    }

    pub fn metadata<P: AsRef<Path>>(path: P) -> Result<std::fs::Metadata, std::io::Error> {
        std::fs::metadata(path)
    }

    pub fn rename<F: AsRef<Path>, T: AsRef<Path>>(from: F, to: T) -> Result<(), std::io::Error> {
        std::fs::rename(from, to)
    }

    pub fn truncate<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        let file = Fs::open_file(path)?;
        file.set_len(0)
    }

    pub fn copy<F: AsRef<Path> + std::convert::AsRef<std::ffi::OsStr>, T: AsRef<Path>>(
        from: F,
        to: T,
    ) -> Result<(), std::io::Error> {
        let from = Path::new(&from);
        let from = PathBuf::from(from);
        if from.is_file() {
            std::fs::copy(from, to)?;
        } else {
            Fs::copy_dir_all(from, to)?;
        }

        Ok(())
    }

    fn copy_dir_all<F: AsRef<Path>, T: AsRef<Path>>(from: F, to: T) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(&from)?;
        for entry in std::fs::read_dir(&from)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                Fs::copy_dir_all(entry.path(), from.as_ref().join(entry.file_name()))?;
            } else {
                std::fs::copy(entry.path(), &to)?;
            }
        }
        Ok(())
    }

    pub fn cleanup(path: &PathBuf) -> Result<(), Error> {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(ProgressStyle::default_spinner().template("{spinner} {msg}"));
        spinner.set_message(format!(
            "clean up {}",
            path.to_str().ok_or(Error::Unknown(
                "failed to convert PathBuf to String".to_string()
            ))?
        ));
        let progress = Progress::start(spinner);

        if path.exists() {
            remove_dir_all(path).unwrap();
        }
        // finish the progress
        progress.finish_and_clear()?;

        Ok(())
    }

    pub fn path_to_str(path: &PathBuf) -> Result<&str, Error> {
        path.as_os_str().to_str().ok_or(Error::Unknown(
            "failed to convert PathBuf to String".to_string(),
        ))
    }

    // change the path to a path relative to the base_path
    pub fn map_path(base_path: &PathBuf, path: &str) -> Result<PathBuf, Error> {
        let mut new_path = base_path.clone();

        let mut path = path.to_string();
        if path.starts_with("/") {
            path = path[1..].to_string();
        }
        new_path.push(path);

        Ok(new_path)
    }
}
