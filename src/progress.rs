use crate::Error;
use indicatif::ProgressBar;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time::Duration;

pub struct Progress {
    sender: Sender<FinishType>,
}

#[allow(dead_code)]
enum FinishType {
    Finish,
    FinishAndClear,
    FinishWithMessage(String),
    AbandonWithMessage(String),
}

#[allow(dead_code)]
impl Progress {
    pub fn start(progress_bar: ProgressBar) -> Self {
        let (sender, receiver) = channel();

        let p = progress_bar.clone();
        thread::spawn(move || {
            // increment the progress bar until receiving a signal
            loop {
                match receiver.try_recv() {
                    Ok(finish_type) => {
                        match finish_type {
                            FinishType::Finish => p.finish(),
                            FinishType::FinishAndClear => p.finish_and_clear(),
                            FinishType::FinishWithMessage(msg) => p.finish_with_message(msg),
                            FinishType::AbandonWithMessage(msg) => p.abandon_with_message(msg),
                        }
                        break;
                    }
                    _ => {
                        thread::sleep(Duration::from_millis(50));
                        p.inc(1);
                    }
                }
            }
        });

        Self { sender }
    }

    pub fn finish(&self) -> Result<(), Error> {
        self.sender.send(FinishType::Finish)?;

        Ok(())
    }

    pub fn finish_and_clear(&self) -> Result<(), Error> {
        self.sender.send(FinishType::FinishAndClear)?;

        Ok(())
    }

    pub fn finish_with_message(&self, msg: &str) -> Result<(), Error> {
        self.sender
            .send(FinishType::FinishWithMessage(msg.to_string()))?;

        Ok(())
    }

    pub fn abandon_with_message(&self, msg: &str) -> Result<(), Error> {
        self.sender
            .send(FinishType::AbandonWithMessage(msg.to_string()))?;

        Ok(())
    }
}
