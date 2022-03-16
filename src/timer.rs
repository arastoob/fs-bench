use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

pub struct Timer {
    runtime: Duration,
    finished: Arc<RwLock<bool>>,
}

impl Timer {

    pub fn new(runtime: Duration) -> Self {
        Self {
            runtime,
            finished: Arc::new(RwLock::new(false)),
        }
    }

    pub fn start(&self) {
        let rt = self.runtime;
        let finished = self.finished.clone();
        thread::spawn(move || {
            thread::sleep(rt);
            *finished.write().unwrap() = true;
        });
    }

    pub fn finished(&self) -> bool {
        *self.finished.read().unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::time::Duration;
    use crate::timer::Timer;

    #[test]
    fn timer() {
        let timer = Timer::new(Duration::from_secs(1));
        timer.start();
        assert_eq!(timer.finished(), false);

        thread::sleep(Duration::from_millis(500));
        assert_eq!(timer.finished(), false);

        thread::sleep(Duration::from_millis(400));
        assert_eq!(timer.finished(), false);

        thread::sleep(Duration::from_millis(500));
        assert_eq!(timer.finished(), true);

        thread::sleep(Duration::from_millis(500));
        assert_eq!(timer.finished(), true);
    }
}