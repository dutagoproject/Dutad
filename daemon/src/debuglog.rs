use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::sync::{Mutex, OnceLock};
use time::{format_description, OffsetDateTime, UtcOffset};

static DEBUGLOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();
static ERRORLOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

pub fn init(data_dir: &str) -> io::Result<()> {
    let data_dir = data_dir.trim_end_matches('/');
    fs::create_dir_all(data_dir)?;
    let debug_path = format!("{}/debug.log", data_dir);
    let error_path = format!("{}/error.log", data_dir);
    let debug_f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&debug_path)?;
    let error_f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&error_path)?;
    let _ = DEBUGLOG.set(Mutex::new(debug_f));
    let _ = ERRORLOG.set(Mutex::new(error_f));
    Ok(())
}

fn ts_prefix() -> String {
    let format =
        format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").ok();
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    let rendered = match format {
        Some(fmt) => OffsetDateTime::now_utc()
            .to_offset(offset)
            .format(&fmt)
            .ok(),
        None => None,
    }
    .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
    format!("[{}]", rendered)
}

fn write_line(target: &OnceLock<Mutex<std::fs::File>>, args: &std::fmt::Arguments) -> bool {
    if let Some(m) = target.get() {
        let mut f = match m.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let _ = writeln!(f, "{} {}", ts_prefix(), args);
        let _ = f.flush();
        return true;
    }
    false
}

pub fn log_debug_line(args: std::fmt::Arguments) {
    if write_line(&DEBUGLOG, &args) {
        return;
    }
    eprintln!("{} {}", ts_prefix(), args);
}

pub fn log_error_line(args: std::fmt::Arguments) {
    let wrote_debug = write_line(&DEBUGLOG, &args);
    let wrote_error = write_line(&ERRORLOG, &args);
    if wrote_debug || wrote_error {
        return;
    }
    eprintln!("{} {}", ts_prefix(), args);
}

pub fn log_warn_line(args: std::fmt::Arguments) {
    if write_line(&DEBUGLOG, &args) {
        return;
    }
    eprintln!("{} {}", ts_prefix(), args);
}

#[macro_export]
macro_rules! dlog {
    ($($arg:tt)*) => {{
        $crate::debuglog::log_debug_line(format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! edlog {
    ($($arg:tt)*) => {{
        $crate::debuglog::log_error_line(format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! wlog {
    ($($arg:tt)*) => {{
        $crate::debuglog::log_warn_line(format_args!($($arg)*));
    }};
}
