use std::io::Write;

pub(crate) fn setup_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stdout)
        .format(|buf, record| {
            let ts = buf.timestamp_micros();
            writeln!(
                buf,
                "[{} {}{}{:#} {:?} {} {}:{}] {}",
                ts,
                buf.default_level_style(record.level()),
                record.level(),
                buf.default_level_style(record.level()),
                std::thread::current().id(),
                record.target(),
                record.file().unwrap_or("<unknown>"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();
}
