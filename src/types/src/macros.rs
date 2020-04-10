#[macro_export]
macro_rules! log_on_err {
    ($x:expr) => {
        if let Err(err) = $x {
            log::error!("{}", err);
        }
    };

    ($x:expr,$msg:expr) => {
        if let Err(err) = $x {
            log::error!($msg, err);
        }
    };
}

#[macro_export]
macro_rules! log_actions {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr, $x5:expr, $x6:expr) => {
        log::debug!(
            "{:<20}: [add:{}, mod:{}, del:{}, skip:{}]",
            format!("{}({})", $x1, $x2),
            $x3,
            $x4,
            $x5,
            $x6
        );
    };
}

#[macro_export]
macro_rules! print_cli_err {
    ($x:expr) => {
        eprintln!("{}", $x);
    };
}

#[macro_export]
macro_rules! print_cli_ok {
    () => {
        println!("\x1B[32mOk!\x1B[0m");
    };
}

#[macro_export]
macro_rules! print_ok_msg {
    ($x:expr, $y:expr) => {
        println!("\x1B[32m{}\x1B[0m: {}", $x, $y);
    };
}