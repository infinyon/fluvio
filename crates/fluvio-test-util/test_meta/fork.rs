// Place the provided code into a child process an async run_block_on
// Start a thread to wait on the process to complete, and return the join handle
// User responsible for running .join()
#[macro_export]
macro_rules! async_process {
    ($child:expr,$msg:expr) => {{
        let child_process = match fork::fork() {
            Ok(fork::Fork::Parent(child_pid)) => child_pid,
            Ok(fork::Fork::Child) => {
                fluvio_future::task::run_block_on($child);
                tracing::debug!("finished child process: {}", $msg);
                std::process::exit(0);
            }
            Err(_) => panic!("Fork failed"),
        };

        let child_waitpid_joinhandle = std::thread::spawn(move || {
            let pid = nix::unistd::Pid::from_raw(child_process);
            match nix::sys::wait::waitpid(pid, None) {
                Ok(status) => {
                    tracing::debug!("[fork] Child exited with status {:?}", status);
                }
                Err(err) => panic!("[fork] waitpid() failed: {}", err),
            }
        });

        // Return the std::thread::JoinHandle
        child_waitpid_joinhandle
    }};
}

#[macro_export]
macro_rules! fork_and_wait {
    ($child:expr) => {{
        let child_process = match fork::fork() {
            Ok(fork::Fork::Parent(child_pid)) => child_pid,
            Ok(fork::Fork::Child) => {
                $child;
                std::process::exit(0);
            }
            Err(_) => panic!("Fork failed"),
        };

        let pid = nix::unistd::Pid::from_raw(child_process);
        match nix::sys::wait::waitpid(pid, None) {
            Ok(status) => {
                tracing::debug!("[fork] Child exited with status {:?}", status);
                status
            }
            Err(err) => panic!("[fork] waitpid() failed: {}", err),
        }
    }};
}
