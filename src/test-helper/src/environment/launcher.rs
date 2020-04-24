use std::process::Child;
use std::future::Future;


pub struct Launcher {
    servers: Vec<Child>
}

impl Launcher {

    pub fn new() -> Self {
        Self {
            servers: vec![]
        }
    }

    pub async fn add<F>(&mut self, name: &str, future: F)
        where F: Future<Output=Child>
    {
        let child = future.await;
        println!("server: {}:{} is being tracked",name,child.id());
        self.servers.push(child);
    }

    pub fn terminate(self)  {

        for mut server in self.servers {
            println!("shutting down server: {}",server.id());
            if let Err(err) = server.kill() {
                eprintln!("fail to terminate: {}",err);
            }
        }

    }
}
