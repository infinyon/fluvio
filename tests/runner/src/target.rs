use std::process::Command;

pub enum Target {
    Direct(String),
    Profile
}

impl Target {

    pub fn direct(addr: String) -> Self {
        Self::Direct(addr)
    }

    pub fn add_to_cmd(&self,cmd: &mut Command) {
        match self {
            Self::Direct(addr) => {
                cmd
                .arg("--sc")
                .arg(addr);
            },
            Self::Profile => {}
        }
    }

}

