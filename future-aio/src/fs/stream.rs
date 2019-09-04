
use futures::Stream;

use crate::fs::AsyncFile;

pub struct AsyncFileStream {
    file: AsyncFile
}

impl AsyncFileStream {
    pub fn new(file: AsyncFile) -> Self {
        Self {
            file
        }
    }
}


impl Stream for AsyncFileStream {

    fn poll_next(self: Pin<&mut Self>, lw: &Waker) -> Poll<Option<Self::Item>> {
        
    }

}