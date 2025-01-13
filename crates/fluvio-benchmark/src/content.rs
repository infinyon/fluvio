use rand::{self, distributions::Alphanumeric, Rng};

pub struct CachedMessages(Vec<Vec<u8>>);

impl CachedMessages {
    pub fn new(msg_size: u64, cache_size: u64) -> CachedMessages {
        let messages = (0..(cache_size / msg_size))
            .map(|_| {
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(msg_size as usize)
                    .collect::<Vec<u8>>()
            })
            .collect::<Vec<_>>();
        CachedMessages(messages)
    }
}

impl<'a> IntoIterator for &'a CachedMessages {
    type Item = &'a [u8];
    type IntoIter = CachedMessagesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        CachedMessagesIterator {
            index: rand::thread_rng().gen_range(0..self.0.len()),
            cache: self,
        }
    }
}

pub struct CachedMessagesIterator<'a> {
    pub index: usize,
    pub cache: &'a CachedMessages,
}

impl<'a> Iterator for CachedMessagesIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.cache.0.len() {
            self.index = 0;
        }
        self.index += 1;
        Some(self.cache.0[self.index - 1].as_slice())
    }
}
