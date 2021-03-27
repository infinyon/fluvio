use std::env::temp_dir;

use dataplane::Size;

use crate::config::ConfigOption;

pub fn default_option(index_max_interval_bytes: Size) -> ConfigOption {
    ConfigOption {
        segment_max_bytes: 100,
        index_max_interval_bytes,
        base_dir: temp_dir(),
        index_max_bytes: 1000,
        ..Default::default()
    }
}

#[cfg(test)]
mod pin_tests {

    use std::pin::Pin;
    use pin_utils::pin_mut;
    use pin_utils::unsafe_unpinned;

    //  impl Unpin for Counter{}

    struct Counter {
        total: u16,
    }

    impl Counter {
        unsafe_unpinned!(total: u16);

        fn get_total(self: Pin<&mut Self>) -> u16 {
            self.total
        }

        fn update_total(mut self: Pin<&mut Self>, val: u16) {
            *self.as_mut().total() = val;
        }
    }

    #[test]
    fn test_read_pin() {
        let counter = Counter { total: 20 };
        pin_mut!(counter); // works with future that requires unpin
        assert_eq!(counter.get_total(), 20);
    }

    #[test]
    fn test_write_pin() {
        let counter = Counter { total: 20 };
        pin_mut!(counter); // works with future that requires unpin
        counter.as_mut().update_total(30);
        assert_eq!(counter.get_total(), 30);
    }
}
