use crate::TestOption;

const VALUE: u8 = 65;

/// generate test data based on iteration and option
#[allow(clippy::all)]
pub fn generate_message(_index: u16, option: &TestOption) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(option.produce.record_size);

    for _ in 0..option.produce.record_size {
        bytes.push(VALUE);
    }

    bytes
}

/// validate the message
#[allow(clippy::needless_range_loop)]
pub fn validate_message(_index: u16, option: &TestOption, data: &[u8]) {
    assert_eq!(data.len(), option.produce.record_size);
    for i in 0..option.produce.record_size {
        assert_eq!(data[i], VALUE);
    }
}
