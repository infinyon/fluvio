use super::{ConsumerConfig, ConsumerConfigBuilder, ProducerConfig, ProducerConfigBuilder};

pub trait CrossIterate<C, B> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut B) + Copy>(self, values: &[T], f: F) -> Self;
    fn build(self) -> Vec<C>;
}

impl CrossIterate<ProducerConfig, ProducerConfigBuilder> for Vec<ProducerConfigBuilder> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut ProducerConfigBuilder) + Copy>(
        self,
        values: &[T],
        f: F,
    ) -> Self {
        self.into_iter()
            .flat_map(|builder| {
                values.iter().map(move |value| {
                    let mut clone = builder.clone();
                    f(value.clone(), &mut clone);
                    clone
                })
            })
            .collect()
    }

    fn build(self) -> Vec<ProducerConfig> {
        self.into_iter().map(|x| x.build().unwrap()).collect()
    }
}

impl CrossIterate<ConsumerConfig, ConsumerConfigBuilder> for Vec<ConsumerConfigBuilder> {
    fn cross_iterate<T: Clone, F: Fn(T, &mut ConsumerConfigBuilder) + Copy>(
        self,
        values: &[T],
        f: F,
    ) -> Self {
        self.into_iter()
            .flat_map(|builder| {
                values.iter().map(move |value| {
                    let mut clone = builder.clone();
                    f(value.clone(), &mut clone);
                    clone
                })
            })
            .collect()
    }

    fn build(self) -> Vec<ConsumerConfig> {
        self.into_iter().map(|x| x.build().unwrap()).collect()
    }
}
