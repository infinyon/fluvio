use std::io::Error as IoError;
use std::path::PathBuf;
use std::path::Path;
use std::env::temp_dir;
use std::convert::TryInto;
use std::sync::Arc;

use fluvio_controlplane_metadata::spu::Endpoint as MetadatEndPoint;

use fluvio_controlplane_metadata::spu::SpuSpec;
use utils::fixture::ensure_clean_dir;
use fluvio_types::socket_helpers::EndPoint;

use crate::core::LocalSpu;
use crate::config::SpuConfig;
use crate::core::GlobalContext;
use crate::core::DefaultSharedGlobalContext;
use crate::core::Receivers;
use crate::services::internal::InternalApiServer;
use crate::start::create_services;
use super::mock_sc::ScGlobalContext;
use super::mock_sc::SharedScContext;
use super::SpuTestRunner;
use super::SpuTest;
use super::mock_sc::MockScServer;

#[derive(Default)]
pub struct TestGenerator {
    base_id: i32,
    base_port: u16,
    base_dir: PathBuf,
}

impl TestGenerator {
    pub fn set_base_id(mut self, id: i32) -> Self {
        self.base_id = id;
        self
    }

    pub fn set_base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

    pub fn set_base_dir<P>(mut self, dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        self.base_dir = temp_dir().join(dir);
        self
    }

    pub fn sc_endpoint(&self) -> EndPoint {
        EndPoint::local_end_point(self.base_port)
    }

    pub fn create_spu_spec(&self, spu_index: u16) -> SpuSpec {
        let port = spu_index * 2 + self.base_port + 1;
        SpuSpec {
            id: self.base_id + spu_index as i32,
            public_endpoint: MetadatEndPoint {
                port,
                ..Default::default()
            },
            private_endpoint: MetadatEndPoint {
                port: port + 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn init(self) -> Self {
        ensure_clean_dir(&self.base_dir);
        self
    }

    fn convert_to_spu(&self, spu: &SpuSpec) -> Result<LocalSpu, IoError> {
        let mut config: SpuConfig = spu.try_into()?;
        config.log.base_dir = self.base_dir.clone();
        config.sc_retry_ms = 10;
        config.sc_endpoint = EndPoint::local_end_point(self.base_port);
        Ok(config.into())
    }

    /// create server with and start controller
    pub fn create_spu_server(
        &self,
        spu: &SpuSpec,
    ) -> Result<(InternalApiServer, DefaultSharedGlobalContext), IoError> {
        let local_spu = self.convert_to_spu(spu)?;
        let (ctx, internal_server, public_server) = create_services(local_spu, true, true);
        let _shutdown = public_server.unwrap().run();
        Ok((internal_server.unwrap(), ctx))
    }

    pub fn create_global_context(
        &self,
        spu: &SpuSpec,
    ) -> Result<(DefaultSharedGlobalContext, Receivers), IoError> {
        let local_spu: LocalSpu = self.convert_to_spu(spu)?;
        Ok(GlobalContext::new_shared_context(local_spu))
    }

    /// create mock sc server which only run internal services.
    pub fn create_sc_server<T>(
        &self,
        test_runner: Arc<SpuTestRunner<T>>,
    ) -> (SharedScContext, MockScServer<T>)
    where
        T: SpuTest + Sync + Send + 'static,
    {
        let sc_contxt = ScGlobalContext::new_shared_context();
        let local_endpoint = EndPoint::local_end_point(self.base_port);
        let server = sc_contxt
            .clone()
            .create_server(local_endpoint.addr, test_runner);
        (sc_contxt, server)
    }
}
