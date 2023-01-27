use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_socket::{VersionedSerialSocket, MultiplexerSocket, SerialFrame};
use tracing::{trace, debug};

use crate::objects::{ListFilter, ObjectApiListRequest, ListRequest, ListResponse, Metadata};
pub use fluvio_socket::{ClientConfig, SocketError};

/// Experimental: this API is not finalized and may be changed in the future.
pub struct SmartModuleApiClient {
    socket: VersionedSerialSocket,
}

impl SmartModuleApiClient {
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self, SocketError> {
        let inner_client = config.connect().await?;
        debug!(addr = %inner_client.config().addr(), "connected to cluster");

        let (socket, config, versions) = inner_client.split();
        let socket = MultiplexerSocket::shared(socket);
        let versioned_socket = VersionedSerialSocket::new(socket, config, versions);

        Ok(Self {
            socket: versioned_socket,
        })
    }

    pub async fn get<F>(&self, name: F) -> Result<Option<SmartModuleSpec>, SocketError>
    where
        ListFilter: From<F>,
    {
        let mut smartmodule_spec_list = self.list_with_params::<F>(vec![name], false).await?;
        Ok(smartmodule_spec_list.pop().map(|m| m.spec))
    }

    pub async fn list_with_params<F>(
        &self,
        filters: Vec<F>,
        summary: bool,
    ) -> Result<Vec<Metadata<SmartModuleSpec>>, SocketError>
    where
        ListFilter: From<F>,
    {
        let filter_list: Vec<ListFilter> = filters.into_iter().map(Into::into).collect();
        let list_request: ListRequest<SmartModuleSpec> = ListRequest::new(filter_list, summary);

        let list_request: ObjectApiListRequest = list_request.into();
        let response = self.socket.send_receive(list_request).await?;
        trace!("list response: {:#?}", response);
        response
            .try_into()
            .map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("can't convert: {err}"))
                    .into()
            })
            .map(|out: ListResponse<SmartModuleSpec>| out.inner())
    }
}
