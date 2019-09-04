
#[cfg(feature = "tokio2")]
mod tcp_stream_3;

#[cfg(not(feature = "tokio2"))]
mod tcp_stream_1;

#[cfg(feature = "tokio2")]
use tcp_stream_3 as common_tcp_stream;

#[cfg(not(feature = "tokio2"))]
use tcp_stream_1 as common_tcp_stream;

pub use self::common_tcp_stream::AsyncTcpListener;
pub use self::common_tcp_stream::AsyncTcpStream;
pub use self::common_tcp_stream::TcpStreamSplitStream;
pub use self::common_tcp_stream::TcpStreamSplitSink;
pub use self::common_tcp_stream::TcpStreamSplit;



