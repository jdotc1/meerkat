use super::types::MeerkatMessage;
use crate::runtime::limits::MAX_NET_MESSAGE_LENGTH;
use anyhow::ensure;
use futures::{AsyncReadExt, AsyncWriteExt};
use libp2p::StreamProtocol;

/// Protocol identifier for Meerkat messages
pub const MEERKAT_PROTOCOL: StreamProtocol = StreamProtocol::new("/meerkat/1.0.0");

/// Serialize and send a message over a stream
pub async fn send_message(
    stream: &mut (impl AsyncWriteExt + Unpin),
    msg: &MeerkatMessage,
) -> anyhow::Result<()> {
    let data = serde_json::to_vec(msg)?;
    ensure!(
        data.len() <= MAX_NET_MESSAGE_LENGTH,
        "encoded message exceeds {}-byte transport limit",
        MAX_NET_MESSAGE_LENGTH
    );
    let len = u32::try_from(data.len())?.to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&data).await?;
    stream.flush().await?;
    Ok(())
}

/// Receive and deserialize a message from a stream
pub async fn recv_message(
    stream: &mut (impl AsyncReadExt + Unpin),
) -> anyhow::Result<MeerkatMessage> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    ensure!(
        len <= MAX_NET_MESSAGE_LENGTH,
        "incoming message exceeds {}-byte transport limit",
        MAX_NET_MESSAGE_LENGTH
    );
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    let msg = serde_json::from_slice(&data)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::limits::MAX_NET_REQUEST_SOURCE_LENGTH;
    use futures::io::Cursor;

    #[tokio::test]
    async fn oversized_prefix_is_rejected_before_reading_any_body() {
        let mut incoming = Cursor::new(((MAX_NET_MESSAGE_LENGTH + 1) as u32).to_be_bytes());
        let error = recv_message(&mut incoming).await.unwrap_err();
        assert!(error.to_string().contains("transport limit"), "{error}");
        assert_eq!(incoming.position(), 4);
    }

    #[tokio::test]
    async fn sender_and_receiver_accept_exact_transport_limit() {
        let overhead = serde_json::to_vec(&MeerkatMessage::Ping {
            content: String::new(),
        })
        .unwrap()
        .len();
        let message = MeerkatMessage::Ping {
            content: "x".repeat(MAX_NET_MESSAGE_LENGTH - overhead),
        };
        let mut wire = Cursor::new(Vec::new());
        send_message(&mut wire, &message).await.unwrap();
        assert_eq!(wire.get_ref().len(), MAX_NET_MESSAGE_LENGTH + 4);
        wire.set_position(0);
        let received = recv_message(&mut wire).await.unwrap();
        match received {
            MeerkatMessage::Ping { content } => {
                assert_eq!(content.len(), MAX_NET_MESSAGE_LENGTH - overhead);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn sender_refuses_oversized_message_without_writing_a_header() {
        let message = MeerkatMessage::Ping {
            content: "x".repeat(MAX_NET_MESSAGE_LENGTH),
        };
        let mut wire = Cursor::new(Vec::new());
        let error = send_message(&mut wire, &message).await.unwrap_err();
        assert!(error.to_string().contains("transport limit"), "{error}");
        assert!(wire.get_ref().is_empty());
    }

    #[tokio::test]
    async fn representative_maximum_service_source_round_trips() {
        let source = "x".repeat(MAX_NET_REQUEST_SOURCE_LENGTH);
        let message = MeerkatMessage::ServiceCodeResponse {
            request_id: 17,
            path: "helper.mkt".to_string(),
            source: source.clone(),
        };
        let mut wire = Cursor::new(Vec::new());
        send_message(&mut wire, &message).await.unwrap();
        wire.set_position(0);
        match recv_message(&mut wire).await.unwrap() {
            MeerkatMessage::ServiceCodeResponse { source: received, .. } => {
                assert_eq!(received, source);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
