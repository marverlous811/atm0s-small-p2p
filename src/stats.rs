use metrics::{describe_counter, describe_gauge};

pub const P2P_ALIAS_CACHE_SIZE: &str = "p2p_alias_cache_size";
pub const P2P_ALIAS_CACHE_UPSERT: &str = "p2p_alias_cache_upsert";
pub const P2P_ALIAS_CACHE_POP: &str = "p2p_alias_cache_pop";
pub const P2P_ALIAS_LIVE_FIND_REQUEST: &str = "p2p_alias_live_find_request";
pub const P2P_ALIAS_FIND_REQUEST: &str = "p2p_alias_find_request";

pub const P2P_LIVE_CONNECTION_COUNT: &str = "p2p_live_connection_count";
pub const P2P_CONNECTION_UPTIME: &str = "p2p_connection_uptime";
pub const P2P_CONNECTION_RTT: &str = "p2p_connection_rtt";
pub const P2P_CONNECTION_SENT_BYTES: &str = "p2p_connection_sent_bytes";
pub const P2P_CONNECTION_RECV_BYTES: &str = "p2p_connection_recv_bytes";
pub const P2P_CONNECTION_LOST_BYTES: &str = "p2p_connection_lost_bytes";
pub const P2P_CONNECTION_LOST_PKT: &str = "p2p_connection_lost_pkt";
pub const P2P_CONNECTION_CONGESTION_EVENTS: &str = "p2p_connection_congestion_events";
pub const P2P_CONNECTION_TX_STREAM: &str = "p2p_connection_tx_stream";
pub const P2P_CONNECTION_RX_STREAM: &str = "p2p_connection_rx_stream";
pub const P2P_CONNECTION_TX_RESET_STREAM: &str = "p2p_connection_tx_reset_stream";
pub const P2P_CONNECTION_RX_RESET_STREAM: &str = "p2p_connection_rx_reset_stream";

pub const P2P_CONNECTION_CWND: &str = "p2p_connection_cwnd";
pub const P2P_CONNECTION_MTU: &str = "p2p_connection_mtu";
pub const P2P_CONNECTION_BLACK_HOLES: &str = "p2p_connection_black_holes";
pub const P2P_CONNECTION_UDP_TX_DATAGRAMS: &str = "p2p_connection_udp_tx_datagrams";
pub const P2P_CONNECTION_UDP_RX_DATAGRAMS: &str = "p2p_connection_udp_rx_datagrams";
pub const P2P_CONNECTION_FRAME_TX_DATA_BLOCKED: &str = "p2p_connection_frame_tx_data_blocked";
pub const P2P_CONNECTION_FRAME_RX_DATA_BLOCKED: &str = "p2p_connection_frame_rx_data_blocked";
pub const P2P_CONNECTION_FRAME_TX_STREAM_DATA_BLOCKED: &str = "p2p_connection_frame_tx_stream_data_blocked";
pub const P2P_CONNECTION_FRAME_RX_STREAM_DATA_BLOCKED: &str = "p2p_connection_frame_rx_stream_data_blocked";

pub fn init_metrics() {
    describe_counter!(P2P_ALIAS_CACHE_UPSERT, "Number of upserts in the alias cache");
    describe_counter!(P2P_ALIAS_CACHE_POP, "Number of pops in the alias cache");
    describe_gauge!(P2P_ALIAS_CACHE_SIZE, "Size of the alias cache");
    describe_gauge!(P2P_ALIAS_LIVE_FIND_REQUEST, "Number of live alias find requests");
    describe_counter!(P2P_ALIAS_FIND_REQUEST, "Number of alias find requests");

    describe_gauge!(P2P_LIVE_CONNECTION_COUNT, "Number of live connections");

    describe_counter!(P2P_CONNECTION_UPTIME, "Connection uptime");
    describe_gauge!(P2P_CONNECTION_RTT, "Round-trip time in connection");
    describe_counter!(P2P_CONNECTION_SENT_BYTES, "Bytes sent in connection");
    describe_counter!(P2P_CONNECTION_RECV_BYTES, "Bytes received in connection");
    describe_counter!(P2P_CONNECTION_LOST_BYTES, "Bytes lost in connection");
    describe_counter!(P2P_CONNECTION_LOST_PKT, "Packet loss in connection");
    describe_counter!(P2P_CONNECTION_CONGESTION_EVENTS, "Congestion events in connection");
    describe_counter!(P2P_CONNECTION_TX_STREAM, "Number of transmitted stream frames in connection");
    describe_counter!(P2P_CONNECTION_RX_STREAM, "Number of received stream frames in connection");
    describe_counter!(P2P_CONNECTION_TX_RESET_STREAM, "Number of transmitted RESET_STREAM frames in connection");
    describe_counter!(P2P_CONNECTION_RX_RESET_STREAM, "Number of received RESET_STREAM frames in connection");

    describe_gauge!(P2P_CONNECTION_CWND, "Congestion window size in bytes");
    describe_gauge!(P2P_CONNECTION_MTU, "Current path MTU in bytes");
    describe_counter!(P2P_CONNECTION_BLACK_HOLES, "Number of black holes detected on the path");
    describe_counter!(P2P_CONNECTION_UDP_TX_DATAGRAMS, "Number of UDP datagrams sent");
    describe_counter!(P2P_CONNECTION_UDP_RX_DATAGRAMS, "Number of UDP datagrams received");
    describe_counter!(P2P_CONNECTION_FRAME_TX_DATA_BLOCKED, "Number of DATA_BLOCKED frames sent (connection-level flow control stall)");
    describe_counter!(P2P_CONNECTION_FRAME_RX_DATA_BLOCKED, "Number of DATA_BLOCKED frames received (connection-level flow control stall)");
    describe_counter!(P2P_CONNECTION_FRAME_TX_STREAM_DATA_BLOCKED, "Number of STREAM_DATA_BLOCKED frames sent (stream-level flow control stall)");
    describe_counter!(P2P_CONNECTION_FRAME_RX_STREAM_DATA_BLOCKED, "Number of STREAM_DATA_BLOCKED frames received (stream-level flow control stall)");
}
