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
}
