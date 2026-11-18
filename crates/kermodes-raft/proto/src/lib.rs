// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The messages raft exchanges and persists (`eraftpb`), vendored from raft-rs's `raft-proto`
//! (README.md). Upstream generates them from `eraftpb.proto` with protobuf or prost; here they are
//! written out, with the same fields and accessors, and encoded in the same protobuf wire format
//! by hand: no protobuf crate and no `protoc`. Unknown fields are skipped when decoding, so a
//! field added later is read past by an older reader, as protobuf's are.

// As upstream: messages are built with `default` and then assigned, as both its codecs
// required.
#![allow(clippy::field_reassign_with_default)]

mod confchange;
mod confstate;

pub use crate::confchange::{
    new_conf_change_single, parse_conf_change, stringify_conf_change, ConfChangeI,
};
pub use crate::confstate::conf_state_eq;

/// What upstream reexports from its codec: here, the codec itself.
pub mod protocompat {
    pub use crate::codec::{PbError, PbMessage};
}

mod codec {
    use bytes::Bytes;

    /// A message that could not be decoded.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct PbError(pub(crate) String);

    impl std::fmt::Display for PbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "cannot decode a raft message: {}", self.0)
        }
    }

    impl std::error::Error for PbError {}

    /// A message in protobuf's wire format.
    pub trait PbMessage: Default {
        /// Its length encoded, computed without encoding it.
        fn encoded_len(&self) -> usize;
        fn encode(&self, buf: &mut Vec<u8>);
        /// Reads field `tag`, of wire type `wire`, into this message.
        fn merge_field(&mut self, tag: u64, wire: u8, r: &mut Reader) -> Result<(), PbError>;

        fn compute_size(&self) -> u32 {
            self.encoded_len() as u32
        }

        fn write_to_bytes(&self) -> Result<Vec<u8>, PbError> {
            let mut buf = Vec::with_capacity(self.encoded_len());
            self.encode(&mut buf);
            Ok(buf)
        }

        /// Reads `bytes` into this message: fields it holds are replaced, repeated fields
        /// extended, as protobuf merges.
        fn merge_from_bytes(&mut self, bytes: &[u8]) -> Result<(), PbError> {
            let mut r = Reader { bytes, at: 0 };
            while !r.done() {
                let key = r.varint()?;
                self.merge_field(key >> 3, (key & 7) as u8, &mut r)?;
            }
            Ok(())
        }
    }

    pub(crate) const VARINT: u8 = 0;
    const FIXED64: u8 = 1;
    pub(crate) const DELIMITED: u8 = 2;
    const FIXED32: u8 = 5;

    /// Reads a message's bytes.
    pub struct Reader<'a> {
        bytes: &'a [u8],
        at: usize,
    }

    impl<'a> Reader<'a> {
        fn done(&self) -> bool {
            self.at >= self.bytes.len()
        }

        pub(crate) fn varint(&mut self) -> Result<u64, PbError> {
            let mut value = 0u64;
            for shift in (0..64).step_by(7) {
                let Some(&byte) = self.bytes.get(self.at) else {
                    return Err(PbError("a varint runs past the end".into()));
                };
                self.at += 1;
                value |= u64::from(byte & 0x7f) << shift;
                if byte < 0x80 {
                    return Ok(value);
                }
            }
            Err(PbError("a varint longer than 10 bytes".into()))
        }

        pub(crate) fn delimited(&mut self) -> Result<&'a [u8], PbError> {
            let len = self.varint()? as usize;
            let end = self
                .at
                .checked_add(len)
                .filter(|&end| end <= self.bytes.len())
                .ok_or_else(|| PbError("a field runs past the end".into()))?;
            let field = &self.bytes[self.at..end];
            self.at = end;
            Ok(field)
        }

        /// Passes over a field this version does not know.
        pub(crate) fn skip(&mut self, wire: u8) -> Result<(), PbError> {
            let width = match wire {
                VARINT => return self.varint().map(drop),
                DELIMITED => return self.delimited().map(drop),
                FIXED64 => 8,
                FIXED32 => 4,
                other => return Err(PbError(format!("unknown wire type {other}"))),
            };
            if self.at + width > self.bytes.len() {
                return Err(PbError("a field runs past the end".into()));
            }
            self.at += width;
            Ok(())
        }
    }

    pub(crate) fn varint_len(value: u64) -> usize {
        (64 - (value | 1).leading_zeros() as usize).div_ceil(7)
    }

    pub(crate) fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            buf.push(value as u8 | 0x80);
            value >>= 7;
        }
        buf.push(value as u8);
    }

    fn key_len(tag: u64) -> usize {
        varint_len(tag << 3)
    }

    fn put_key(buf: &mut Vec<u8>, tag: u64, wire: u8) {
        put_varint(buf, tag << 3 | u64::from(wire));
    }

    fn expect(wire: u8, want: u8) -> Result<(), PbError> {
        if wire == want {
            Ok(())
        } else {
            Err(PbError(format!(
                "wire type {wire} where {want} was expected"
            )))
        }
    }

    /// A field's value: how it is encoded under a tag (nothing when it is the default, as proto3
    /// encodes), and read back.
    pub trait Field {
        fn field_len(&self, tag: u64) -> usize;
        fn encode_field(&self, tag: u64, buf: &mut Vec<u8>);
        fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError>;
    }

    /// Integers encoded as varints: `u64`, `i64` (two's complement), `bool`.
    macro_rules! varint_field {
        ($ty:ty, $to:expr, $from:expr) => {
            impl Field for $ty {
                fn field_len(&self, tag: u64) -> usize {
                    let value: u64 = $to(*self);
                    if value == 0 {
                        0
                    } else {
                        key_len(tag) + varint_len(value)
                    }
                }

                fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
                    let value: u64 = $to(*self);
                    if value != 0 {
                        put_key(buf, tag, VARINT);
                        put_varint(buf, value);
                    }
                }

                fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
                    expect(wire, VARINT)?;
                    *self = $from(r.varint()?);
                    Ok(())
                }
            }
        };
    }

    varint_field!(u64, |v: u64| v, |v: u64| v);
    varint_field!(i64, |v: i64| v as u64, |v: u64| v as i64);
    varint_field!(bool, u64::from, |v: u64| v != 0);

    impl Field for Bytes {
        fn field_len(&self, tag: u64) -> usize {
            if self.is_empty() {
                0
            } else {
                key_len(tag) + varint_len(self.len() as u64) + self.len()
            }
        }

        fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
            if !self.is_empty() {
                put_key(buf, tag, DELIMITED);
                put_varint(buf, self.len() as u64);
                buf.extend_from_slice(self);
            }
        }

        fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
            expect(wire, DELIMITED)?;
            *self = Bytes::copy_from_slice(r.delimited()?);
            Ok(())
        }
    }

    /// Repeated `u64`s, each under the tag (unpacked), as upstream's rust-protobuf writes them;
    /// read packed or not.
    impl Field for Vec<u64> {
        fn field_len(&self, tag: u64) -> usize {
            self.iter().map(|&v| key_len(tag) + varint_len(v)).sum()
        }

        fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
            for &value in self {
                put_key(buf, tag, VARINT);
                put_varint(buf, value);
            }
        }

        fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
            if wire == VARINT {
                self.push(r.varint()?);
                return Ok(());
            }
            expect(wire, DELIMITED)?;
            let mut packed = Reader {
                bytes: r.delimited()?,
                at: 0,
            };
            while !packed.done() {
                self.push(packed.varint()?);
            }
            Ok(())
        }
    }

    /// A message within a message, encoded unless it is the default.
    impl<T: PbMessage + PartialEq> Field for T {
        fn field_len(&self, tag: u64) -> usize {
            if *self == T::default() {
                return 0;
            }
            let len = self.encoded_len();
            key_len(tag) + varint_len(len as u64) + len
        }

        fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
            if *self != T::default() {
                put_key(buf, tag, DELIMITED);
                put_varint(buf, self.encoded_len() as u64);
                self.encode(buf);
            }
        }

        fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
            expect(wire, DELIMITED)?;
            self.merge_from_bytes(r.delimited()?)
        }
    }

    /// Repeated messages, each under the tag.
    impl<T: PbMessage> Field for Vec<T> {
        fn field_len(&self, tag: u64) -> usize {
            self.iter()
                .map(|m| {
                    let len = m.encoded_len();
                    key_len(tag) + varint_len(len as u64) + len
                })
                .sum()
        }

        fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
            for m in self {
                put_key(buf, tag, DELIMITED);
                put_varint(buf, m.encoded_len() as u64);
                m.encode(buf);
            }
        }

        fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
            expect(wire, DELIMITED)?;
            let mut m = T::default();
            m.merge_from_bytes(r.delimited()?)?;
            self.push(m);
            Ok(())
        }
    }
}

/// The messages, as upstream's `eraftpb.proto` defines them, with the accessors its generated
/// code has.
#[allow(missing_docs)]
pub mod eraftpb {
    use bytes::Bytes;

    use crate::codec::{put_varint, varint_len, Field, PbError, Reader, VARINT};
    use crate::protocompat::PbMessage;

    /// An enum, encoded as its number.
    macro_rules! enumeration {
        ($(#[$meta:meta])* pub enum $name:ident { $($variant:ident = $n:literal,)* }) => {
            $(#[$meta])*
            #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
            #[repr(i32)]
            pub enum $name {
                #[default]
                $($variant = $n,)*
            }

            impl $name {
                pub fn values() -> &'static [$name] {
                    &[$($name::$variant,)*]
                }

                pub fn from_i32(n: i32) -> Option<$name> {
                    match n {
                        $($n => Some($name::$variant),)*
                        _ => None,
                    }
                }
            }

            impl Field for $name {
                fn field_len(&self, tag: u64) -> usize {
                    let value = *self as i32 as u64;
                    if value == 0 { 0 } else { varint_len(tag << 3) + varint_len(value) }
                }

                fn encode_field(&self, tag: u64, buf: &mut Vec<u8>) {
                    let value = *self as i32 as u64;
                    if value != 0 {
                        put_varint(buf, tag << 3);
                        put_varint(buf, value);
                    }
                }

                fn merge_field(&mut self, wire: u8, r: &mut Reader) -> Result<(), PbError> {
                    if wire != VARINT {
                        return Err(PbError(format!("wire type {wire} for an enum")));
                    }
                    let n = r.varint()? as i32;
                    *self = $name::from_i32(n).ok_or_else(|| {
                        PbError(format!("{} has no value {n}", stringify!($name)))
                    })?;
                    Ok(())
                }
            }
        };
    }

    /// A repeated field's element type, which a macro cannot take apart from `Vec<T>` once
    /// parsed as a type. Public only because accessor signatures name it.
    #[doc(hidden)]
    pub trait List {
        type Elem;
    }

    impl<T> List for Vec<T> {
        type Elem = T;
    }

    /// One accessor set, as upstream's generated code names them: scalars and enums get, set,
    /// and clear; bytes, messages, and repeated fields also give a mutable reference and are
    /// taken.
    macro_rules! accessors {
        (copy $field:ident: $ty:ty [$get:ident, $set:ident, $clear:ident]) => {
            pub fn $get(&self) -> $ty {
                self.$field
            }
            pub fn $set(&mut self, v: $ty) {
                self.$field = v;
            }
            pub fn $clear(&mut self) {
                self.$field = Default::default();
            }
        };
        (bytes $field:ident: $ty:ty [$get:ident, $set:ident, $mut:ident, $take:ident, $clear:ident]) => {
            pub fn $get(&self) -> &[u8] {
                &self.$field
            }
            pub fn $set(&mut self, v: Bytes) {
                self.$field = v;
            }
            pub fn $mut(&mut self) -> &mut Bytes {
                &mut self.$field
            }
            pub fn $take(&mut self) -> Bytes {
                std::mem::take(&mut self.$field)
            }
            pub fn $clear(&mut self) {
                self.$field = Bytes::new();
            }
        };
        (owned $field:ident: $ty:ty [$get:ident, $set:ident, $mut:ident, $take:ident, $clear:ident]) => {
            pub fn $get(&self) -> &$ty {
                &self.$field
            }
            pub fn $set(&mut self, v: $ty) {
                self.$field = v;
            }
            pub fn $mut(&mut self) -> &mut $ty {
                &mut self.$field
            }
            pub fn $take(&mut self) -> $ty {
                std::mem::take(&mut self.$field)
            }
            pub fn $clear(&mut self) {
                self.$field = Default::default();
            }
        };
        (list $field:ident: $ty:ty [$get:ident, $set:ident, $mut:ident, $take:ident, $clear:ident]) => {
            pub fn $get(&self) -> &[<$ty as List>::Elem] {
                &self.$field
            }
            pub fn $set(&mut self, v: $ty) {
                self.$field = v;
            }
            pub fn $mut(&mut self) -> &mut $ty {
                &mut self.$field
            }
            pub fn $take(&mut self) -> $ty {
                std::mem::take(&mut self.$field)
            }
            pub fn $clear(&mut self) {
                self.$field.clear();
            }
        };
    }

    /// A message: its fields (each with its protobuf tag), accessors, and encoding.
    macro_rules! message {
        (
            $(#[$meta:meta])*
            pub struct $name:ident {
                $($(#[$fmeta:meta])* $kind:ident $field:ident: $ty:ty = $tag:literal [$($acc:ident),*],)*
            }
        ) => {
            $(#[$meta])*
            #[derive(Clone, Debug, Default, PartialEq)]
            pub struct $name {
                $($(#[$fmeta])* pub $field: $ty,)*
            }

            impl $name {
                pub fn new() -> $name {
                    $name::default()
                }

                $(accessors!($kind $field: $ty [$($acc),*]);)*
            }

            impl PbMessage for $name {
                fn encoded_len(&self) -> usize {
                    0 $(+ self.$field.field_len($tag))*
                }

                fn encode(&self, buf: &mut Vec<u8>) {
                    $(self.$field.encode_field($tag, buf);)*
                }

                fn merge_field(&mut self, tag: u64, wire: u8, r: &mut Reader) -> Result<(), PbError> {
                    match tag {
                        $($tag => Field::merge_field(&mut self.$field, wire, r),)*
                        _ => r.skip(wire),
                    }
                }
            }
        };
    }

    enumeration! {
        pub enum EntryType {
            EntryNormal = 0,
            EntryConfChange = 1,
            EntryConfChangeV2 = 2,
        }
    }

    message! {
        /// A change to apply. For a normal entry, `data` is the change and `context` anything
        /// the application wants with it; for a configuration change, `data` is the
        /// `ConfChange` or `ConfChangeV2`.
        pub struct Entry {
            copy entry_type: EntryType = 1 [get_entry_type, set_entry_type, clear_entry_type],
            copy term: u64 = 2 [get_term, set_term, clear_term],
            copy index: u64 = 3 [get_index, set_index, clear_index],
            bytes data: Bytes = 4 [get_data, set_data, mut_data, take_data, clear_data],
            bytes context: Bytes = 6 [get_context, set_context, mut_context, take_context, clear_context],
            /// Deprecated upstream; kept as a field so its tag is not reused.
            copy sync_log: bool = 5 [get_sync_log, set_sync_log, clear_sync_log],
        }
    }

    message! {
        pub struct SnapshotMetadata {
            owned conf_state: ConfState = 1 [get_conf_state, set_conf_state, mut_conf_state, take_conf_state, clear_conf_state],
            copy index: u64 = 2 [get_index, set_index, clear_index],
            copy term: u64 = 3 [get_term, set_term, clear_term],
        }
    }

    message! {
        pub struct Snapshot {
            bytes data: Bytes = 1 [get_data, set_data, mut_data, take_data, clear_data],
            owned metadata: SnapshotMetadata = 2 [get_metadata, set_metadata, mut_metadata, take_metadata, clear_metadata],
        }
    }

    impl Snapshot {
        /// For a given snapshot, determine if it's empty or not.
        pub fn is_empty(&self) -> bool {
            self.get_metadata().index == 0
        }
    }

    enumeration! {
        pub enum MessageType {
            MsgHup = 0,
            MsgBeat = 1,
            MsgPropose = 2,
            MsgAppend = 3,
            MsgAppendResponse = 4,
            MsgRequestVote = 5,
            MsgRequestVoteResponse = 6,
            MsgSnapshot = 7,
            MsgHeartbeat = 8,
            MsgHeartbeatResponse = 9,
            MsgUnreachable = 10,
            MsgSnapStatus = 11,
            MsgCheckQuorum = 12,
            MsgTransferLeader = 13,
            MsgTimeoutNow = 14,
            MsgReadIndex = 15,
            MsgReadIndexResp = 16,
            MsgRequestPreVote = 17,
            MsgRequestPreVoteResponse = 18,
        }
    }

    message! {
        pub struct Message {
            copy msg_type: MessageType = 1 [get_msg_type, set_msg_type, clear_msg_type],
            copy to: u64 = 2 [get_to, set_to, clear_to],
            copy from: u64 = 3 [get_from, set_from, clear_from],
            copy term: u64 = 4 [get_term, set_term, clear_term],
            /// The term of the entry at `index`: for an append, of the entry before the ones
            /// sent; for a rejection, of the follower's entry there.
            copy log_term: u64 = 5 [get_log_term, set_log_term, clear_log_term],
            copy index: u64 = 6 [get_index, set_index, clear_index],
            list entries: Vec<Entry> = 7 [get_entries, set_entries, mut_entries, take_entries, clear_entries],
            copy commit: u64 = 8 [get_commit, set_commit, clear_commit],
            copy commit_term: u64 = 15 [get_commit_term, set_commit_term, clear_commit_term],
            owned snapshot: Snapshot = 9 [get_snapshot, set_snapshot, mut_snapshot, take_snapshot, clear_snapshot],
            copy request_snapshot: u64 = 13 [get_request_snapshot, set_request_snapshot, clear_request_snapshot],
            copy reject: bool = 10 [get_reject, set_reject, clear_reject],
            copy reject_hint: u64 = 11 [get_reject_hint, set_reject_hint, clear_reject_hint],
            bytes context: Bytes = 12 [get_context, set_context, mut_context, take_context, clear_context],
            copy deprecated_priority: u64 = 14 [get_deprecated_priority, set_deprecated_priority, clear_deprecated_priority],
            /// Used when set; otherwise `deprecated_priority`.
            copy priority: i64 = 16 [get_priority, set_priority, clear_priority],
        }
    }

    message! {
        pub struct HardState {
            copy term: u64 = 1 [get_term, set_term, clear_term],
            copy vote: u64 = 2 [get_vote, set_vote, clear_vote],
            copy commit: u64 = 3 [get_commit, set_commit, clear_commit],
        }
    }

    enumeration! {
        /// How a `ConfChangeV2` uses joint consensus: `Auto` uses the simple protocol when it
        /// can, else `Implicit`; `Implicit` always uses it and leaves it by itself; `Explicit`
        /// stays joint until the application proposes an empty change.
        pub enum ConfChangeTransition {
            Auto = 0,
            Implicit = 1,
            Explicit = 2,
        }
    }

    message! {
        pub struct ConfState {
            list voters: Vec<u64> = 1 [get_voters, set_voters, mut_voters, take_voters, clear_voters],
            list learners: Vec<u64> = 2 [get_learners, set_learners, mut_learners, take_learners, clear_learners],
            /// The voters of the outgoing configuration: not empty while joint.
            list voters_outgoing: Vec<u64> = 3 [get_voters_outgoing, set_voters_outgoing, mut_voters_outgoing, take_voters_outgoing, clear_voters_outgoing],
            /// Nodes that become learners when the outgoing configuration is left.
            list learners_next: Vec<u64> = 4 [get_learners_next, set_learners_next, mut_learners_next, take_learners_next, clear_learners_next],
            /// Whether raft leaves the joint configuration by itself once it is safe.
            copy auto_leave: bool = 5 [get_auto_leave, set_auto_leave, clear_auto_leave],
        }
    }

    enumeration! {
        pub enum ConfChangeType {
            AddNode = 0,
            RemoveNode = 1,
            AddLearnerNode = 2,
        }
    }

    message! {
        pub struct ConfChange {
            copy change_type: ConfChangeType = 2 [get_change_type, set_change_type, clear_change_type],
            copy node_id: u64 = 3 [get_node_id, set_node_id, clear_node_id],
            bytes context: Bytes = 4 [get_context, set_context, mut_context, take_context, clear_context],
            copy id: u64 = 1 [get_id, set_id, clear_id],
        }
    }

    message! {
        /// One change of a `ConfChangeV2`, which carries several atomically.
        pub struct ConfChangeSingle {
            copy change_type: ConfChangeType = 1 [get_change_type, set_change_type, clear_change_type],
            copy node_id: u64 = 2 [get_node_id, set_node_id, clear_node_id],
        }
    }

    message! {
        /// A configuration change: one at a time by the simple protocol, or any by joint
        /// consensus (`transition`). Changes take effect when applied, not when appended.
        pub struct ConfChangeV2 {
            copy transition: ConfChangeTransition = 1 [get_transition, set_transition, clear_transition],
            list changes: Vec<ConfChangeSingle> = 2 [get_changes, set_changes, mut_changes, take_changes, clear_changes],
            bytes context: Bytes = 3 [get_context, set_context, mut_context, take_context, clear_context],
        }
    }
}

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };
}

pub mod util {
    use crate::eraftpb::ConfState;

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.mut_voters().extend(voters);
            conf_state.mut_learners().extend(learners);
            conf_state
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::eraftpb::{
        ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType, Message, MessageType,
    };
    use crate::protocompat::PbMessage;
    use crate::{new_conf_change_single, ConfChangeI};

    /// Bytes upstream's generated code (rust-protobuf 2, raft-proto at ad13f3d) writes for the
    /// same values: the encoding is protobuf's, so either side reads the other's.
    #[test]
    fn messages_encode_as_upstream_does() {
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryConfChangeV2);
        entry.term = 3;
        entry.index = 300;
        entry.data = Bytes::from_static(b"ab");
        assert_eq!(
            entry.write_to_bytes().unwrap(),
            [0x08, 0x02, 0x10, 0x03, 0x18, 0xac, 0x02, 0x22, 0x02, b'a', b'b']
        );
        let mut message = Message::default();
        message.set_msg_type(MessageType::MsgAppend);
        message.to = 2;
        message.priority = -1;
        message.mut_entries().push(entry.clone());
        let bytes = message.write_to_bytes().unwrap();
        assert_eq!(bytes.len(), message.compute_size() as usize);
        assert_eq!(
            bytes,
            [
                0x08, 0x03, 0x10, 0x02, 0x3a, 0x0b, 0x08, 0x02, 0x10, 0x03, 0x18, 0xac, 0x02, 0x22,
                0x02, b'a', b'b', 0x80, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x01,
            ]
        );
        let mut read = Message::default();
        read.merge_from_bytes(&bytes).unwrap();
        assert_eq!(read, message);
        let mut conf_state = ConfState::default();
        conf_state.voters = vec![1, 300];
        conf_state.auto_leave = true;
        let bytes = conf_state.write_to_bytes().unwrap();
        assert_eq!(bytes, [0x08, 0x01, 0x08, 0xac, 0x02, 0x28, 0x01]);
        let mut read = ConfState::default();
        read.merge_from_bytes(&bytes).unwrap();
        assert_eq!(read, conf_state);
    }

    #[test]
    fn a_field_this_version_does_not_know_is_read_past() {
        let mut cc = ConfChangeV2::default();
        cc.mut_changes()
            .push(new_conf_change_single(4, ConfChangeType::AddLearnerNode));
        cc.context = Bytes::from_static(b"why");
        let mut bytes = cc.write_to_bytes().unwrap();
        // Field 9: a varint, then a delimited field, as a newer writer might add.
        bytes.extend_from_slice(&[0x48, 0x07, 0x4a, 0x02, 0xaa, 0xbb]);
        let mut read = ConfChangeV2::default();
        read.merge_from_bytes(&bytes).unwrap();
        assert_eq!(read, cc);
        assert_eq!(read.as_v2().get_changes().len(), 1);
    }

    #[test]
    fn a_truncated_message_is_an_error_not_a_panic() {
        let mut message = Message::default();
        message.mut_entries().push(Entry {
            data: Bytes::from_static(b"payload"),
            ..Default::default()
        });
        let bytes = message.write_to_bytes().unwrap();
        for end in 0..bytes.len() {
            let mut read = Message::default();
            if read.merge_from_bytes(&bytes[..end]).is_ok() {
                assert_ne!(read, message, "{end} bytes decoded as the whole message");
            }
        }
    }
}
