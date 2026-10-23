//// RPC メッセージのデータ型とその純粋な変換。
////
//// InstallSnapshot RPC はスナップショットインストールの細部がアプリ依存
//// なため含めない。DESIGN §2 D1 により、可変参照の代わりに値を返す形に
//// した。panic 相当の debug_assert は備えない（DESIGN §7）。

import gleam/option.{None, Some}
import gleam_raft/log.{type LogEntries}
import gleam_raft/types.{type LogIndex, type LogPosition, type NodeId, type Term}

/// RPC のメッセージ。
pub type Message(command) {
  /// RequestVote RPC の呼び出し。
  RequestVoteCall(term: Term, from: NodeId, last_log_position: LogPosition)
  /// RequestVote RPC の応答。
  RequestVoteReply(term: Term, from: NodeId, vote_granted: Bool)
  /// AppendEntries RPC の呼び出し。
  ///
  /// entries が大きすぎる場合は送信前に `log.entries_truncate` で縮められる。
  /// 一部を送信した後は `strip_append_entries_prefix` で送信済み prefix を
  /// 落とせる。
  AppendEntriesCall(
    term: Term,
    from: NodeId,
    leader_commit: LogIndex,
    entries: LogEntries(command),
  )
  /// AppendEntries RPC の応答。
  ///
  /// Raft 論文のブール値 success の代わりにフォロワの最終位置を返す。
  /// 大幅に遅れたフォロワでも leader は即座に match index を確定できる。
  AppendEntriesReply(term: Term, from: NodeId, last_log_position: LogPosition)
}

/// 差出人のノード ID を返す。
pub fn message_from(message: Message(command)) -> NodeId {
  message.from
}

/// メッセージの term を返す。
pub fn message_term(message: Message(command)) -> Term {
  message.term
}

/// 未消費のメッセージをマージする。
///
/// 双方が AE Call で self の entries が other の接続点を含むなら entries を
/// 連結し、その他は other で置き換える。連結時も from / term / leader_commit は
/// other の方を使う。同一差出人・term 単調という前提は検証しない。
pub fn message_merge(
  self: Message(command),
  other: Message(command),
) -> Message(command) {
  case self, other {
    AppendEntriesCall(..), AppendEntriesCall(..) ->
      case log.entries_append_suffix(self.entries, other.entries) {
        #(merged_entries, True) ->
          AppendEntriesCall(..other, entries: merged_entries)
        #(_, False) -> other
      }
    _, _ -> other
  }
}

/// AE Call なら先頭の entries を `sent_count` 個落とす。
///
/// 戻り値の Bool は AE Call で処理が行われたことを表す（AE Call でなければ
/// 第 1 要素はそのまま、第 2 要素は False）。`sent_count` が entries 数以上の
/// 場合は全除去となり、接続点は最終位置になる。
pub fn strip_append_entries_prefix(
  message: Message(command),
  sent_count: Int,
) -> #(Message(command), Bool) {
  case message {
    AppendEntriesCall(..) -> #(
      AppendEntriesCall(
        ..message,
        entries: strip_entries(message.entries, sent_count),
      ),
      True,
    )
    _ -> #(message, False)
  }
}

/// `strip_append_entries_prefix` の entries 部。
///
/// 部分除去で引く新しい接続点の index は (prev, last] の範囲に保証されるため
/// `get_term` も `since` も Some が成り立つ。None 分岐は到達不能な防御として
/// 無操作で返す。
fn strip_entries(
  entries: LogEntries(command),
  sent_count: Int,
) -> LogEntries(command) {
  case sent_count == 0 {
    True -> entries
    False ->
      case sent_count >= log.entries_len(entries) {
        True -> log.entries_new(entries.last_position)
        False -> {
          let new_prev_index = entries.prev_position.index + sent_count
          case log.entries_get_term(entries, new_prev_index) {
            Some(new_prev_term) ->
              case
                log.entries_since(
                  entries,
                  types.position(new_prev_term, new_prev_index),
                )
              {
                Some(suffix) -> suffix
                None -> entries
              }
            None -> entries
          }
        }
      }
  }
}

/// スナップショットインストール済みとして位置を position に張り替える。
///
/// メッセージ自身の term は更新しない。RVR は保持する位置が
/// ないため不変。位置を持つメッセージは position.index を下回るときだけ
/// 張り替える。AE Call の entries は log 側の処理に委譲し、成否は捨てる。
pub fn message_handle_snapshot_installed(
  message: Message(command),
  position: LogPosition,
) -> Message(command) {
  case message {
    RequestVoteCall(..) ->
      case message.last_log_position.index < position.index {
        True -> RequestVoteCall(..message, last_log_position: position)
        False -> message
      }
    RequestVoteReply(..) -> message
    AppendEntriesCall(..) -> {
      let #(kept_entries, _retained) =
        log.entries_handle_snapshot_installed(message.entries, position)
      AppendEntriesCall(..message, entries: kept_entries)
    }
    AppendEntriesReply(..) ->
      case message.last_log_position.index < position.index {
        True -> AppendEntriesReply(..message, last_log_position: position)
        False -> message
      }
  }
}
