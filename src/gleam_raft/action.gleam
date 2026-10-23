//// Node が実行すべき I/O 操作を表す Action と、その未実行集合 Actions。
////
//// 意図的な設計上の差分は D4 のみ。AppendLogEntries の連結が anchor
//// 不一致で失敗したときは、panic 相当の代わりに新値で置換する。

import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set

import gleam_raft/log.{type LogEntries, entries_append_suffix}
import gleam_raft/message.{type Message, message_merge}
import gleam_raft/types.{type NodeId}

/// Node が実行すべき I/O 操作。sans-IO なので実行は呼び出し側の責務。
pub type Action(command) {
  /// 選挙タイムアウト（または leader の heartbeat タイムアウト）を設定する。
  SetElectionTimeout
  /// 現在の項を永続ストレージへ保存する。
  SaveCurrentTerm
  /// voted_for を永続ストレージへ保存する。
  SaveVotedFor
  /// 永続ストレージのログへ末尾を追記する。
  AppendLogEntries(entries: LogEntries(command))
  /// 他の全ノードへメッセージをブロードキャストする。
  BroadcastMessage(message: Message(command))
  /// 特定のノードへメッセージを送る。
  SendMessage(to: NodeId, message: Message(command))
  /// 特定のノードへスナップショットをインストールする。
  InstallSnapshot(to: NodeId)
}

/// 未実行 Action の集合。`actions_set` で同種の Action を格納時にマージする。
///
/// 取り出し順は `actions_take` を参照。フィールドは Action の種類と 1 対 1。
pub type Actions(command) {
  Actions(
    set_election_timeout: Bool,
    save_current_term: Bool,
    save_voted_for: Bool,
    append_log_entries: Option(LogEntries(command)),
    broadcast_message: Option(Message(command)),
    send_messages: dict.Dict(NodeId, Message(command)),
    install_snapshots: set.Set(NodeId),
  )
}

/// 空の集合を作る。
pub fn actions_empty() -> Actions(command) {
  Actions(
    set_election_timeout: False,
    save_current_term: False,
    save_voted_for: False,
    append_log_entries: None,
    broadcast_message: None,
    send_messages: dict.new(),
    install_snapshots: set.new(),
  )
}

/// 実行すべき Action が 1 件もないなら True。
pub fn actions_is_empty(actions: Actions(command)) -> Bool {
  !actions.set_election_timeout
  && !actions.save_current_term
  && !actions.save_voted_for
  && actions.append_log_entries == None
  && actions.broadcast_message == None
  && dict.is_empty(actions.send_messages)
  && set.is_empty(actions.install_snapshots)
}

/// Action を 1 件集合へ入れる。既存の同種があれば格納時にマージする。
///
/// マージ規則は DESIGN §7 の表どおり。bool 系は true へ立てるだけ（冪等）、
/// AppendLogEntries は連結（失敗時は置換、D4）、メッセージ系は
/// `message_merge`、InstallSnapshot は Set への挿入。
pub fn actions_set(
  actions: Actions(command),
  action: Action(command),
) -> Actions(command) {
  case action {
    SetElectionTimeout -> Actions(..actions, set_election_timeout: True)
    SaveCurrentTerm -> Actions(..actions, save_current_term: True)
    SaveVotedFor -> Actions(..actions, save_voted_for: True)
    AppendLogEntries(entries: new) -> {
      let merged = case actions.append_log_entries {
        None -> new
        Some(existing) ->
          case entries_append_suffix(existing, new) {
            #(merged_entries, True) -> merged_entries
            #(_, False) -> new
          }
      }
      Actions(..actions, append_log_entries: Some(merged))
    }
    BroadcastMessage(message: msg) -> {
      let merged = case actions.broadcast_message {
        None -> msg
        Some(existing) -> message_merge(existing, msg)
      }
      Actions(..actions, broadcast_message: Some(merged))
    }
    SendMessage(to: to, message: msg) -> {
      let merged = case dict.get(actions.send_messages, to) {
        Ok(existing) -> message_merge(existing, msg)
        Error(_) -> msg
      }
      Actions(
        ..actions,
        send_messages: dict.insert(actions.send_messages, to, merged),
      )
    }
    InstallSnapshot(to: to) ->
      Actions(
        ..actions,
        install_snapshots: set.insert(actions.install_snapshots, to),
      )
  }
}

/// 優先順位で Action を 1 件取り出し、取り出し済みフィールドを初期値へ戻す。
///
/// 順は set_election_timeout → save_current_term → save_voted_for →
/// append_log_entries → broadcast_message → send_messages（昇順 1 件）→
/// install_snapshots（昇順 1 件）。局所永続化を送信より先に置くのは、
/// solo voter が commit を永続化前に公開しないため。
pub fn actions_take(
  actions: Actions(command),
) -> #(Option(Action(command)), Actions(command)) {
  case actions.set_election_timeout {
    True -> #(
      Some(SetElectionTimeout),
      Actions(..actions, set_election_timeout: False),
    )
    False ->
      case actions.save_current_term {
        True -> #(
          Some(SaveCurrentTerm),
          Actions(..actions, save_current_term: False),
        )
        False ->
          case actions.save_voted_for {
            True -> #(
              Some(SaveVotedFor),
              Actions(..actions, save_voted_for: False),
            )
            False ->
              case actions.append_log_entries {
                Some(entries) -> #(
                  Some(AppendLogEntries(entries)),
                  Actions(..actions, append_log_entries: None),
                )
                None ->
                  case actions.broadcast_message {
                    Some(msg) -> #(
                      Some(BroadcastMessage(msg)),
                      Actions(..actions, broadcast_message: None),
                    )
                    None ->
                      case take_min_send(actions.send_messages) {
                        Some(#(to, msg, rest)) -> #(
                          Some(SendMessage(to, msg)),
                          Actions(..actions, send_messages: rest),
                        )
                        None ->
                          case take_min_snapshot(actions.install_snapshots) {
                            Some(#(to, rest)) -> #(
                              Some(InstallSnapshot(to)),
                              Actions(..actions, install_snapshots: rest),
                            )
                            None -> #(None, actions)
                          }
                      }
                  }
              }
          }
      }
  }
}

fn take_min_send(
  messages: dict.Dict(NodeId, Message(command)),
) -> Option(#(NodeId, Message(command), dict.Dict(NodeId, Message(command)))) {
  case
    messages
    |> dict.to_list
    |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
  {
    [] -> None
    [#(to, msg), ..rest] -> Some(#(to, msg, dict.from_list(rest)))
  }
}

fn take_min_snapshot(
  targets: set.Set(NodeId),
) -> Option(#(NodeId, set.Set(NodeId))) {
  case targets |> set.to_list |> list.sort(int.compare) {
    [] -> None
    [to, ..] -> Some(#(to, set.delete(targets, to)))
  }
}
