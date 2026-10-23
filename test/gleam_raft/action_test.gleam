import gleam/option.{None, Some}
import gleam_raft/action
import gleam_raft/log
import gleam_raft/message
import gleam_raft/types
import gleeunit/should

fn pos(term: Int, index: Int) -> types.LogPosition {
  types.position(term, index)
}

fn cmd(text: String) -> log.LogEntry(String) {
  log.CommandEntry(text)
}

fn entries(
  prev: types.LogPosition,
  items: List(log.LogEntry(String)),
) -> log.LogEntries(String) {
  log.entries_from_list(prev, items)
}

fn rvc(
  term: Int,
  from: Int,
  last_log_position: types.LogPosition,
) -> message.Message(String) {
  message.RequestVoteCall(term:, from:, last_log_position:)
}

fn aec(
  term: Int,
  from: Int,
  leader_commit: Int,
  entries_: log.LogEntries(String),
) -> message.Message(String) {
  message.AppendEntriesCall(term:, from:, leader_commit:, entries: entries_)
}

pub fn actions_set_test() -> Nil {
  let assert #(None, a) = action.actions_take(action.actions_empty())

  // SetElectionTimeout は冪等。2 回入れても取り出しは 1 回。
  let a =
    a
    |> action.actions_set(action.SetElectionTimeout)
    |> action.actions_set(action.SetElectionTimeout)
  let assert #(Some(action.SetElectionTimeout), a) = action.actions_take(a)
  let assert #(None, a) = action.actions_take(a)

  // SaveCurrentTerm
  let a =
    a
    |> action.actions_set(action.SaveCurrentTerm)
    |> action.actions_set(action.SaveCurrentTerm)
  let assert #(Some(action.SaveCurrentTerm), a) = action.actions_take(a)
  let assert #(None, a) = action.actions_take(a)

  // SaveVotedFor
  let a =
    a
    |> action.actions_set(action.SaveVotedFor)
    |> action.actions_set(action.SaveVotedFor)
  let assert #(Some(action.SaveVotedFor), a) = action.actions_take(a)
  let assert #(None, a) = action.actions_take(a)

  // BroadcastMessage は message_merge に委譲され、RVC の後に AE Call が入ると
  // AE Call になる。
  let a =
    a
    |> action.actions_set(action.BroadcastMessage(rvc(2, 3, pos(2, 8))))
    |> action.actions_set(
      action.BroadcastMessage(aec(2, 3, 10, log.entries_new(pos(2, 10)))),
    )
  let assert #(Some(action.BroadcastMessage(msg)), a) = action.actions_take(a)
  let assert message.AppendEntriesCall(..) = msg
  let assert #(None, a) = action.actions_take(a)

  // AppendLogEntries は anchor が連続していれば連結される。
  let a =
    a
    |> action.actions_set(
      action.AppendLogEntries(entries(pos(2, 3), [cmd("a")])),
    )
    |> action.actions_set(
      action.AppendLogEntries(entries(pos(2, 4), [cmd("b")])),
    )
  let assert #(Some(action.AppendLogEntries(merged)), a) =
    action.actions_take(a)
  merged |> should.equal(entries(pos(2, 3), [cmd("a"), cmd("b")]))
  let assert #(None, a) = action.actions_take(a)

  // SendMessage は宛先ごとに保持され、昇順（2 が先）で取り出される。
  let a =
    a
    |> action.actions_set(action.SendMessage(4, rvc(2, 3, pos(2, 8))))
    |> action.actions_set(action.SendMessage(
      2,
      aec(2, 3, 10, log.entries_new(pos(2, 10))),
    ))
  let assert #(Some(action.SendMessage(2, msg)), a) = action.actions_take(a)
  let assert message.AppendEntriesCall(..) = msg
  let assert #(Some(action.SendMessage(4, msg)), a) = action.actions_take(a)
  let assert message.RequestVoteCall(..) = msg
  let assert #(None, a) = action.actions_take(a)

  // InstallSnapshot は Set なので重複が消え、昇順で取り出される。
  let a =
    a
    |> action.actions_set(action.InstallSnapshot(3))
    |> action.actions_set(action.InstallSnapshot(2))
    |> action.actions_set(action.InstallSnapshot(3))
  let assert #(Some(action.InstallSnapshot(2)), a) = action.actions_take(a)
  let assert #(Some(action.InstallSnapshot(3)), a) = action.actions_take(a)
  let assert #(None, a) = action.actions_take(a)
  should.be_true(a |> action.actions_is_empty)
}

pub fn actions_iter_prioritizes_log_append_before_broadcast_test() -> Nil {
  let entries = entries(pos(2, 3), [cmd("a")])
  let msg = aec(2, 3, 4, log.entries_new(pos(2, 4)))
  let a =
    action.actions_empty()
    |> action.actions_set(action.BroadcastMessage(msg))
    |> action.actions_set(action.AppendLogEntries(entries))

  // 局所永続化が送信より先。取り出し順は登録順に依存しない。
  let assert #(Some(action.AppendLogEntries(taken)), a) = action.actions_take(a)
  taken |> should.equal(entries)
  let assert #(Some(action.BroadcastMessage(taken)), a) = action.actions_take(a)
  taken |> should.equal(msg)
  let assert #(None, a) = action.actions_take(a)
  should.be_true(a |> action.actions_is_empty)
}

pub fn actions_merge_same_kind_once_and_kinds_independent_test() -> Nil {
  let a0 = action.actions_empty()
  a0 |> action.actions_is_empty |> should.be_true
  let a =
    a0
    |> action.actions_set(action.SetElectionTimeout)
    |> action.actions_set(action.SaveCurrentTerm)
    |> action.actions_set(action.SaveVotedFor)
    |> action.actions_set(
      action.AppendLogEntries(entries(pos(2, 3), [cmd("a")])),
    )
    |> action.actions_set(action.BroadcastMessage(rvc(2, 3, pos(2, 8))))
    |> action.actions_set(action.SendMessage(2, rvc(2, 3, pos(2, 8))))
    |> action.actions_set(action.SendMessage(
      2,
      aec(2, 3, 10, log.entries_new(pos(2, 10))),
    ))
    |> action.actions_set(action.InstallSnapshot(2))
    |> action.actions_set(action.InstallSnapshot(2))
  a |> action.actions_is_empty |> should.be_false

  // 同種マージは 1 件のまま。SendMessage は同一宛先 2 件が AE Call に
  // マージされ、InstallSnapshot の重複は 1 件になる。異なる種類は互いに
  // 干渉せず、優先順どおりに取り出される。
  let assert #(Some(action.SetElectionTimeout), a) = action.actions_take(a)
  let assert #(Some(action.SaveCurrentTerm), a) = action.actions_take(a)
  let assert #(Some(action.SaveVotedFor), a) = action.actions_take(a)
  let assert #(Some(action.AppendLogEntries(_)), a) = action.actions_take(a)
  let assert #(Some(action.BroadcastMessage(_)), a) = action.actions_take(a)
  let assert #(Some(action.SendMessage(2, merged)), a) = action.actions_take(a)
  let assert message.AppendEntriesCall(..) = merged
  let assert #(Some(action.InstallSnapshot(2)), a) = action.actions_take(a)
  let assert #(None, a) = action.actions_take(a)
  a |> action.actions_is_empty |> should.be_true
}

pub fn actions_append_log_entries_replaces_on_suffix_failure_test() -> Nil {
  // D4: anchor 不一致で append_suffix が失敗した連結は、新値での置換に
  // フォールバックする（panic 相当の代わりに）。
  let a =
    action.actions_empty()
    |> action.actions_set(
      action.AppendLogEntries(entries(pos(2, 3), [cmd("a")])),
    )
    |> action.actions_set(
      action.AppendLogEntries(entries(pos(9, 9), [cmd("z")])),
    )
  let assert #(Some(action.AppendLogEntries(stored)), a) =
    action.actions_take(a)
  stored |> should.equal(entries(pos(9, 9), [cmd("z")]))
  let assert #(None, a) = action.actions_take(a)
  should.be_true(a |> action.actions_is_empty)
}
