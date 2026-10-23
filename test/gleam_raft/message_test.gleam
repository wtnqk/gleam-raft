import gleam_raft/log
import gleam_raft/message.{
  AppendEntriesCall, AppendEntriesReply, RequestVoteCall, RequestVoteReply,
}
import gleam_raft/types
import gleeunit/should

fn pos(term: Int, index: Int) -> types.LogPosition {
  types.position(term, index)
}

fn term_entry(term: Int) -> log.LogEntry(String) {
  log.TermEntry(term)
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

fn sample_entries() -> log.LogEntries(String) {
  entries(pos(2, 10), [cmd("c1"), term_entry(3), cmd("c2")])
}

pub fn strip_append_entries_prefix_for_append_entries_call_test() -> Nil {
  let call =
    AppendEntriesCall(
      term: 3,
      from: 1,
      leader_commit: 5,
      entries: sample_entries(),
    )
  let #(stripped, applied) = message.strip_append_entries_prefix(call, 1)
  should.be_true(applied)
  case stripped {
    AppendEntriesCall(term:, from:, leader_commit:, entries:) -> {
      should.equal(term, 3)
      should.equal(from, 1)
      should.equal(leader_commit, 5)
      should.equal(log.entries_len(entries), 2)
      should.equal(entries.prev_position, pos(2, 11))
      should.equal(entries.last_position, pos(3, 13))
      should.equal(entries.entries, [term_entry(3), cmd("c2")])
    }
    _ -> should.fail()
  }
}

pub fn strip_append_entries_prefix_with_zero_len_test() -> Nil {
  let original_entries = entries(pos(2, 10), [cmd("c1"), cmd("c2")])
  let call =
    AppendEntriesCall(
      term: 3,
      from: 1,
      leader_commit: 5,
      entries: original_entries,
    )
  let #(stripped, applied) = message.strip_append_entries_prefix(call, 0)
  should.be_true(applied)
  case stripped {
    AppendEntriesCall(entries:, ..) -> should.equal(entries, original_entries)
    _ -> should.fail()
  }
}

// 境界値 usize::MAX は D6 の 2^53-1 に置き換える。
pub fn strip_append_entries_prefix_drops_all_entries_test() -> Nil {
  let all_entries = sample_entries()
  let old_last_position = all_entries.last_position
  let call =
    AppendEntriesCall(term: 3, from: 1, leader_commit: 5, entries: all_entries)
  let #(stripped, applied) = message.strip_append_entries_prefix(call, 3)
  should.be_true(applied)
  case stripped {
    AppendEntriesCall(entries:, ..) -> {
      should.be_true(log.entries_is_empty(entries))
      should.equal(entries.prev_position, old_last_position)
      should.equal(entries.last_position, old_last_position)
      // 空に対する全除去も同じ結果になる。
      let #(again, applied_again) =
        message.strip_append_entries_prefix(stripped, 9_007_199_254_740_991)
      should.be_true(applied_again)
      case again {
        AppendEntriesCall(entries: again_entries, ..) -> {
          should.be_true(log.entries_is_empty(again_entries))
          should.equal(again_entries.prev_position, old_last_position)
          should.equal(again_entries.last_position, old_last_position)
        }
        _ -> should.fail()
      }
    }
    _ -> should.fail()
  }
}

pub fn strip_append_entries_prefix_non_append_entries_call_is_noop_test() -> Nil {
  let request_vote =
    RequestVoteCall(term: 3, from: 1, last_log_position: pos(3, 5))
  let #(stripped, applied) =
    message.strip_append_entries_prefix(request_vote, 1)
  should.be_false(applied)
  should.equal(stripped, request_vote)
}

pub fn handle_snapshot_installed_keeps_request_vote_reply_term_test() -> Nil {
  let vote_reply = RequestVoteReply(term: 10, from: 1, vote_granted: True)
  let installed =
    message.message_handle_snapshot_installed(vote_reply, pos(12, 5))
  should.equal(
    installed,
    RequestVoteReply(term: 10, from: 1, vote_granted: True),
  )
}

pub fn handle_snapshot_installed_keeps_append_entries_reply_term_test() -> Nil {
  let reply =
    AppendEntriesReply(term: 10, from: 1, last_log_position: pos(10, 3))
  let installed = message.message_handle_snapshot_installed(reply, pos(12, 5))
  should.equal(
    installed,
    AppendEntriesReply(term: 10, from: 1, last_log_position: pos(12, 5)),
  )
}

pub fn handle_snapshot_installed_keeps_request_vote_call_term_test() -> Nil {
  let call = RequestVoteCall(term: 10, from: 1, last_log_position: pos(3, 2))
  let installed = message.message_handle_snapshot_installed(call, pos(12, 5))
  should.equal(
    installed,
    RequestVoteCall(term: 10, from: 1, last_log_position: pos(12, 5)),
  )
}

pub fn handle_snapshot_installed_keeps_append_entries_call_term_test() -> Nil {
  let call =
    AppendEntriesCall(
      term: 10,
      from: 1,
      leader_commit: 0,
      entries: log.entries_new(pos(3, 2)),
    )
  let installed = message.message_handle_snapshot_installed(call, pos(12, 5))
  case installed {
    AppendEntriesCall(term:, entries:, ..) -> {
      should.equal(term, 10)
      should.equal(entries.prev_position, pos(12, 5))
    }
    _ -> should.fail()
  }
}

// 自前追加（TESTPLAN §4）: prefix 完全一致での連結と、それ以外（不連続・非 AE
// Call）の other 置換の境界。from / term / leader_commit が other 側になることも
// 確認する。
pub fn message_merge_ae_call_boundary_test() -> Nil {
  let self_call =
    AppendEntriesCall(
      term: 5,
      from: 1,
      leader_commit: 0,
      entries: entries(types.zero_position, [cmd("c1")]),
    )
  let contiguous =
    AppendEntriesCall(
      term: 6,
      from: 1,
      leader_commit: 1,
      entries: entries(types.position(0, 1), [cmd("c2")]),
    )
  case message.message_merge(self_call, contiguous) {
    AppendEntriesCall(term:, from:, leader_commit:, entries: merged) -> {
      should.equal(term, 6)
      should.equal(from, 1)
      should.equal(leader_commit, 1)
      should.equal(merged, entries(types.zero_position, [cmd("c1"), cmd("c2")]))
    }
    _ -> should.fail()
  }
  let discontinuous =
    AppendEntriesCall(
      term: 6,
      from: 1,
      leader_commit: 1,
      entries: entries(types.position(0, 5), [cmd("c9")]),
    )
  should.equal(message.message_merge(self_call, discontinuous), discontinuous)
  let vote_reply = RequestVoteReply(term: 6, from: 1, vote_granted: False)
  should.equal(message.message_merge(self_call, vote_reply), vote_reply)
  let vote_call =
    RequestVoteCall(term: 5, from: 1, last_log_position: types.zero_position)
  should.equal(message.message_merge(vote_call, contiguous), contiguous)
}
