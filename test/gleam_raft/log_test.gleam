import gleam/option.{None, Some}
import gleam/set
import gleam_raft/cluster_config.{ClusterConfig}
import gleam_raft/log
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

fn two(
  prev: types.LogPosition,
  e0: log.LogEntry(String),
  e1: log.LogEntry(String),
) -> log.LogEntries(String) {
  log.entries_new(prev) |> log.entries_push(e0) |> log.entries_push(e1)
}

// 未検証の pub(crate) append 相当の連結だが、このテストの anchor は
// いずれも局所側が含むので entries_append_suffix で同一結果になる。
pub fn log_entries_append_test() -> Nil {
  let entries = log.entries_new(types.zero_position)
  should.equal(log.entries_len(entries), 0)
  should.equal(log_last(entries), pos(0, 0))

  // 末尾への追記。
  let #(entries, ok1) =
    log.entries_append_suffix(entries, two(pos(0, 0), term_entry(0), cmd("c1")))
  should.be_true(ok1)
  should.equal(log_last(entries), pos(0, 2))
  should.equal(log.entries_get_entry(entries, 0), None)
  should.equal(log.entries_get_entry(entries, 1), Some(term_entry(0)))
  should.equal(log.entries_get_entry(entries, 2), Some(cmd("c1")))

  // 再度、末尾への追記。
  let #(entries, ok2) =
    log.entries_append_suffix(entries, two(pos(0, 2), term_entry(1), cmd("c2")))
  should.be_true(ok2)
  should.equal(log_last(entries), pos(1, 4))
  should.equal(log.entries_get_entry(entries, 0), None)
  should.equal(log.entries_get_entry(entries, 1), Some(term_entry(0)))
  should.equal(log.entries_get_entry(entries, 2), Some(cmd("c1")))
  should.equal(log.entries_get_entry(entries, 3), Some(term_entry(1)))
  should.equal(log.entries_get_entry(entries, 4), Some(cmd("c2")))

  // 衝突する項目を切り詰めてから追記。
  let #(entries, ok3) =
    log.entries_append_suffix(entries, two(pos(1, 3), term_entry(2), cmd("c3")))
  should.be_true(ok3)
  should.equal(log_last(entries), pos(2, 5))
  should.equal(log.entries_get_entry(entries, 3), Some(term_entry(1)))
  should.equal(log.entries_get_entry(entries, 4), Some(term_entry(2)))
  should.equal(log.entries_get_entry(entries, 5), Some(cmd("c3")))

  // 衝突する項目を切り詰めてから追記、その 2 回目。
  let #(entries, ok4) =
    log.entries_append_suffix(entries, two(pos(0, 2), term_entry(3), cmd("c4")))
  should.be_true(ok4)
  should.equal(log_last(entries), pos(3, 4))
  should.equal(log.entries_get_entry(entries, 0), None)
  should.equal(log.entries_get_entry(entries, 1), Some(term_entry(0)))
  should.equal(log.entries_get_entry(entries, 2), Some(cmd("c1")))
  should.equal(log.entries_get_entry(entries, 3), Some(term_entry(3)))
  should.equal(log.entries_get_entry(entries, 4), Some(cmd("c4")))
}

fn log_last(e: log.LogEntries(String)) -> types.LogPosition {
  e.last_position
}

pub fn log_entries_append_suffix_replaces_local_suffix_test() -> Nil {
  let local =
    entries(types.zero_position, [term_entry(0), cmd("c1"), term_entry(1)])
  let suffix = two(pos(0, 2), term_entry(2), cmd("c2"))

  let #(merged, ok) = log.entries_append_suffix(local, suffix)
  should.be_true(ok)
  should.equal(log_last(merged), pos(2, 4))
  should.equal(log.entries_iter_with_positions(merged), [
    #(pos(0, 1), term_entry(0)),
    #(pos(0, 2), cmd("c1")),
    #(pos(2, 3), term_entry(2)),
    #(pos(2, 4), cmd("c2")),
  ])
}

pub fn log_entries_append_suffix_truncates_with_empty_suffix_test() -> Nil {
  let entries =
    entries(types.zero_position, [term_entry(0), cmd("c1"), term_entry(1)])
  let suffix = log.entries_new(pos(0, 2))

  let #(merged, ok) = log.entries_append_suffix(entries, suffix)
  should.be_true(ok)
  should.equal(merged.prev_position, types.zero_position)
  should.equal(log_last(merged), pos(0, 2))
  should.equal(log.entries_get_entry(merged, 3), None)
}

pub fn log_entries_append_suffix_returns_false_for_missing_anchor_test() -> Nil {
  let local =
    entries(types.zero_position, [term_entry(0), cmd("c1"), term_entry(1)])
  let suffix = entries(pos(0, 3), [cmd("x")])

  let #(merged, ok) = log.entries_append_suffix(local, suffix)
  should.be_false(ok)
  should.equal(suffix.prev_position, pos(0, 3))
  should.equal(merged, local)
}

pub fn log_append_suffix_preserves_snapshot_config_test() -> Nil {
  let snapshot_config =
    ClusterConfig(
      voters: set.from_list([1]),
      new_voters: set.new(),
      non_voters: set.new(),
    )
  let raft_log =
    log.log_new(
      snapshot_config,
      entries(types.zero_position, [term_entry(0), cmd("c1"), term_entry(1)]),
    )
  let suffix = two(pos(0, 2), term_entry(2), cmd("c2"))

  let #(merged, ok) = log.log_append_suffix(raft_log, suffix)
  should.be_true(ok)
  should.equal(merged.snapshot_config, snapshot_config)
  should.equal(
    merged.entries,
    entries(types.zero_position, [
      term_entry(0),
      cmd("c1"),
      term_entry(2),
      cmd("c2"),
    ]),
  )
}

// usize::MAX は JS 側の Number 正確値 2^53-1 に置き換える（D6）。
pub fn log_entries_truncate_with_large_len_has_no_effect_test() -> Nil {
  let entries = entries(pos(1, 5), [cmd("c1"), term_entry(2), cmd("c2")])

  let #(truncated, last) = log.entries_truncate(entries, 9_007_199_254_740_991)
  should.equal(truncated, entries)
  should.equal(last, pos(2, 8))
}

pub fn log_entries_truncate_after_high_index_snapshot_test() -> Nil {
  let snapshot_index = 4_294_967_295
  // u32::MAX
  let entries =
    entries(pos(1, snapshot_index), [cmd("c1"), term_entry(2), cmd("c2")])
  should.equal(log.entries_len(entries), 3)

  let #(truncated, last) = log.entries_truncate(entries, 1)
  should.equal(truncated.prev_position, pos(1, snapshot_index))
  should.equal(last, pos(1, snapshot_index + 1))
  should.equal(log.entries_iter_with_positions(truncated), [
    #(pos(1, snapshot_index + 1), cmd("c1")),
  ])
}

pub fn log_entries_since_test() -> Nil {
  let entries =
    entries(types.zero_position, [
      term_entry(0),
      cmd("c1"),
      term_entry(1),
      cmd("c2"),
      cmd("c3"),
    ])

  should.equal(log.entries_since(entries, pos(0, 0)), Some(entries))

  should.equal(
    log.entries_since(entries, pos(0, 2))
      |> option_iter,
    Some([
      #(pos(1, 3), term_entry(1)),
      #(pos(1, 4), cmd("c2")),
      #(pos(1, 5), cmd("c3")),
    ]),
  )

  should.equal(
    log.entries_since(entries, pos(1, 3)) |> option_iter,
    Some([#(pos(1, 4), cmd("c2")), #(pos(1, 5), cmd("c3"))]),
  )

  case log.entries_since(entries, pos(1, 5)) {
    Some(suffix) -> {
      should.be_true(log.entries_is_empty(suffix))
      should.equal(suffix.prev_position, pos(1, 5))
      should.equal(log_last(suffix), pos(1, 5))
    }
    None -> should.fail()
  }

  should.equal(log.entries_since(entries, pos(0, 3)), None)
  // 項が合わない
}

fn option_iter(
  result: option.Option(log.LogEntries(String)),
) -> option.Option(List(#(types.LogPosition, log.LogEntry(String)))) {
  case result {
    Some(entries) -> Some(log.entries_iter_with_positions(entries))
    None -> None
  }
}

pub fn log_entries_handle_snapshot_installed_keeps_suffix_test() -> Nil {
  let entries =
    entries(pos(1, 2), [cmd("c1"), cmd("c2"), term_entry(2), cmd("c3")])

  let #(installed, ok) =
    log.entries_handle_snapshot_installed(entries, pos(1, 4))
  should.be_true(ok)
  should.equal(installed.prev_position, pos(1, 4))
  should.equal(log_last(installed), pos(2, 6))
  should.equal(log.entries_iter_with_positions(installed), [
    #(pos(2, 5), term_entry(2)),
    #(pos(2, 6), cmd("c3")),
  ])
}

pub fn log_entries_handle_snapshot_installed_discards_incompatible_suffix_test() -> Nil {
  let entries = entries(pos(1, 2), [cmd("c1"), cmd("c2")])

  let #(installed, ok) =
    log.entries_handle_snapshot_installed(entries, pos(2, 3))
  should.be_false(ok)
  should.be_true(log.entries_is_empty(installed))
  should.equal(installed.prev_position, pos(2, 3))
  should.equal(log_last(installed), pos(2, 3))
}

pub fn log_entries_strip_common_prefix_test() -> Nil {
  let local =
    entries(types.zero_position, [
      term_entry(0),
      cmd("c1"),
      term_entry(1),
      cmd("c2"),
      cmd("c3"),
    ])
  should.equal(log_last(local), pos(1, 5))

  // remote.prev == local.last
  let remote = entries(pos(1, 5), [cmd("r")])
  should.equal(
    log.entries_strip_common_prefix(remote, local).prev_position,
    pos(1, 5),
  )

  // 分岐なし
  let remote = entries(pos(1, 4), [cmd("r1"), cmd("r2")])
  should.equal(
    log.entries_strip_common_prefix(remote, local).prev_position,
    pos(1, 5),
  )

  // 分岐あり
  let remote = entries(pos(1, 4), [term_entry(2), cmd("r1"), term_entry(3)])
  should.equal(
    log.entries_strip_common_prefix(remote, local).prev_position,
    pos(1, 4),
  )

  let remote = entries(pos(1, 3), [term_entry(1), term_entry(2), cmd("r1")])
  should.equal(
    log.entries_strip_common_prefix(remote, local).prev_position,
    pos(1, 4),
  )
}

pub fn log_entries_strip_common_prefix_falls_back_outside_local_range_test() -> Nil {
  let local =
    entries(types.zero_position, [
      term_entry(1),
      cmd("c1"),
      cmd("c2"),
      term_entry(3),
    ])
  let remote = entries(pos(1, 3), [cmd("r1"), cmd("r2"), term_entry(2)])

  should.be_true(log.entries_contains(local, remote.prev_position))
  should.be_false(log.entries_contains(local, log_last(remote)))
  should.equal(log.entries_strip_common_prefix(remote, local), remote)
}

pub fn strip_common_prefix_with_config_entry_no_terms_test() -> Nil {
  let config =
    ClusterConfig(
      voters: set.from_list([0]),
      new_voters: set.from_list([0, 1]),
      non_voters: set.new(),
    )
  // remote は index 1 に構成項目のみで、Term マーカーを持たない。
  let remote =
    log.entries_push(
      log.entries_new(types.zero_position),
      log.ConfigEntry(config),
    )
  let local =
    entries(types.zero_position, [
      term_entry(1),
      cmd("c1"),
      cmd("c2"),
      log.ConfigEntry(config),
    ])

  // panic してはいけないという元の意図どおり、anchor は ZERO のまま。
  should.equal(
    log.entries_strip_common_prefix(remote, local).prev_position,
    types.zero_position,
  )
}

// 自前の追加: 0 件への切り詰めと範囲外参照の境界。
pub fn entries_truncate_to_zero_and_out_of_range_test() -> Nil {
  let entries = entries(pos(1, 5), [cmd("c1"), term_entry(2), cmd("c2")])

  let #(truncated, last) = log.entries_truncate(entries, 0)
  should.be_true(log.entries_is_empty(truncated))
  should.equal(last, pos(1, 5))
  should.equal(log.entries_get_entry(truncated, 5), None)
  // anchor は断片の外
  should.equal(log.entries_get_term(truncated, 5), Some(1))
  // 番兵由来
  should.equal(log.entries_get_term(truncated, 4), None)
  // 範囲外
  should.equal(log.entries_get_term(truncated, 7), None)
  // 範囲外
  should.equal(log.entries_get_entry(truncated, 7), None)
}

// 自前の追加: Log 級のスナップショット適用で構成を差し替え、
// 非互換 suffix では false を返す。
pub fn log_handle_snapshot_installed_test() -> Nil {
  let old_config =
    ClusterConfig(
      voters: set.from_list([1]),
      new_voters: set.new(),
      non_voters: set.new(),
    )
  let new_config =
    ClusterConfig(
      voters: set.from_list([1, 2]),
      new_voters: set.new(),
      non_voters: set.new(),
    )
  let raft_log =
    log.log_new(
      old_config,
      entries(pos(1, 2), [cmd("c1"), cmd("c2"), term_entry(2), cmd("c3")]),
    )

  // suffix が position を含む場合は残し、構成は差し替える。
  let #(installed, ok) =
    log.log_handle_snapshot_installed(raft_log, pos(1, 4), new_config)
  should.be_true(ok)
  should.equal(installed.snapshot_config, new_config)
  should.equal(installed.entries.prev_position, pos(1, 4))
  should.equal(log.entries_iter_with_positions(installed.entries), [
    #(pos(2, 5), term_entry(2)),
    #(pos(2, 6), cmd("c3")),
  ])
  should.equal(log.log_latest_config(installed), new_config)
  // 保持断片に構成マーカーが残らないため、接続点の index を返す。
  should.equal(log.log_latest_config_index(installed), 4)

  // 項が合わず suffix を捨てる場合は false、構成は差し替わる。
  let #(reset, ok_reset) =
    log.log_handle_snapshot_installed(raft_log, pos(2, 3), new_config)
  should.be_false(ok_reset)
  should.equal(reset.snapshot_config, new_config)
  should.be_true(log.entries_is_empty(reset.entries))
}
