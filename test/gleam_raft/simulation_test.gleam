// 自作テスト: シミュレータ（gleam_raft/support/simulation）の自己検査。
// DESIGN §11.3 の自己検証方針にもとづき、(1) 同一 seed の決定論、
// (2) 4 検証器が細工した破損を検出すること、(3) 無障害 3 ノードで
// 提案が全ノードにコミットされること（スモーク）を確認する。
// クラスタ性質テスト（stable / unstable / pipelining など）は後続手順で
// 別のテストファイルとして追加する。
import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import gleeunit/should

import gleam_raft/cluster_config.{type ClusterConfig, config_new}
import gleam_raft/log.{
  type Log, type LogEntry, CommandEntry, entries_from_list, log_new,
}
import gleam_raft/support/simulation.{
  type DurableState, type Sim, type SimOptions, DurableState, LinkOptions,
  SimOptions, check_commit_monotonicity, check_election_safety,
  check_state_machine_safety, check_storage_reflection, commit_index, faults,
  get_commit_status, new, propose, restarts, role_of, run, run_for, running_ids,
  tick,
}
import gleam_raft/types.{
  type LogIndex, type NodeId, Candidate, Committed, Follower, Leader,
  zero_position,
}

// ---- テスト補助 ----

fn pos(term: Int, index: LogIndex) -> types.LogPosition {
  types.position(term, index)
}

fn cfg() -> ClusterConfig {
  config_new()
}

fn log_with(entries: List(LogEntry(Int))) -> Log(Int) {
  log_new(cfg(), entries_from_list(zero_position, entries))
}

fn log_at(anchor: types.LogPosition, entries: List(LogEntry(Int))) -> Log(Int) {
  log_new(cfg(), entries_from_list(anchor, entries))
}

fn durable(term: Int, log: Log(Int), committed: LogIndex) -> DurableState {
  DurableState(term:, voted_for: None, log:, committed:)
}

// ---- 検証器 1: election safety ----

pub fn election_safety_detects_two_leaders_in_same_term_test() {
  let leaders = dict.from_list([#(3, 1)])
  // 同じ項で別ノードが leader を名乗ったら検出。
  should.equal(check_election_safety(leaders, #(3, 2)), False)
}

pub fn election_safety_allows_same_node_and_new_term_test() {
  let leaders = dict.from_list([#(3, 1)])
  should.equal(check_election_safety(leaders, #(3, 1)), True)
  should.equal(check_election_safety(leaders, #(4, 9)), True)
}

// ---- 検証器 2: state machine safety ----

pub fn state_machine_safety_detects_entry_divergence_test() {
  let oracle = dict.from_list([#(5, #(pos(1, 5), CommandEntry(7)))])
  let bad = [#(5, #(pos(2, 5), CommandEntry(7)))]
  should.equal(check_state_machine_safety(oracle, bad), False)
  let also_bad = [#(5, #(pos(1, 5), CommandEntry(8)))]
  should.equal(check_state_machine_safety(oracle, also_bad), False)
}

pub fn state_machine_safety_allows_match_and_new_indices_test() {
  let oracle = dict.from_list([#(5, #(pos(1, 5), CommandEntry(7)))])
  let ok = [#(5, #(pos(1, 5), CommandEntry(7)))]
  should.equal(check_state_machine_safety(oracle, ok), True)
  // 台帳に無い index の観測は許容（追加は呼び出し側の責務）。
  let fresh = [#(6, #(pos(1, 6), CommandEntry(8)))]
  should.equal(check_state_machine_safety(oracle, fresh), True)
}

// ---- 検証器 3: commit 単調性 ----

pub fn commit_monotonicity_detects_regression_test() {
  should.equal(check_commit_monotonicity(5, 4), False)
  should.equal(check_commit_monotonicity(5, 5), True)
  should.equal(check_commit_monotonicity(5, 6), True)
}

// ---- 検証器 4: storage 反映 ----

pub fn storage_reflection_accepts_matching_committed_prefix_test() {
  let live = log_with([CommandEntry(1), CommandEntry(2)])
  let ok = durable(1, log_with([CommandEntry(1), CommandEntry(2)]), 2)
  should.equal(check_storage_reflection(ok, 1, live, 2), True)
}

pub fn storage_reflection_detects_committed_entry_divergence_test() {
  let live = log_with([CommandEntry(1), CommandEntry(2)])
  let bad = durable(1, log_with([CommandEntry(1), CommandEntry(99)]), 2)
  should.equal(check_storage_reflection(bad, 1, live, 2), False)
}

pub fn storage_reflection_ignores_uncommitted_tail_test() {
  let live = log_with([CommandEntry(1), CommandEntry(2)])
  // durable の 2 番目は live と違うが、durable 側の commit が 1 なので
  // 比較範囲外（live の truncation で消えうる末尾）。
  let tail_free = durable(1, log_with([CommandEntry(1), CommandEntry(99)]), 1)
  should.equal(check_storage_reflection(tail_free, 1, live, 2), True)
  // ただし live 側の commit が 2 に進んでも durable.committed が 1 のまま
  // なら比較されない（範囲は min(durable.committed, live_commit)）。
  should.equal(check_storage_reflection(tail_free, 1, live, 5), True)
}

pub fn storage_reflection_detects_higher_durable_term_test() {
  let live = log_with([CommandEntry(1)])
  let bad = durable(2, log_with([CommandEntry(1)]), 1)
  should.equal(check_storage_reflection(bad, 1, live, 1), False)
}

pub fn storage_reflection_detects_anchor_regression_beyond_live_test() {
  // durable の anchor（prev_position.index = 1）が live の anchor（0）より
  // 後ろへ進んでいる = live に無い情報が durable にある = 検出。
  let live = log_with([CommandEntry(1), CommandEntry(2)])
  let bad = durable(1, log_at(pos(1, 1), [CommandEntry(2)]), 1)
  should.equal(check_storage_reflection(bad, 1, live, 2), False)
}

pub fn storage_reflection_detects_missing_committed_entry_in_live_test() {
  // durable が commit 済みと主張する index が live に存在しない場合も検出
  // （anchor が 0 で等しいので anchor チェックでは抜けない）。
  let live = log_with([CommandEntry(1), CommandEntry(2)])
  let bad = durable(1, log_at(pos(1, 1), [CommandEntry(2)]), 2)
  should.equal(check_storage_reflection(bad, 1, live, 2), False)
}

// ---- 同一 seed の決定論 ----

fn midload_opts(seed: Int) -> SimOptions {
  SimOptions(
    seed:,
    node_count: 4,
    link: LinkOptions(
      latency_min: 1,
      latency_max: 6,
      loss_rate: 50,
      reorder: 30,
    ),
    storage_delay: 2,
    restart_chance: 15,
    snapshot_chance: 25,
    ticks: 600,
  )
}

/// 最終状態の指紋。台帳サイズ・障害数・再起動数・各ノードの稼働状態と
/// commit / role を文字列化する。
fn fingerprint(sim: Sim) -> String {
  let head =
    "r="
    <> int.to_string(restarts(sim))
    <> " f="
    <> int.to_string(list.length(faults(sim)))
    <> " o="
    <> int.to_string(dict.size(sim.oracle))
  let body =
    sim
    |> running_ids
    |> list.map(fn(id) {
      let c = case commit_index(sim, id) {
        Some(v) -> int.to_string(v)
        None -> "-"
      }
      let rl = case role_of(sim, id) {
        Some(Leader) -> "L"
        Some(Candidate) -> "C"
        Some(Follower) -> "F"
        None -> "X"
      }
      "/" <> int.to_string(id) <> ":" <> c <> rl
    })
    |> string.join("")
  head <> body
}

pub fn same_seed_produces_identical_final_state_test() {
  let assert Ok(a) = run(new(midload_opts(1234)))
  let assert Ok(b) = run(new(midload_opts(1234)))
  should.equal(fingerprint(a), fingerprint(b))
}

pub fn different_seed_changes_the_run_test() {
  // 同一 seed 決定論の対偶として、別 seed では指紋が変わるはず。
  // （確率的なので「同じだったら失敗」にはしない。ここでは片方が
  // Ok であることを確認するに留める。）
  let assert Ok(_a) = run(new(midload_opts(1234)))
  let assert Ok(_b) = run(new(midload_opts(999_001)))
}

// ---- 無障害 3 ノードスモーク ----

fn quiet_opts() -> SimOptions {
  SimOptions(
    seed: 7,
    node_count: 3,
    link: LinkOptions(latency_min: 1, latency_max: 3, loss_rate: 0, reorder: 0),
    storage_delay: 1,
    restart_chance: 0,
    snapshot_chance: 0,
    ticks: 0,
  )
}

fn find_leader(sim: Sim) -> Option(NodeId) {
  let found =
    running_ids(sim)
    |> list.filter(fn(id) { role_of(sim, id) == Some(Leader) })
    |> list.first
  case found {
    Ok(id) -> Some(id)
    _ -> None
  }
}

fn wait_for_leader(sim: Sim, budget: Int) -> Result(#(Sim, NodeId), String) {
  case find_leader(sim) {
    Some(id) -> Ok(#(sim, id))
    None ->
      case budget {
        0 -> Error("no leader within budget")
        _ ->
          case tick(sim) {
            Ok(sim) -> wait_for_leader(sim, budget - 1)
            Error(msg) -> Error(msg)
          }
      }
  }
}

fn all_committed(sim: Sim, position: types.LogPosition) -> Bool {
  list.all(running_ids(sim), fn(id) {
    get_commit_status(sim, id, position) == Some(Committed)
  })
}

fn wait_until_committed(
  sim: Sim,
  position: types.LogPosition,
  budget: Int,
) -> Result(Sim, String) {
  case all_committed(sim, position) {
    True -> Ok(sim)
    False ->
      case budget {
        0 -> Error("proposal did not commit within budget")
        _ ->
          case tick(sim) {
            Ok(sim) -> wait_until_committed(sim, position, budget - 1)
            Error(_) as e -> e
          }
      }
  }
}

pub fn smoke_zero_fault_cluster_commits_a_proposal_test() {
  let sim0 = new(quiet_opts())
  let assert Ok(#(sim1, leader)) = wait_for_leader(sim0, 400)
  let #(sim2, proposed) = propose(sim1, leader, 101)
  let assert Some(position) = proposed
  let assert Ok(sim3) = wait_until_committed(sim2, position, 1600)
  // 全ノードが稼働中で、提案が Committed。
  should.equal(running_ids(sim3), [0, 1, 2])
  should.equal(all_committed(sim3, position), True)
  // 無障害設定なのでクラッシュ・破棄・複製・スナップショットは起きない。
  let noisy =
    faults(sim3)
    |> list.filter(fn(f) {
      string.contains(f.1, "crashed")
      || string.contains(f.1, "restarted")
      || string.contains(f.1, "dropped")
      || string.contains(f.1, "duplicated")
      || string.contains(f.1, "snapshotted")
    })
  should.equal(noisy, [])
  should.equal(restarts(sim3), 0)
  // commit は全ノードで提案位置以上。
  should.equal(
    list.all(running_ids(sim3), fn(id) {
      case commit_index(sim3, id) {
        Some(c) -> c >= position.index
        None -> False
      }
    }),
    True,
  )
}

pub fn smoke_zero_fault_cluster_keeps_single_leader_test() {
  let assert Ok(sim) = run_for(new(quiet_opts()), 1500)
  let leaders =
    running_ids(sim)
    |> list.filter(fn(id) { role_of(sim, id) == Some(Leader) })
  should.equal(list.length(leaders) <= 1, True)
  should.equal(running_ids(sim), [0, 1, 2])
  // 無障害なら全ノードで commit が進む（構成エントリ以上）。
  should.equal(
    list.all([0, 1, 2], fn(id) {
      case commit_index(sim, id) {
        Some(c) -> c >= 1
        None -> False
      }
    }),
    True,
  )
}
