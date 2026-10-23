// クラスタのプロパティテスト 7 本。gleam-raft のシミュレータ
// （support/simulation.gleam）の上で動かす。共通方針:
//
//   - 乱数: support/rng.gleam の決定論 rng を使う。各ケースの決定事象は
//     ケース専用 rng（seed_for_case(base, i)）から引き、シミュレータ内部の
//     sim.rng とは分離する（決定論の二本立て）。
//   - シード: 環境変数 GLEAM_RAFT_PBT_SEED（既定 900_001）。ケース数は
//     GLEAM_RAFT_PBT_CASES（正整数のときのみ有効、既定は下表）。
//   - ケース数: CI 実行時間に収まる既定値とする（stable 16 /
//     unstable 6 / pipelining 6 / membership 2 / restarts 6 / divergent 6 /
//     safety 6）。
//   - クラッシュ/再起動: restarts は窓クラッシュを注入する
//     （set_running_ticks、node 0 だけ running/stopping 800..=5000）。他の
//     テストはクラッシュなしで、safety にのみ毎 tick の restart_chance 抽選を
//     追加した。divergent テストの隔離は pause/resume（live 状態保持のまま
//     tick を止める。理由は support/simulation.gleam の注記参照）。
//   - 創出ノード: 最大 id 固定（support/simulation.gleam の new 仕様。
//     id_list 先頭）。
//   - budget: 10_000 〜 1_000_000、wait_until_terminal rounds 20_000。
//     Erlang を主対象とするため、JS の尾部ケースの実時間増は許容する。
//   - 失敗メッセージのノード状態ダンプ（membership のタイムアウト）は省略
//     する。シードは 10 進で表記する。

import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import gleam/string
import gleam_raft/cluster_config.{type ClusterConfig, ClusterConfig}
import gleam_raft/internal/node.{
  commit_index, config, current_term, get_commit_status,
}
import gleam_raft/log.{
  type Log, CommandEntry, ConfigEntry, entries_iter_with_positions,
  log_snapshot_position,
}
import gleam_raft/support/pbt_env
import gleam_raft/support/rng.{type Rng}
import gleam_raft/support/simulation as sim
import gleam_raft/types.{
  type CommitStatus, type LogPosition, type NodeId, Committed, InProgress,
  Leader, Rejected, Unknown,
}

// 環境変数の読み取り（GLEAM_RAFT_PBT_SEED / GLEAM_RAFT_PBT_CASES）は
// support/pbt_env.gleam に共通化している（M5 手順5）。

// ---- 共通ヘルパ ----

/// 失敗メッセージ用の position 表記。
fn show_position(p: LogPosition) -> String {
  "term=" <> int.to_string(p.term) <> " index=" <> int.to_string(p.index)
}

fn status_name(status: CommitStatus) -> String {
  case status {
    InProgress -> "InProgress"
    Committed -> "Committed"
    Rejected -> "Rejected"
    Unknown -> "Unknown"
  }
}

/// 偽なら msg を添えて Error を返す。M6 手順6 で Result 返却へ移行し、
/// 失敗が run_cases の "case i seed=s: " 接頭辞経路に乗るようにした
/// （以前の直接 panic は接頭辞を迂回していた）。
fn require(cond: Bool, msg: String) -> Result(Nil, String) {
  case cond {
    True -> Ok(Nil)
    False -> Error(msg)
  }
}

/// per-case ループの外にあるカバレッジ要求用の assert。ケース単位の
/// 接頭辞は不要で、メッセージ側に seed を含むため panic のまま据える。
fn assert_coverage(cond: Bool, msg: String) -> Nil {
  case cond {
    True -> Nil
    False -> panic as msg
  }
}

/// 0..n-1 の昇順リスト（stdlib 1.0.5 に list.range がないため自作）。
fn upto(n: Int) -> List(Int) {
  upto_loop(n, [])
}

fn upto_loop(k: Int, acc: List(Int)) -> List(Int) {
  case k {
    x if x <= 0 -> acc
    _ -> upto_loop(k - 1, [k - 1, ..acc])
  }
}

fn unwrap_proposal(
  result_option: Option(LogPosition),
) -> Result(LogPosition, String) {
  case result_option {
    Some(p) -> Ok(p)
    None -> Error("failed to propose command")
  }
}

/// ケース実行の共通 Driver。各ケースは seed_for_case(base, i) の専用シードを
/// 受け取り、失敗時は "case i seed=s: " を接頭辞して打ち切る。
fn run_cases(
  base: Int,
  count: Int,
  each: fn(Int) -> Result(Nil, String),
) -> Result(Nil, String) {
  list.try_fold(upto(count), Nil, fn(_acc: Nil, i: Int) {
    let seed = rng.seed_for_case(base, i)
    case each(seed) {
      Ok(Nil) -> Ok(Nil)
      Error(msg) ->
        Error(
          "case "
          <> int.to_string(i)
          <> " seed="
          <> int.to_string(seed)
          <> ": "
          <> msg,
        )
    }
  })
}

/// 現 leader（稼働中ノードの昇順最初）。
fn leader_of(s: sim.Sim) -> Option(NodeId) {
  case
    list.find(sim.running_ids(s), fn(id) { sim.role_of(s, id) == Some(Leader) })
  {
    Ok(l) -> Some(l)
    Error(_) -> None
  }
}

/// leader が現れるまで最大 budget tick 進める。戻り値の Option が None は
/// タイムアウト（エラーではない）。
fn run_while_leader_absent(
  s: sim.Sim,
  budget: Int,
) -> Result(#(sim.Sim, Option(NodeId)), String) {
  case leader_of(s) {
    Some(l) -> Ok(#(s, Some(l)))
    None ->
      case budget {
        0 -> Ok(#(s, None))
        _ ->
          case sim.tick(s) {
            Ok(s2) -> run_while_leader_absent(s2, budget - 1)
            Error(e) -> Error(e)
          }
      }
  }
}

/// run_while_leader_absent + タイムアウト時のエラーメッセージ化。
fn await_leader(
  s: sim.Sim,
  budget: Int,
  on_timeout: String,
) -> Result(#(sim.Sim, NodeId), String) {
  case run_while_leader_absent(s, budget) {
    Error(e) -> Error(e)
    Ok(#(_s2, None)) -> Error(on_timeout)
    Ok(#(s2, Some(l))) -> Ok(#(s2, l))
  }
}

/// 述語が成立するまで最大 budget tick 進める。
fn run_until(
  s: sim.Sim,
  budget: Int,
  pred: fn(sim.Sim) -> Bool,
) -> Result(#(sim.Sim, Bool), String) {
  case pred(s) {
    True -> Ok(#(s, True))
    False ->
      case budget {
        0 -> Ok(#(s, False))
        _ ->
          case sim.tick(s) {
            Ok(s2) -> run_until(s2, budget - 1, pred)
            Error(e) -> Error(e)
          }
      }
  }
}

/// 登録済み全ノードの commit_index が一致する。停止ノードも含めて raw
/// 状態を参照する。
fn all_commits_equal(s: sim.Sim) -> Bool {
  case sim.node_of(s, 0) {
    None -> False
    Some(first) ->
      list.all(sim.all_ids(s), fn(id) {
        case sim.node_of(s, id) {
          Some(n) -> commit_index(n) == commit_index(first)
          None -> False
        }
      })
  }
}

/// leader を探して position の terminal 状況を尋ね、InProgress なら 10 tick
/// 進めて最大 rounds 回繰り返す。leader 不在継続は None = 非 terminal と
/// して返す。
fn wait_until_terminal(
  s: sim.Sim,
  position: LogPosition,
  rounds: Int,
) -> Result(#(sim.Sim, Option(CommitStatus)), String) {
  case rounds {
    0 -> Ok(#(s, None))
    _ ->
      case run_while_leader_absent(s, 100_000) {
        Error(e) -> Error(e)
        Ok(#(s1, None)) -> Ok(#(s1, None))
        Ok(#(s1, Some(l))) ->
          case sim.get_commit_status(s1, l, position) {
            Some(st) if st != InProgress -> Ok(#(s1, Some(st)))
            _ ->
              case sim.run_for(s1, 10) {
                Ok(s2) -> wait_until_terminal(s2, position, rounds - 1)
                Error(e) -> Error(e)
              }
          }
      }
  }
}

/// 全 proposal が terminal になるまで待ち、Committed 数を数える。ラベルは
/// バッチ内番号。
fn assert_all_terminal(
  s: sim.Sim,
  proposals: List(#(String, LogPosition)),
  allow_rejected: Bool,
  allow_unknown: Bool,
) -> Result(#(sim.Sim, Int), String) {
  list.try_fold(proposals, #(s, 0), fn(acc: #(sim.Sim, Int), item) {
    let #(s1, committed) = acc
    let #(label, position) = item
    case wait_until_terminal(s1, position, 1000) {
      Error(e) -> Error(e)
      Ok(#(_s2, None)) ->
        Error("proposal " <> label <> " did not reach a terminal status")
      Ok(#(s2, Some(Committed))) -> Ok(#(s2, committed + 1))
      Ok(#(s2, Some(Rejected))) if allow_rejected -> Ok(#(s2, committed))
      Ok(#(s2, Some(Unknown))) if allow_unknown -> Ok(#(s2, committed))
      Ok(#(_s2, Some(st))) ->
        Error(
          "proposal "
          <> label
          <> " ended with unexpected status "
          <> status_name(st),
        )
    }
  })
}

// ---- プロポーザルループ（3 種類の leader 探索方針） ----

/// leader 在席のみ確認して提案し、毎回 rand(1..=10) tick 進める。
fn propose_present(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  n: Int,
  cmd: Int,
  acc: List(#(String, LogPosition)),
  when_absent: String,
) -> Result(#(sim.Sim, List(#(String, LogPosition)), Rng), String) {
  case i >= n {
    True -> Ok(#(s, acc, rnd))
    False ->
      case leader_of(s) {
        None -> Error(when_absent)
        Some(l) -> {
          let #(s1, proposal) = sim.propose(s, l, cmd)
          use position <- result.try(unwrap_proposal(proposal))
          let #(rnd2, ticks) = rng.rand_in(rnd, 1, 10)
          case sim.run_for(s1, ticks) {
            Ok(s2) ->
              propose_present(
                s2,
                rnd2,
                i + 1,
                n,
                cmd + 1,
                list.append(acc, [#(int.to_string(i), position)]),
                when_absent,
              )
            Error(e) -> Error(e)
          }
        }
      }
  }
}

/// leader を最大 budget tick 待ってから提案し、毎回 rand(1..=10) tick
/// 進める。
fn propose_with_wait(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  n: Int,
  cmd: Int,
  acc: List(#(String, LogPosition)),
  budget: Int,
) -> Result(#(sim.Sim, List(#(String, LogPosition)), Rng), String) {
  case i >= n {
    True -> Ok(#(s, acc, rnd))
    False ->
      case run_while_leader_absent(s, budget) {
        Error(e) -> Error(e)
        Ok(#(_s1, None)) -> Error("leader absent while proposing")
        Ok(#(s1, Some(l))) -> {
          let #(s2, proposal) = sim.propose(s1, l, cmd)
          use position <- result.try(unwrap_proposal(proposal))
          let #(rnd2, ticks) = rng.rand_in(rnd, 1, 10)
          case sim.run_for(s2, ticks) {
            Ok(s3) ->
              propose_with_wait(
                s3,
                rnd2,
                i + 1,
                n,
                cmd + 1,
                list.append(acc, [#(int.to_string(i), position)]),
                budget,
              )
            Error(e) -> Error(e)
          }
        }
      }
  }
}

/// tick を挟まずに連続提案する（隔離直前のパイプライン提案。tick が進ま
/// ないので leader は変わらない）。
fn propose_no_ticks(
  s: sim.Sim,
  i: Int,
  n: Int,
  cmd: Int,
  acc: List(#(String, LogPosition)),
) -> Result(#(sim.Sim, List(#(String, LogPosition))), String) {
  case i >= n {
    True -> Ok(#(s, acc))
    False ->
      case leader_of(s) {
        None -> Error("leader absent while proposing")
        Some(l) -> {
          let #(s1, proposal) = sim.propose(s, l, cmd)
          use position <- result.try(unwrap_proposal(proposal))
          propose_no_ticks(
            s1,
            i + 1,
            n,
            cmd + 1,
            list.append(acc, [#(int.to_string(i), position)]),
          )
        }
      }
  }
}

// ---- テスト 1: 安定リンクでのコミット ----
//
// 既定リンクから loss だけ 0 に落とす。既定 16 ケース。

fn stable_case(seed: Int) -> Result(Nil, String) {
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: sim.LinkOptions(
        latency_min: 5,
        latency_max: 20,
        loss_rate: 0,
        reorder: 0,
      ),
      storage_delay: 0,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use #(s1, _leader) <- result.try(await_leader(
    s0,
    10_000,
    "cluster creation timed out",
  ))
  let r0 = rng.from_seed(seed)
  let #(r1, n) = rng.rand_in(r0, 1, 32)
  use #(s2, proposals, _r2) <- result.try(propose_present(
    s1,
    r1,
    0,
    n,
    1,
    [],
    "leader disappeared",
  ))
  use #(s3, committed) <- result.try(assert_all_terminal(
    s2,
    proposals,
    False,
    False,
  ))
  use _ <- result.try(require(committed == n, "all proposals must commit"))
  use #(s4, synced) <- result.try(run_until(s3, 1000, all_commits_equal))
  use _ <- result.try(require(synced, "commit indices are not synchronized"))
  case sim.node_of(s4, 0) {
    None -> Error("node 0 disappeared")
    Some(n0) ->
      require(
        current_term(n0) == 1,
        "leader must not change under stable links",
      )
  }
}

/// 安定リンク: 3 ノードで 1〜32 件の提案が全件コミットし、最終的に全ノード
/// の commit index が一致し、leader 交代（term 上昇）が起きないこと。
pub fn proposals_commit_with_stable_links_test() -> Nil {
  let seed = pbt_env.seed()
  case run_cases(seed, pbt_env.cases(16), stable_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 2: 不安定リンクでのコミット ----
//
// loss 300‰・latency 1..=1000。既定 6 ケース。

fn unstable_case(seed: Int) -> Result(Nil, String) {
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: sim.LinkOptions(
        latency_min: 1,
        latency_max: 1000,
        loss_rate: 300,
        reorder: 0,
      ),
      storage_delay: 0,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use #(s1, _leader) <- result.try(await_leader(
    s0,
    100_000,
    "cluster creation timed out",
  ))
  let r0 = rng.from_seed(seed)
  let #(r1, n) = rng.rand_in(r0, 1, 16)
  use #(s2, proposals, _r2) <- result.try(propose_with_wait(
    s1,
    r1,
    0,
    n,
    1,
    [],
    100_000,
  ))
  use #(s3, committed) <- result.try(assert_all_terminal(
    s2,
    proposals,
    False,
    False,
  ))
  use _ <- result.try(require(committed == n, "all proposals must commit"))
  use #(_s4, synced) <- result.try(run_until(s3, 100_000, all_commits_equal))
  use _ <- result.try(require(synced, "commit indices are not synchronized"))
  Ok(Nil)
}

/// 不安定リンク（高損失・高遅延）でも提案が全件コミットし、commit index
/// が全ノードで一致すること。
pub fn proposals_commit_with_unstable_links_test() -> Nil {
  let seed = pbt_env.seed()
  case run_cases(seed, pbt_env.cases(6), unstable_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 3: パイプラインと明示 heartbeat ----
//
// 既定リンク loss 10‰、バッチ先頭 2 件は創出 leader へ tick なし連続提案、
// 5 分率 4 でパイプライン継続、半分は提案直後に heartbeat。既定 6 ケース。

fn pipelining_case(seed: Int) -> Result(Nil, String) {
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: sim.LinkOptions(
        latency_min: 5,
        latency_max: 20,
        loss_rate: 10,
        reorder: 0,
      ),
      storage_delay: 0,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use #(s1, leader0) <- result.try(await_leader(
    s0,
    10_000,
    "cluster creation timed out",
  ))
  // 創出 leader への先頭 2 件: 再引き直しも tick を挟みもしない。ノード
  // 状態の更新は進むが、tick は進めない。
  let #(s2, first) = sim.propose(s1, leader0, 1)
  let #(s3, second) = sim.propose(s2, leader0, 2)
  use p1 <- result.try(unwrap_proposal(first))
  use p2 <- result.try(unwrap_proposal(second))
  let r0 = rng.from_seed(seed)
  let #(r1, n) = rng.rand_in(r0, 2, 32)
  use #(s4, all, _r2) <- result.try(pipelining_loop(s3, r1, 2, n, [p1, p2], 3))
  // acc は先頭 2 件を既に含んだままループが n 件分積み上がるので、ここで
  // 初めて通し番号を振る（二重計上 prevented）。
  let proposals = list.map2(upto(n), all, fn(i, p) { #(int.to_string(i), p) })
  use #(s5, committed) <- result.try(assert_all_terminal(
    s4,
    proposals,
    False,
    False,
  ))
  use _ <- result.try(require(committed == n, "all proposals must commit"))
  use #(_s6, synced) <- result.try(run_until(s5, 10_000, all_commits_equal))
  use _ <- result.try(require(synced, "commit indices are not synchronized"))
  Ok(Nil)
}

fn pipelining_loop(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  n: Int,
  acc: List(LogPosition),
  cmd: Int,
) -> Result(#(sim.Sim, List(LogPosition), Rng), String) {
  case i >= n {
    True -> Ok(#(s, acc, rnd))
    False -> {
      let #(r1, pipeline) = rng.rand_ratio(rnd, 4, 5)
      let #(r2, do_heartbeat) = rng.rand_bool(r1)
      case run_while_leader_absent(s, 10_000) {
        Error(e) -> Error(e)
        Ok(#(_s1, None)) -> Error("leader absent while proposing")
        Ok(#(s1, Some(l))) -> {
          let #(s2, proposal) = sim.propose(s1, l, cmd)
          use position <- result.try(unwrap_proposal(proposal))
          use s3 <- result.try(case do_heartbeat {
            True ->
              case sim.heartbeat(s2, l) {
                #(s3, True) -> Ok(s3)
                #(_s3, False) -> Error("heartbeat must succeed on the leader")
              }
            False -> Ok(s2)
          })
          use s4 <- result.try(case pipeline {
            True -> Ok(s3)
            False -> {
              let #(_r3, ticks) = rng.rand_in(r2, 0, 5)
              case sim.run_for(s3, ticks) {
                Ok(s4) -> Ok(s4)
                Error(e) -> Error(e)
              }
            }
          })
          pipelining_loop(
            s4,
            r2,
            i + 1,
            n,
            list.append(acc, [position]),
            cmd + 1,
          )
        }
      }
    }
  }
}

/// パイプライン提案と明示 heartbeat を混ぜても全件コミットし、commit
/// index が収束すること。
pub fn proposals_commit_with_pipelining_test() -> Nil {
  let seed = pbt_env.seed()
  case run_cases(seed, pbt_env.cases(6), pipelining_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 4: クラッシュと再起動を跨ぐコミット ----
//
// 結成直後（new 直後）に node 0 だけへ set_running_ticks で
// running/stopping 800..=5000 の窓クラッシュを注入する。uptime ≥ 800 が
// 保証されるので起動中の停電は起きず、停止中も健全な voter 2 台が leader を
// 維持する。既定 6 ケース。
// （経緯: 以前は全ノード restart_chance 1〜2‰ の毎 tick 抽選で模倣したが、
// node 0 が起動直のストレージ保留窓で停電し得るため構成が 2 ノードから
// 恒久喪失し、コミットが止まるライブロックに成り得た。窓モデルはこれを
// 構造的に許さない。seed=900050 で実測。）

fn restarts_case(seed: Int) -> Result(Nil, String) {
  let r0 = rng.from_seed(seed)
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: sim.LinkOptions(
        latency_min: 5,
        latency_max: 20,
        loss_rate: 10,
        reorder: 0,
      ),
      storage_delay: 1,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s_raw = sim.new(opts)
  // 窓クラッシュを node 0 のみに有効化（他の 2 台は restart_chance 0
  // なので終始クラッシュしない）。
  let s0 = sim.set_running_ticks(s_raw, 0, 800, 5000, 800, 5000)
  use #(s1, _leader) <- result.try(await_leader(
    s0,
    10_000,
    "cluster creation timed out",
  ))
  let #(r2, n1) = rng.rand_in(r0, 1, 16)
  use #(s2, before, r3) <- result.try(propose_with_wait(
    s1,
    r2,
    0,
    n1,
    1,
    [],
    10_000,
  ))
  // いずれかのノードが 1 回以上再起動するまで待つ。
  use #(s3, restarted) <- result.try(
    run_until(s2, 50_000, fn(s) { sim.restarts(s) > 0 }),
  )
  use _ <- result.try(require(restarted, "no node restarted within the budget"))
  let #(r4, n2) = rng.rand_in(r3, 8, 48)
  use #(s4, after, _r5) <- result.try(propose_with_wait(
    s3,
    r4,
    0,
    n2,
    n1 + 1,
    [],
    10_000,
  ))
  use #(s5, _before_committed) <- result.try(assert_all_terminal(
    s4,
    before,
    True,
    False,
  ))
  use #(s6, after_committed) <- result.try(assert_all_terminal(
    s5,
    after,
    True,
    False,
  ))
  use _ <- result.try(require(
    after_committed > 0,
    "no post-restart proposal committed",
  ))
  use #(_s7, synced) <- result.try(run_until(s6, 50_000, all_commits_equal))
  use _ <- result.try(require(synced, "commit indices are not synchronized"))
  Ok(Nil)
}

/// ノードが繰り返しクラッシュ・再起動する中でも、再起動後に少なくとも 1
/// 件の提案がコミットし、最終的に commit index が全ノードで一致すること。
pub fn proposals_commit_across_node_restarts_test() -> Nil {
  let seed = pbt_env.seed()
  case run_cases(seed, pbt_env.cases(6), restarts_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 5: 分岐ログの収束 ----
//
// leader は pause/resume で隔離する（live 状態を保ったまま tick を止める）。
// pause 前にバッチ 2 の AE がスケジュール済みなので未知状態ではなく却下と
// して着地する。既定 6 ケース。

fn divergent_case(seed: Int) -> Result(Nil, String) {
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: sim.LinkOptions(
        latency_min: 5,
        latency_max: 20,
        loss_rate: 10,
        reorder: 0,
      ),
      storage_delay: 0,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use #(s1, _leader) <- result.try(await_leader(
    s0,
    10_000,
    "cluster creation timed out",
  ))
  // 初期構成・term エントリを届かせる。
  use s2 <- result.try(sim.run_for(s1, 100))
  let r0 = rng.from_seed(seed)
  let #(r1, n1) = rng.rand_in(r0, 4, 16)
  use #(s3, batch1, r2) <- result.try(propose_with_wait(
    s2,
    r1,
    0,
    n1,
    1,
    [],
    50_000,
  ))
  let #(r3, n2) = rng.rand_in(r2, 4, 16)
  use #(s4, batch2) <- result.try(propose_no_ticks(s3, 0, n2, n1 + 1, []))
  // 現 leader を一時停止して分離する。
  case leader_of(s4) {
    None -> Error("leader disappeared during isolation")
    Some(old_leader) -> {
      let s5 = sim.pause_node(s4, old_leader)
      use #(s6, _new_leader) <- result.try(await_leader(
        s5,
        50_000,
        "new leader was not elected",
      ))
      let #(r4, n3) = rng.rand_in(r3, 4, 16)
      use #(s7, batch3, _r5) <- result.try(propose_with_wait(
        s6,
        r4,
        0,
        n3,
        n1 + n2 + 1,
        [],
        50_000,
      ))
      let s8 = sim.resume_node(s7, old_leader)
      use #(s9, _c1) <- result.try(assert_all_terminal(s8, batch1, True, False))
      use #(s10, _c2) <- result.try(assert_all_terminal(s9, batch2, True, False))
      use #(s11, c3) <- result.try(assert_all_terminal(s10, batch3, True, False))
      use _ <- result.try(require(
        c3 > 0,
        "the newly elected leader committed no proposal",
      ))
      use #(_s12, synced) <- result.try(run_until(
        s11,
        10_000,
        all_commits_equal,
      ))
      use _ <- result.try(require(synced, "commit indices are not synchronized"))
      Ok(Nil)
    }
  }
}

/// 旧 leader を分離して新 leader が選出されても、全 proposal が terminal
/// に着地し（旧 leader 発の未複製エントリは Rejected 可）、新 leader 発の
/// 提案が少なくとも 1 件コミットし、commit index が収束すること。
pub fn divergent_logs_reconcile_test() -> Nil {
  let seed = pbt_env.seed()
  case run_cases(seed, pbt_env.cases(6), divergent_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 6: メンバ変更 ----
//
// loss 300‰・latency 1..=1000 の不安定リンクで 1〜4 回の voter/non-voter
// 追加・削除とコマンド提案を行う。既定 2 ケース。settle/収束 budget は
// 1_000_000、wait_until_terminal rounds は 20_000。タイムアウト時のノード
// 状態ダンプは省略する。

/// 不安定リンク（テスト 6 の既定）。
fn unstable_link() -> sim.LinkOptions {
  sim.LinkOptions(latency_min: 1, latency_max: 1000, loss_rate: 300, reorder: 0)
}

/// リンク回復設定（drop 0・latency 1..=10）。
fn healed_link() -> sim.LinkOptions {
  sim.LinkOptions(latency_min: 1, latency_max: 10, loss_rate: 0, reorder: 0)
}

/// expected.voters に属する観測ノードの position 状況一覧（raw 状態参照）。
fn voter_statuses(
  s: sim.Sim,
  position: LogPosition,
  expected: ClusterConfig,
) -> List(CommitStatus) {
  sim.all_ids(s)
  |> list.filter(fn(id) { set.contains(expected.voters, id) })
  |> list.filter_map(fn(id) {
    case sim.node_of(s, id) {
      Some(n) -> Ok(get_commit_status(n, position))
      None -> Error(Nil)
    }
  })
}

/// 観測ノードの position に対する最初の非 InProgress 状況。
fn observed_config_status(
  s: sim.Sim,
  position: LogPosition,
  expected: ClusterConfig,
) -> Option(CommitStatus) {
  case
    list.find(voter_statuses(s, position, expected), fn(st) { st != InProgress })
  {
    Ok(st) -> Some(st)
    Error(_) -> None
  }
}

/// 観測ノード間で terminal 状況が食い違っていないか。
fn has_conflicting_status(
  s: sim.Sim,
  position: LogPosition,
  expected: ClusterConfig,
  status: CommitStatus,
) -> Bool {
  voter_statuses(s, position, expected)
  |> list.any(fn(st) { st != InProgress && st != status })
}

/// expected 構成のログ上の位置（スナップショット境界または最後の Config
/// エントリ）。
fn expected_position(
  lg: Log(Int),
  expected: ClusterConfig,
) -> Option(LogPosition) {
  let markers =
    entries_iter_with_positions(lg.entries)
    |> list.filter_map(fn(item) {
      case item.1 {
        ConfigEntry(c) if c == expected -> Ok(item.0)
        _ -> Error(Nil)
      }
    })
  case list.last(markers) {
    Ok(p) -> Some(p)
    Error(_) ->
      case lg.snapshot_config == expected {
        True -> Some(log_snapshot_position(lg))
        False -> None
      }
  }
}

/// 補正済み expected 構成が全ノードで確定したか。構成マーカーの position
/// が同一ノードで Committed であることも含む。
fn config_is_committed(s: sim.Sim, expected: ClusterConfig) -> Bool {
  !cluster_config.is_joint_consensus(expected)
  && list.all(cluster_config.unique_nodes(expected), fn(id) {
    case sim.node_of(s, id) {
      None -> False
      Some(n) ->
        config(n) == expected
        && case expected_position(node.log(n), expected) {
          None -> False
          Some(p) -> get_commit_status(n, p) == Committed
        }
    }
  })
}

fn settled(s: sim.Sim) -> Bool {
  case leader_of(s) {
    Some(l) ->
      case sim.node_of(s, l) {
        Some(n) -> !cluster_config.is_joint_consensus(config(n))
        None -> False
      }
    None -> False
  }
}

/// voter 追加・削除の joint consensus 構成と補正済み expected 構成を作る。
/// changed が偽ならエラー。
fn joint_with_expected(
  cfg: ClusterConfig,
  adding: List(NodeId),
  removing: List(NodeId),
  label: String,
) -> Result(#(ClusterConfig, ClusterConfig), String) {
  let #(joint, changed) =
    cluster_config.to_joint_consensus(cfg, adding, removing)
  case changed {
    True ->
      Ok(#(
        joint,
        ClusterConfig(
          voters: joint.new_voters,
          new_voters: set.new(),
          non_voters: joint.non_voters,
        ),
      ))
    False ->
      Error("config change " <> label <> " could not build a joint config")
  }
}

/// プロポーザル種別: (更新後シミュレータ, 変更提案か, 乱数状態)。
type RollOutcome {
  RollOutcome(
    sim: sim.Sim,
    change: Option(#(LogPosition, ClusterConfig, Option(NodeId))),
    rnd: Rng,
  )
}

/// 0..9 の抽選で 7 未満なら追加、それ以外で削除を提案する（追加 7/10、
/// 削除 3/10）。追加は 1/2 の確率で voter。
fn roll_change(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  label: String,
) -> Result(RollOutcome, String) {
  let #(r0, roll) = rng.rand_in(rnd, 0, 9)
  case roll < 7 {
    True -> add_change(s, r0, i, label)
    False -> remove_change(s, r0, i, label)
  }
}

fn add_change(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  label: String,
) -> Result(RollOutcome, String) {
  let new_id = 3 + i
  // 提案前にノードを登録する。
  let s1 = sim.add_node(s, new_id)
  let #(r1, is_voter) = rng.rand_ratio(rnd, 1, 2)
  case leader_of(s1) {
    None -> Error("leader absent while proposing")
    Some(l) ->
      case sim.node_of(s1, l) {
        None -> Error("leader disappeared")
        Some(ln) -> {
          let cfg = config(ln)
          let built = case is_voter {
            True -> joint_with_expected(cfg, [new_id], [], label)
            False -> {
              let new_config =
                ClusterConfig(
                  voters: cfg.voters,
                  new_voters: cfg.new_voters,
                  non_voters: set.insert(cfg.non_voters, new_id),
                )
              Ok(#(new_config, new_config))
            }
          }
          use #(new_config, expected) <- result.try(built)
          let #(s2, outcome) = sim.propose_config(s1, l, new_config)
          case outcome {
            Error(_) -> Error("failed to propose config change " <> label)
            Ok(position) ->
              Ok(RollOutcome(s2, Some(#(position, expected, Some(new_id))), r1))
          }
        }
      }
  }
}

fn remove_change(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  label: String,
) -> Result(RollOutcome, String) {
  let _ = i
  case leader_of(s) {
    None -> Error("leader absent while proposing")
    Some(l) ->
      case sim.node_of(s, l) {
        None -> Error("leader disappeared")
        Some(ln) -> {
          let cfg = config(ln)
          // 候補 = 全 non_voter +（voter が 3 人以上なら）全 voter。空なら
          // 変更なしでサイレントにスキップ。
          let candidates =
            list.append(
              list.map(set.to_list(cfg.non_voters), fn(id) { #(id, False) }),
              case set.size(cfg.voters) > 2 {
                True ->
                  list.map(set.to_list(cfg.voters), fn(id) { #(id, True) })
                False -> []
              },
            )
          case candidates {
            [] -> Ok(RollOutcome(s, None, rnd))
            _ -> {
              let len = list.length(candidates)
              let #(r1, pick) = rng.rand_in(rnd, 0, len - 1)
              let assert Ok(#(target, is_voter)) = case
                list.drop(candidates, pick)
              {
                [c, ..] -> Ok(c)
                [] -> Error(Nil)
              }
              let built = case is_voter {
                True -> joint_with_expected(cfg, [], [target], label)
                False -> {
                  let new_config =
                    ClusterConfig(
                      voters: cfg.voters,
                      new_voters: cfg.new_voters,
                      non_voters: set.delete(cfg.non_voters, target),
                    )
                  Ok(#(new_config, new_config))
                }
              }
              use #(new_config, expected) <- result.try(built)
              let #(s1, outcome) = sim.propose_config(s, l, new_config)
              case outcome {
                Error(_) -> Error("failed to propose config change " <> label)
                Ok(position) ->
                  Ok(RollOutcome(s1, Some(#(position, expected, None)), r1))
              }
            }
          }
        }
      }
  }
}

/// メンバ変更 1 ラウンド: settle 待ち → 追加または削除の提案 → 不安定期
/// 経過 → 回復して terminal 状況観測 → 確定構成の確認と台帳整理 →
/// コマンド提案。acc = (確定コマンド数, 確定メンバ変更数)。
fn membership_round(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  rounds: Int,
  acc: #(Int, Int),
) -> Result(#(Int, Int), String) {
  case i >= rounds {
    True -> Ok(acc)
    False -> {
      let label = int.to_string(i)
      use #(s1, ok) <- result.try(run_until(s, 1_000_000, settled))
      use _ <- result.try(require(
        ok,
        "config change " <> label <> " did not settle",
      ))
      use outcome <- result.try(roll_change(s1, rnd, i, label))
      case outcome.change {
        None -> propose_commands(outcome.sim, outcome.rnd, acc)
        Some(change) -> {
          let #(position, expected, added) = change
          // 不安定期を過ごしてからリンクを回復する。
          let #(r2, unstable_ticks) = rng.rand_in(outcome.rnd, 1, 10_000)
          use s2 <- result.try(sim.run_for(outcome.sim, unstable_ticks))
          let s3 = sim.set_link(s2, healed_link())
          use #(s4, terminal) <- result.try(
            run_until(s3, 1_000_000, fn(sx) {
              observed_config_status(sx, position, expected) != None
            }),
          )
          case terminal {
            False ->
              Error(
                "config change " <> label <> " did not reach a terminal status",
              )
            True ->
              case observed_config_status(s4, position, expected) {
                None -> Error("config change " <> label <> " status vanished")
                Some(status) -> {
                  use _ <- result.try(
                    case
                      has_conflicting_status(s4, position, expected, status)
                    {
                      True ->
                        Error(
                          "config change "
                          <> label
                          <> " has conflicting terminal statuses at "
                          <> show_position(position),
                        )
                      False -> Ok(Nil)
                    },
                  )
                  case status {
                    Committed -> {
                      use #(s5, finalized) <- result.try(
                        run_until(s4, 1_000_000, fn(sx) {
                          config_is_committed(sx, expected)
                        }),
                      )
                      use _ <- result.try(require(
                        finalized,
                        "committed config change "
                          <> label
                          <> " did not produce the expected final config",
                      ))
                      // 確定構成から外れたノードは除籍する。
                      let s6 =
                        list.fold(sim.all_ids(s5), s5, fn(sy, id) {
                          case cluster_config.contains(expected, id) {
                            True -> sy
                            False -> sim.remove_node(sy, id)
                          }
                        })
                      restore_and_continue(s6, r2, #(acc.0, acc.1 + 1))
                    }
                    Rejected -> {
                      // 却下された追加ノードは未参加なので除籍する。
                      let s5 = case added {
                        Some(a) -> sim.remove_node(s4, a)
                        None -> s4
                      }
                      restore_and_continue(s5, r2, acc)
                    }
                    other ->
                      Error(
                        "config change "
                        <> label
                        <> " ended with unexpected status "
                        <> status_name(other),
                      )
                  }
                }
              }
          }
        }
      }
    }
  }
}

/// リンクを不安定設定へ戻してコマンド提案フェーズへ。
fn restore_and_continue(
  s: sim.Sim,
  rnd: Rng,
  acc: #(Int, Int),
) -> Result(#(Int, Int), String) {
  let s1 = sim.set_link(s, unstable_link())
  propose_commands(s1, rnd, acc)
}

/// コマンド提案フェーズ。構成変更が InProgress 残りになり得るため
/// Committed 数のみ数える（rounds は 20_000）。
fn propose_commands(
  s: sim.Sim,
  rnd: Rng,
  acc: #(Int, Int),
) -> Result(#(Int, Int), String) {
  let #(r1, count) = rng.rand_in(rnd, 1, 4)
  use #(s1, positions, _r2) <- result.try(member_command_loop(
    s,
    r1,
    0,
    count,
    [],
    acc.0 + 1,
  ))
  use #(_s2, acc2) <- result.try(
    list.try_fold(
      positions,
      #(s1, acc),
      fn(pair: #(sim.Sim, #(Int, Int)), position: LogPosition) {
        let #(sx, #(committed, changes)) = pair
        case wait_until_terminal(sx, position, 20_000) {
          Error(e) -> Error(e)
          Ok(#(sy, Some(Committed))) -> Ok(#(sy, #(committed + 1, changes)))
          Ok(#(sy, _)) -> Ok(#(sy, #(committed, changes)))
        }
      },
    ),
  )
  Ok(acc2)
}

fn member_command_loop(
  s: sim.Sim,
  rnd: Rng,
  i: Int,
  n: Int,
  acc: List(LogPosition),
  cmd: Int,
) -> Result(#(sim.Sim, List(LogPosition), Rng), String) {
  case i >= n {
    True -> Ok(#(s, acc, rnd))
    False ->
      case run_while_leader_absent(s, 1_000_000) {
        Error(e) -> Error(e)
        Ok(#(_s1, None)) -> Error("leader absent while proposing")
        Ok(#(s1, Some(l))) -> {
          let #(s2, proposal) = sim.propose(s1, l, cmd)
          use position <- result.try(unwrap_proposal(proposal))
          let #(r2, ticks) = rng.rand_in(rnd, 1, 10)
          case sim.run_for(s2, ticks) {
            Ok(s3) ->
              member_command_loop(
                s3,
                r2,
                i + 1,
                n,
                list.append(acc, [position]),
                cmd + 1,
              )
            Error(e) -> Error(e)
          }
        }
      }
  }
}

fn membership_case(seed: Int) -> Result(#(Int, Int), String) {
  let opts =
    sim.SimOptions(
      seed:,
      node_count: 3,
      link: unstable_link(),
      storage_delay: 0,
      restart_chance: 0,
      snapshot_chance: 0,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use #(s1, _leader) <- result.try(await_leader(
    s0,
    100_000,
    "cluster creation timed out",
  ))
  let r0 = rng.from_seed(seed)
  let #(r1, rounds) = rng.rand_in(r0, 1, 4)
  use #(total_committed, total_changes) <- result.try(
    membership_round(s1, r1, 0, rounds, #(0, 0)),
  )
  use _ <- result.try(require(
    total_committed > 0,
    "no proposal committed; the cluster made no progress",
  ))
  Ok(#(total_committed, total_changes))
}

/// 不安定リンク下での voter/non-voter 追加・削除が joint consensus に
/// 留まらず収束し、少なくとも 1 つの構成変更が確定し、少なくとも 1 つの
/// コマンドがコミットすること。
pub fn membership_changes_settle_test() -> Nil {
  let seed = pbt_env.seed()
  let count = pbt_env.cases(2)
  let summed =
    list.try_fold(upto(count), #(0, 0), fn(acc: #(Int, Int), i: Int) {
      let case_seed = rng.seed_for_case(seed, i)
      case membership_case(case_seed) {
        Ok(#(committed, changes)) -> Ok(#(acc.0 + committed, acc.1 + changes))
        Error(msg) ->
          Error(
            "case "
            <> int.to_string(i)
            <> " seed="
            <> int.to_string(case_seed)
            <> ": "
            <> msg,
          )
      }
    })
  case summed {
    Ok(#(commit_sum, change_sum)) -> {
      assert_coverage(
        change_sum > 0,
        "no case exercised a config change (seed=" <> int.to_string(seed) <> ")",
      )
      assert_coverage(
        commit_sum > 0,
        "no case committed a command under membership changes (seed="
          <> int.to_string(seed)
          <> ")",
      )
      Nil
    }
    Error(msg) -> panic as msg
  }
}

// ---- テスト 7: ランダム妨害下での安全性 ----
//
// 安全性はシミュレータの毎 tick 検証器で見る。カバレッジラベルは観測
// 可能な 8 種（crash/restart/duplicate/drop は faults ログ、local snapshot
// は raw ログ境界、remote install・multi-term・committed command は
// カウンタ/台帳で観測）。既定 6 ケース。

/// 1 ケースで観測されたカバレッジ旗印。
type SafetyFlags {
  SafetyFlags(
    committed_command: Bool,
    crash: Bool,
    restart: Bool,
    duplicate: Bool,
    drop: Bool,
    multi_term: Bool,
    local_snapshot: Bool,
    remote_install: Bool,
  )
}

fn flags_or(a: SafetyFlags, b: SafetyFlags) -> SafetyFlags {
  SafetyFlags(
    committed_command: a.committed_command || b.committed_command,
    crash: a.crash || b.crash,
    restart: a.restart || b.restart,
    duplicate: a.duplicate || b.duplicate,
    drop: a.drop || b.drop,
    multi_term: a.multi_term || b.multi_term,
    local_snapshot: a.local_snapshot || b.local_snapshot,
    remote_install: a.remote_install || b.remote_install,
  )
}

fn any_fault(s: sim.Sim, needle: String) -> Bool {
  list.any(sim.faults(s), fn(f) { string.contains(f.1, needle) })
}

fn has_committed_command(s: sim.Sim) -> Bool {
  list.any(dict.values(s.oracle), fn(item) {
    case item.1 {
      CommandEntry(_) -> True
      _ -> False
    }
  })
}

fn has_local_snapshot(s: sim.Sim) -> Bool {
  list.any(sim.all_ids(s), fn(id) {
    case sim.node_of(s, id) {
      Some(n) -> log_snapshot_position(node.log(n)).index > 0
      None -> False
    }
  })
}

fn safety_case(seed: Int) -> Result(SafetyFlags, String) {
  let r0 = rng.from_seed(seed)
  let #(r1, node_count) = rng.rand_in(r0, 3, 5)
  let opts =
    sim.SimOptions(
      seed:,
      node_count:,
      link: sim.LinkOptions(
        latency_min: 5,
        latency_max: 20,
        loss_rate: 50,
        reorder: 100,
      ),
      storage_delay: 2,
      restart_chance: 2,
      snapshot_chance: 40,
      ticks: 0,
    )
  let s0 = sim.new(opts)
  use s1 <- result.try(safety_drive(s0, r1, 2500, 1))
  Ok(SafetyFlags(
    committed_command: has_committed_command(s1),
    crash: any_fault(s1, " crashed"),
    restart: sim.restarts(s1) > 0,
    duplicate: any_fault(s1, "duplicated to"),
    drop: any_fault(s1, "dropped to"),
    multi_term: sim.leader_term_count(s1) >= 2,
    local_snapshot: has_local_snapshot(s1),
    remote_install: sim.snapshots_installed(s1) > 0,
  ))
}

/// 残り ticks を rand(1..=10) チャンクで進め、各チャンク後に現 leader へ
/// 1 件提案する。leader 不在は許容。
fn safety_drive(
  s: sim.Sim,
  rnd: Rng,
  remaining: Int,
  cmd: Int,
) -> Result(sim.Sim, String) {
  case remaining <= 0 {
    True -> Ok(s)
    False -> {
      let #(r1, chunk) = rng.rand_in(rnd, 1, 10)
      use s1 <- result.try(sim.run_for(s, chunk))
      let s2 = case leader_of(s1) {
        Some(l) -> sim.propose(s1, l, cmd).0
        None -> s1
      }
      safety_drive(s2, r1, remaining - chunk, cmd + 1)
    }
  }
}

/// 高確率妨害（クラッシュ・破棄・複製・スナップショット・ストレージ遅延）
/// を受ける 2500 tick のどこでも 4 種の検証器（選挙安全性・状態機械安全
/// ・commit 単調性・ストレージ反映）が破れないこと。加えてカバレッジ:
/// 少なくとも 1 ケースが各妨害事象を実際に通っていること。
pub fn cluster_invariants_hold_test() -> Nil {
  let seed = pbt_env.seed()
  let count = pbt_env.cases(6)
  let base =
    SafetyFlags(
      committed_command: False,
      crash: False,
      restart: False,
      duplicate: False,
      drop: False,
      multi_term: False,
      local_snapshot: False,
      remote_install: False,
    )
  let covered =
    list.try_fold(upto(count), base, fn(acc: SafetyFlags, i: Int) {
      let case_seed = rng.seed_for_case(seed, i)
      case safety_case(case_seed) {
        Ok(flags) -> Ok(flags_or(acc, flags))
        Error(msg) ->
          Error(
            "case "
            <> int.to_string(i)
            <> " seed="
            <> int.to_string(case_seed)
            <> ": "
            <> msg,
          )
      }
    })
  case covered {
    Error(msg) -> panic as msg
    Ok(f) -> {
      let note = " (seed=" <> int.to_string(seed) <> ")"
      let checks = [
        #(f.committed_command, "no case exercised a committed command"),
        #(f.crash, "no case exercised an effective crash"),
        #(f.restart, "no case exercised an effective restart"),
        #(f.duplicate, "no case exercised duplicate message delivery"),
        #(f.drop, "no case exercised a dropped message"),
        #(f.multi_term, "no case exercised leaders in multiple terms"),
        #(f.local_snapshot, "no case exercised a local snapshot"),
        #(
          f.remote_install,
          "no case exercised a remote snapshot install that succeeded",
        ),
      ]
      // 環境変数で seed/ケース数を絞っている実行は失敗再現用のデバッグ
      // 実行なので、カバレッジ要求はスキップする（絞り込みでは特定の
      // 妨害事象を必ずしも通せない）。デフォルト実行では要求する。
      let _ = case pbt_env.debug_override() {
        True -> Nil
        False ->
          list.each(checks, fn(check) {
            assert_coverage(check.0, check.1 <> note)
          })
      }
      Nil
    }
  }
}
