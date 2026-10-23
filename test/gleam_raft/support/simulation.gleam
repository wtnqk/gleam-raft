// DESIGN §11.3 のシミュレーションカーネル。仕様が法であり、実装側の決め
// 打ちを以下に注記する。確率の目盛りは千分率で統一した（1/100 → 10、
// 3/10 → 300）。性質から検証器へのマッピングの詳細は docs/TESTPLAN.md。
//
// - 1 tick の進め方（tick）。ノード走査順は決定論のため昇順の id リストで
//   回す（gleam の dict fold 順に依存しない）。
// - 配送遅延は link.latency_min/max から抽样し、AE のみ entries_len 倍、
//   他は 1 倍する。
// - クラッシュは二様式。set_running_ticks が稼働窓そのもので（restart を
//   挟むケースで使う）、未設定ノードは毎 tick のトス restart_chance（停止長
//   は定数 restart_delay_ticks）。
// - InstallSnapshot の配送遅延は仕様どおり定数 1000 tick に固定。
// - 選挙タイムアウトの役割別テーブルは Leader → min、Candidate → sample、
//   Follower → max。
// - 検証器は DESIGN §11.3 の 4 系統。各 check_* は細工破損を検出する自己
//   テストのため純粋関数として公開する。
// - 検証器 4（storage 反映）は「durable の commit 済み範囲は live と一致、
//   未 commit 末尾は live 側の truncation で消えうるため比較しない」。
//   実 disk も未 commit エントリを持つのでこれは正しさの範囲と一致する。
// - 「現 leader をクラスタから物理的に外して後で live 状態ごと戻す」操作は、
//   paused フラグによる一時停止（resume で live 状態を保ったまま復帰）で
//   表す。停止中の宛先メッセージは失わず inbox に保持して復帰後に配送する
//   （復帰後の収束が速くなるだけの差）。
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

import gleam_raft/action.{
  type Action, AppendLogEntries, BroadcastMessage, InstallSnapshot,
  SaveCurrentTerm, SaveVotedFor, SendMessage, SetElectionTimeout,
}
import gleam_raft/cluster_config.{type ClusterConfig}
import gleam_raft/internal/node.{type Node}
import gleam_raft/log.{
  type Log, type LogEntry, entries_iter_with_positions, entries_len,
  log_get_position_and_config, log_snapshot_position,
}
import gleam_raft/message.{type Message, AppendEntriesCall}
import gleam_raft/support/rng.{type Rng}
import gleam_raft/types.{
  type CommitStatus, type LogIndex, type LogPosition, type NodeId, type Role,
  type Term, Candidate, Follower, Leader,
}

// ---- 固定定数（DESIGN §11.3 で仕様化した値。SimOptions には出さない） ----

/// 選挙タイムアウトの下限 tick。Leader はこの長さで待つ。
pub const election_timeout_min: Int = 100

/// 選挙タイムアウトの上限 tick。Follower はこの長さで待つ。
pub const election_timeout_max: Int = 1000

/// InstallSnapshot の配送遅延（tick）。仕様どおり 1000 に固定する。
pub const snapshot_install_delay_ticks: Int = 1000

/// クラッシュからの再起動までの待ち tick。停止長は固定値に単純化した。
/// 稼働窓を有効化したノードは使わず、窓クラッシュ時に down_window から
/// 抽样する。
pub const restart_delay_ticks: Int = 200

// ---- 生成オプション（DESIGN §11.3 の型そのもの） ----

/// メッセージリンクの挙動。loss_rate と reorder は千分率。
pub type LinkOptions {
  LinkOptions(
    /// 配送遅延の下限（tick）。AE は entries_len 倍、その他は 1 倍される。
    latency_min: Int,
    /// 配送遅延の上限（tick）。
    latency_max: Int,
    /// 送出時の破棄確率（千分率）。
    loss_rate: Int,
    /// 到着時の複製確率（千分率）。届いたメッセージを 1 本複写して新しい
    /// 遅延で再投入する。
    reorder: Int,
  )
}

/// 1 ケースのシミュレーション設定。全乱数事象は seed から決定づける。
pub type SimOptions {
  SimOptions(
    seed: Int,
    node_count: Int,
    link: LinkOptions,
    /// AppendLogEntries / Save* の完了遅延（1 単位 × 件数）。0 でも最低
    /// 1 tick はかかる。
    storage_delay: Int,
    /// 毎 tick のクラッシュ確率（千分率）。
    restart_chance: Int,
    /// 毎 tick の leader ローカル・スナップショット導入確率（千分率）。
    snapshot_chance: Int,
    /// 進める tick 数。
    ticks: Int,
  )
}

// ---- 永続化・ノード状態 ----

/// 永続ストレージが保持する状態。restart の入力と commit 済み長さ。
/// `committed` は fold 時点の live commit で、検証器 4 の比較範囲を決める。
pub type DurableState {
  DurableState(
    term: Term,
    voted_for: Option(NodeId),
    log: Log(Int),
    committed: LogIndex,
  )
}

/// ストレージ書き込み中の未確定バッチ。完了 tick で durable へ原子的に
/// fold する。クラッシュ時は捨てられる。
pub type PendingStorage {
  PendingStorage(
    finish_at: Int,
    durable: DurableState,
    /// ストレージ確定まで保留した送信。
    held: List(#(NodeId, Message(Int))),
  )
}

/// 受信中の InstallSnapshot。1 ノードにつき 1 本だけ持つ。
pub type SnapshotInstall {
  SnapshotInstall(finish_at: Int, position: LogPosition, config: ClusterConfig)
}

/// ノード単位の内部状態。`node` は live 状態で、クラッシュ後も参照だけ
/// 残る（再起動時に durable から再構築して上書きする）。`Sim` の公開
/// フィールドになるため型自体は公開している。
pub type SimNode {
  SimNode(
    node: Node(Int),
    running: Bool,
    /// クラスタから一時的に外している（dual の stop/pause）。tick ループは
    /// 全処理をスキップし、宛先メッセージは inbox に積む。復帰時は live
    /// 状態を保ったまま。stop_node との使い分けは stop_node の注記参照。
    paused: Bool,
    /// 再起動予定 tick（稼働中は 0）。
    start_at: Int,
    /// 稼働窓 (min, max)。#(0, 0) は無効で、そのノードは毎 tick の
    /// restart_chance 抽選に従う。有効時は uptime が最低 min tick 保証され、
    /// 起動直後のストレージ書き込み窓での停電が構造上起きない。
    run_window: #(Int, Int),
    /// 窓クラッシュ時に引く停止期間 (min, max)。
    down_window: #(Int, Int),
    /// 稼働中の稼働窓終了 tick（None = 未抽样または無効）。
    stop_at: Option(Int),
    durable: DurableState,
    pending: Option(PendingStorage),
    timeout_at: Option(Int),
    install: Option(SnapshotInstall),
    /// 生存期間（tenure）内の commit 単調性検査ベースライン。再起動時に
    /// durable 由来の commit へリセットする。
    commit_base: Int,
    inbox: List(#(Int, Message(Int))),
  )
}

/// クラスタ全体のシミュレーション状態。
pub type Sim {
  Sim(
    now: Int,
    opts: SimOptions,
    rng: Rng,
    nodes: Dict(NodeId, SimNode),
    /// 項 → その項で leader を名乗ったノード（検証器 1 の台帳）。
    leaders: Dict(Term, NodeId),
    /// index → (position, entry) の確定済み観測台帳（検証器 2）。
    oracle: Dict(LogIndex, #(LogPosition, LogEntry(Int))),
    restarts: Int,
    /// 遠隔 InstallSnapshot が受理された回数（カバレッジ集計用）。
    installs_applied: Int,
    faults: List(#(Int, String)),
  )
}

// ---- 公開 API ----

/// 設定からクラスタを作る。id_list 先頭（最大 id。3 nodes なら node 2）が
/// 全ノードを voter に create_cluster し、
/// 残りは空構成で待機する。
/// `node_count >= 1` が前条件。
pub fn new(opts: SimOptions) -> Sim {
  let ids = id_list(opts.node_count)
  let base =
    Sim(
      now: 0,
      opts: opts,
      rng: rng.from_seed(opts.seed),
      nodes: dict.new(),
      leaders: dict.new(),
      oracle: dict.new(),
      restarts: 0,
      installs_applied: 0,
      faults: [],
    )
  let base =
    list.fold(ids, base, fn(sim, id) { set_node(sim, id, boot_node(id)) })
  let assert Ok(first) = list.first(ids)
  let #(n0, created) = node.create_cluster(get_node(base, first).node, ids)
  let assert Ok(_) = created
  after_actions(base, first, n0)
}

/// opts.ticks だけ進める。
pub fn run(sim: Sim) -> Result(Sim, String) {
  run_for(sim, sim.opts.ticks)
}

/// 残り ticks を進める。検証器が破損を検出したら Error で seed と失敗
/// tick を含むメッセージを返す（DESIGN §11.3「再現可能にする」）。
pub fn run_for(sim: Sim, ticks: Int) -> Result(Sim, String) {
  case ticks {
    t if t <= 0 -> Ok(sim)
    _ ->
      case tick(sim) {
        Ok(sim) -> run_for(sim, ticks - 1)
        Error(_) as e -> e
      }
  }
}

/// 1 tick 進める。全ノード処理後に 4 検証器を走らせる。
pub fn tick(sim: Sim) -> Result(Sim, String) {
  let sim = list.fold(node_ids(sim), sim, tick_node)
  case verify_tick(sim) {
    Error(msg) -> Error(fail_msg(sim, msg))
    Ok(sim) -> Ok(Sim(..sim, now: sim.now + 1))
  }
}

/// 稼働中のノードでコマンドを提案する。leader でなければ None。提案直後
/// に Action を通常の dispatch 経路で処理して、キューをその場で流す。
pub fn propose(
  sim: Sim,
  id: NodeId,
  command: Int,
) -> #(Sim, Option(LogPosition)) {
  case running_node(sim, id) {
    None -> #(sim, None)
    Some(n) -> {
      let #(n, proposed) = node.propose_command(n, command)
      // ストレージ未確定中は drain しない（pending が二重になると
      // fold 前状態と held を失う）。Action は node に溜め、pending
      // 完了後の次 tick で流れる。
      let sn = get_node(sim, id)
      case sn.pending {
        Some(_) -> #(set_node(sim, id, SimNode(..sn, node: n)), proposed)
        None -> #(after_actions(sim, id, n), proposed)
      }
    }
  }
}

/// 稼働中ノードの commit 状況。停止中なら None。
pub fn get_commit_status(
  sim: Sim,
  id: NodeId,
  position: LogPosition,
) -> Option(CommitStatus) {
  case running_node(sim, id) {
    None -> None
    Some(n) -> Some(node.get_commit_status(n, position))
  }
}

/// 稼働中ノードの commit_index。停止中なら None。
pub fn commit_index(sim: Sim, id: NodeId) -> Option(LogIndex) {
  case running_node(sim, id) {
    None -> None
    Some(n) -> Some(node.commit_index(n))
  }
}

/// 稼働中ノードの role。停止中なら None。
pub fn role_of(sim: Sim, id: NodeId) -> Option(Role) {
  case running_node(sim, id) {
    None -> None
    Some(n) -> Some(node.role(n))
  }
}

/// ノードが稼働中か（登録済み全ノードで定義される。停止・一時停止は偽）。
pub fn is_running(sim: Sim, id: NodeId) -> Bool {
  case dict.get(sim.nodes, id) {
    Ok(sn) -> sn.running && !sn.paused
    Error(_) -> False
  }
}

/// 稼働中の全ノード id（昇順）。
pub fn running_ids(sim: Sim) -> List(NodeId) {
  list.filter(node_ids(sim), fn(id) { is_running(sim, id) })
}

/// 観測された障害・特記事項の履歴（時系列、`(tick, 説明)`）。
pub fn faults(sim: Sim) -> List(#(Int, String)) {
  list.reverse(sim.faults)
}

/// 再起動の総数。
pub fn restarts(sim: Sim) -> Int {
  sim.restarts
}

/// 遠隔 InstallSnapshot の受理総数（カバレッジ集計用）。
pub fn snapshots_installed(sim: Sim) -> Int {
  sim.installs_applied
}

/// 登録済みノードの live 状態をそのまま返す（停止・一時停止後も参照可）。
/// オーラクル検査（config scp や log 走査）に使う。未登録 id は None。
pub fn node_of(sim: Sim, id: NodeId) -> Option(Node(Int)) {
  case dict.get(sim.nodes, id) {
    Ok(sn) -> Some(sn.node)
    Error(_) -> None
  }
}

/// 登録済みの全ノード id（昇順。停止・一時停止を含む）。
pub fn all_ids(sim: Sim) -> List(NodeId) {
  node_ids(sim)
}

/// 稼働中ノードでクラスタ構成変更を提案する。`propose` と同じ drain 規約。
/// 前条件違反や非稼働・非 leader は `Error` を返し、その場合 state は
/// 不変。
pub fn propose_config(
  sim: Sim,
  id: NodeId,
  config: ClusterConfig,
) -> #(Sim, Result(LogPosition, types.ProposeConfigError)) {
  case running_node(sim, id) {
    None -> #(sim, Error(types.NotLeader))
    Some(n) -> {
      let #(n, outcome) = node.propose_config(n, config)
      case outcome {
        Error(_) -> #(sim, outcome)
        Ok(_) -> {
          let sn = get_node(sim, id)
          case sn.pending {
            Some(_) -> #(set_node(sim, id, SimNode(..sn, node: n)), outcome)
            None -> #(after_actions(sim, id, n), outcome)
          }
        }
      }
    }
  }
}

/// 稼働中ノードで能動的ハートビートを 1 回打つ。core の返す Bool を
/// そのまま返す（leader なら必ず true）。drain 規約は `propose` と同じ。
pub fn heartbeat(sim: Sim, id: NodeId) -> #(Sim, Bool) {
  case running_node(sim, id) {
    None -> #(sim, False)
    Some(n) -> {
      let #(n, ok) = node.heartbeat(n)
      let sn = get_node(sim, id)
      case sn.pending {
        Some(_) -> #(set_node(sim, id, SimNode(..sn, node: n)), ok)
        None -> #(after_actions(sim, id, n), ok)
      }
    }
  }
}

/// 空構成のノードを 1 台クラスタに追加する（既存なら何もしない）。
/// 追加直後は誰も voter に含まないため、leader 側の構成提案で参加する。
pub fn add_node(sim: Sim, id: NodeId) -> Sim {
  case dict.has_key(sim.nodes, id) {
    True -> sim
    False -> set_node(sim, id, boot_node(id))
  }
}

/// ノードをクラスタから完全除籍する（台帳・inbox 含む全状態を捨てる）。
/// 構成の commit 後に退役させる場合に使う。
pub fn remove_node(sim: Sim, id: NodeId) -> Sim {
  Sim(..sim, nodes: dict.delete(sim.nodes, id))
}

/// ノードをクラスタから一時的に外す（tick 対象から除外。live 状態と
/// inbox は保持）。「leader を live 状態ごとクラスタから外す」操作の写像。
/// 未登録なら何もしない。
pub fn pause_node(sim: Sim, id: NodeId) -> Sim {
  case dict.get(sim.nodes, id) {
    Error(_) -> sim
    Ok(sn) ->
      set_node(
        record_fault(
          sim,
          "node " <> int.to_string(id) <> " removed from cluster",
        ),
        id,
        SimNode(..sn, paused: True),
      )
  }
}

/// 一時停止したノードを live 状態のまま復帰させる（durable からの再構築は
/// しない）。一時停止中でないなら何もしない。
pub fn resume_node(sim: Sim, id: NodeId) -> Sim {
  case dict.get(sim.nodes, id) {
    Error(_) -> sim
    Ok(sn) ->
      case sn.paused {
        False -> sim
        True ->
          set_node(
            record_fault(
              sim,
              "node " <> int.to_string(id) <> " rejoined cluster",
            ),
            id,
            SimNode(..sn, paused: False),
          )
      }
  }
}

/// 実行中にリンク設定を差し替える。収束を待つ間に損失率を 0 へ戻すように
/// 使う。
pub fn set_link(sim: Sim, link: LinkOptions) -> Sim {
  Sim(..sim, opts: SimOptions(..sim.opts, link:))
}

/// 単一ノードに窓クラッシュを有効化する。稼働窓と停止窓をそれぞれ
/// (min, max) で与える。既定は無効で、restart を挟むケースでは node 0
/// だけに 800..=5000 の窓を置く。有効化したノードは全局 restart_chance
/// 抽選の対象外。0 <= min <= max が前条件で、max = 0 は無効化を意味する。
pub fn set_running_ticks(
  sim: Sim,
  id: NodeId,
  run_min: Int,
  run_max: Int,
  down_min: Int,
  down_max: Int,
) -> Sim {
  let sn = get_node(sim, id)
  set_node(
    sim,
    id,
    SimNode(
      ..sn,
      run_window: #(run_min, run_max),
      down_window: #(down_min, down_max),
      stop_at: None,
    ),
  )
}

/// leader を輩出した項の数（カバレッジ集計用。2 以上ならリーダー交代が
/// 起きたことになる）。
pub fn leader_term_count(sim: Sim) -> Int {
  dict.size(sim.leaders)
}

// ---- 検証器（DESIGN §11.3 の 4 系統。純粋関数として公開し、細工破損を
// 検出する自己テストの材料にする） ----

/// 検証器 1: election safety。同じ項で leader を名乗るノードは高々 1。
/// 台帳 `leaders` に対して候補 `(term, id)` が矛盾しないか。
pub fn check_election_safety(
  leaders: Dict(Term, NodeId),
  candidate: #(Term, NodeId),
) -> Bool {
  let #(term, id) = candidate
  case dict.get(leaders, term) {
    Ok(other) -> other == id
    Error(_) -> True
  }
}

/// 検証器 2: state machine safety。確定済み台帳 `oracle` に対して
/// `observations`（index → (position, entry)）が一致するか。台帳に無い
/// index の観測は許容（台帳への追加は呼び出し側の責務）。
pub fn check_state_machine_safety(
  oracle: Dict(LogIndex, #(LogPosition, LogEntry(Int))),
  observations: List(#(LogIndex, #(LogPosition, LogEntry(Int)))),
) -> Bool {
  list.all(observations, fn(obs) {
    let #(index, entry) = obs
    case dict.get(oracle, index) {
      Ok(expected) -> expected == entry
      Error(_) -> True
    }
  })
}

/// 検証器 3: commit 単調性。生存期間（tenure）内で commit_index は減少
/// しない。再起動時に呼び出し側がベースラインを durable 由来の commit へ
/// リセットする（DESIGN §11.3 の実装上の解釈。docs に注記済み）。
pub fn check_commit_monotonicity(prev: LogIndex, cur: LogIndex) -> Bool {
  cur >= prev
}

/// 検証器 4: storage 反映。durable.term ≤ live.term、durable の anchor は
/// live の anchor より後ろへ進まない、そして durable の commit 済み範囲
/// （`durable.committed` と `live_commit` の min まで）は live と完全一致。
/// 未 commit 末尾は live 側の truncation で消えうるため比較しない。
pub fn check_storage_reflection(
  durable: DurableState,
  live_term: Term,
  live: Log(Int),
  live_commit: LogIndex,
) -> Bool {
  let compare_upto = int.min(durable.committed, live_commit)
  let live_at =
    dict.from_list(
      list.map(entries_iter_with_positions(live.entries), fn(p_e) {
        #(p_e.0.index, p_e)
      }),
    )
  durable.term <= live_term
  && durable.log.entries.prev_position.index <= live.entries.prev_position.index
  && list.all(entries_iter_with_positions(durable.log.entries), fn(p_e) {
    case p_e.0.index <= compare_upto {
      True ->
        case dict.get(live_at, p_e.0.index) {
          Ok(found) -> found == p_e
          Error(_) -> False
        }
      False -> True
    }
  })
}

// ---- ノード 1 台の tick 処理 ----

fn tick_node(sim: Sim, id: NodeId) -> Sim {
  let sn0 = get_node(sim, id)
  case sn0.paused {
    True -> sim
    False -> tick_paused_free(sim, sn0, id)
  }
}

fn tick_paused_free(sim: Sim, sn0: SimNode, id: NodeId) -> Sim {
  let sim = case sn0.running, sn0.start_at <= sim.now {
    False, True -> restart_node(sim, id)
    _, _ -> sim
  }
  let sn = get_node(sim, id)
  case sn.running {
    False -> sim
    True ->
      case crash_step(sim, id, sn) {
        #(s1, Some(crashed)) -> set_node(s1, id, crashed)
        #(s1, None) -> tick_running(s1, id)
      }
  }
}

/// クラッシュ判定の入口。稼働窓を有効化したノードは窓のサイクル（毎 tick
/// の停止時刻抽样 → 満了で停電 → 停止期間を抽样して再起動）、それ以外の
/// ノードは全局の毎 tick restart_chance 抽選（roll_crash）に従う。
fn crash_step(sim: Sim, id: NodeId, sn: SimNode) -> #(Sim, Option(SimNode)) {
  case sn.run_window {
    #(_, 0) -> roll_crash(sim, id, sn)
    #(min, max) ->
      case sn.stop_at {
        None -> {
          let #(rng, up) = rng.rand_in(sim.rng, min, max)
          let sim = Sim(..sim, rng:)
          #(set_node(sim, id, SimNode(..sn, stop_at: Some(sim.now + up))), None)
        }
        Some(stop_at) if stop_at <= sim.now -> window_crash(sim, id, sn)
        Some(_) -> #(sim, None)
      }
  }
}

/// 稼働窓満了によるクラッシュ。停電の割り切り（pending / held / timeout /
/// install 破棄、inbox 保持）は roll_crash と同じ。停止期間を抽样して
/// 再起動予定を置き、stop_at を未抽样へ戻す（再抽样は再起動後の最初の稼働
/// tick で行う）。
fn window_crash(sim: Sim, id: NodeId, sn: SimNode) -> #(Sim, Option(SimNode)) {
  let #(rng, down) = rng.rand_in(sim.rng, sn.down_window.0, sn.down_window.1)
  let sim = Sim(..sim, rng:)
  let sim = record_fault(sim, "node " <> int.to_string(id) <> " crashed")
  #(
    sim,
    Some(
      SimNode(
        ..sn,
        node: sn.node,
        running: False,
        start_at: sim.now + down,
        stop_at: None,
        pending: None,
        timeout_at: None,
        install: None,
      ),
    ),
  )
}

/// クラッシュ抽選。当たったら停止状態へ（pending / held / timeout /
/// install は破棄、inbox は保持）。
/// 抽選は rng を消費し、命中時は faults にも書き込むため、更新後の sim を
/// 必ず呼び出し側へ返す（戻り値から sim を落としてはいけない）。
fn roll_crash(sim: Sim, id: NodeId, sn: SimNode) -> #(Sim, Option(SimNode)) {
  let #(rng, hit) = rng.rand_ratio(sim.rng, sim.opts.restart_chance, 1000)
  let sim = Sim(..sim, rng:)
  case hit {
    False -> #(sim, None)
    True -> {
      let sim = record_fault(sim, "node " <> int.to_string(id) <> " crashed")
      #(
        sim,
        Some(
          SimNode(
            ..sn,
            node: sn.node,
            running: False,
            start_at: sim.now + restart_delay_ticks,
            pending: None,
            timeout_at: None,
            install: None,
          ),
        ),
      )
    }
  }
}

/// durable からの再起動本体。クラッシュ自動復帰と手動復帰（revive_node）
/// で共有する。停止中に締切を過ぎたメッセージは捨て、未来配送分は保持する。
fn do_restart(sim: Sim, id: NodeId) -> Sim {
  let sn = get_node(sim, id)
  let inbox = list.filter(sn.inbox, fn(m) { m.0 >= sim.now })
  let n =
    node.restart(id, sn.durable.term, sn.durable.voted_for, sn.durable.log)
  let sn =
    SimNode(
      node: n,
      running: True,
      paused: False,
      start_at: 0,
      run_window: sn.run_window,
      down_window: sn.down_window,
      stop_at: None,
      durable: sn.durable,
      pending: None,
      timeout_at: None,
      install: None,
      commit_base: node.commit_index(n),
      inbox:,
    )
  set_node(sim, id, sn)
}

fn restart_node(sim: Sim, id: NodeId) -> Sim {
  let sim = do_restart(sim, id)
  record_fault(
    Sim(..sim, restarts: sim.restarts + 1),
    "node " <> int.to_string(id) <> " restarted from durable",
  )
}

/// 稼働中ノードの残り処理。ストレージ未確定なら確定 tick まで全面ポーズ。
fn tick_running(sim: Sim, id: NodeId) -> Sim {
  let sn = get_node(sim, id)
  case sn.pending {
    Some(pending) ->
      case pending.finish_at <= sim.now {
        False -> sim
        True -> {
          // fold + held 解放後、この tick はそれ以上進めない（完成 tick
          // もポーズ。DESIGN §11.3 の実施記録に注記）。
          let sim =
            record_fault(
              sim,
              "node "
                <> int.to_string(id)
                <> " storage committed ("
                <> int.to_string(list.length(pending.held))
                <> " held sends released)",
            )
          let sim =
            set_node(
              sim,
              id,
              SimNode(..sn, pending: None, durable: pending.durable),
            )
          list.fold(pending.held, sim, fn(sim, send) {
            schedule_send(sim, send.0, send.1)
          })
        }
      }
    None -> {
      // 選挙タイムアウト
      let sn = get_node(sim, id)
      let #(n, sn) = case sn.timeout_at {
        Some(t) if t <= sim.now -> {
          let n = node.handle_election_timeout(sn.node)
          #(n, SimNode(..sn, node: n, timeout_at: None))
        }
        _ -> #(sn.node, sn)
      }
      // InstallSnapshot 完了（1 本だけ。無効スナップショットは捨てて
      // スロットを空ける）
      let #(n, sn, sim) = case sn.install {
        Some(inst) if inst.finish_at <= sim.now -> {
          let #(n, ok) =
            node.handle_snapshot_installed(n, inst.position, inst.config)
          let sn = SimNode(..sn, node: n, install: None)
          case ok {
            True -> #(
              n,
              SimNode(..sn, durable: snap_live(n)),
              Sim(..sim, installs_applied: sim.installs_applied + 1),
            )
            False -> #(n, sn, sim)
          }
        }
        _ -> #(n, sn, sim)
      }
      // inbox 配送（期限到達分を at 昇順。reorder は到着時に複製）
      let due =
        sn.inbox
        |> list.filter(fn(m) { m.0 <= sim.now })
        |> list.sort(by: fn(a, b) { int.compare(a.0, b.0) })
      let rest = list.filter(sn.inbox, fn(m) { m.0 > sim.now })
      let sim = set_node(sim, id, SimNode(..sn, node: n, inbox: rest))
      let sim = list.fold(due, sim, fn(sim, m) { deliver(sim, id, m.1) })
      // Action drain（ここで pending が生まれると送信は held へ回る）
      after_actions(sim, id, get_node(sim, id).node)
    }
  }
}

/// inbox に届いたメッセージを 1 本 node へ渡す。reorder 抽選に当たると
/// 複写を新しい遅延で再投入する。
fn deliver(sim: Sim, id: NodeId, message: Message(Int)) -> Sim {
  let sn = get_node(sim, id)
  let n =
    node.handle_message(sn.node, message)
    |> result.unwrap(sn.node)
  let sim = set_node(sim, id, SimNode(..sn, node: n))
  let #(rng, dup) = rng.rand_ratio(sim.rng, sim.opts.link.reorder, 1000)
  let sim = Sim(..sim, rng:)
  case dup {
    False -> sim
    True -> {
      let sim =
        record_fault(
          sim,
          "duplicated to " <> int.to_string(id) <> " " <> msg_kind(message),
        )
      schedule_send(sim, id, message)
    }
  }
}

// ---- Action の dispatch ----

/// live ノードの Action 集合を drain し、ストレージ・送信・スナップショット・
/// タイムアウトへ配る。ストレージ Action が 1 つでもあれば pending を作り、
/// この tick の送信は held へ回す。
/// pending を作らなかった場合のみ送信をスケジュールし、ローカル・スナップ
/// ショット注入を試みる。
fn after_actions(sim: Sim, id: NodeId, n0: Node(Int)) -> Sim {
  let #(n, actions) = drain(n0)
  // SetElectionTimeout（pending 有無に関わらず即時反映。待ち時間は下の
  // case が役割別テーブル）
  let #(sim, timeout_at) = case
    list.any(actions, fn(a) { a == SetElectionTimeout })
  {
    False -> #(sim, None)
    True ->
      case node.role(n) {
        Leader -> #(sim, Some(sim.now + election_timeout_min))
        Follower -> #(sim, Some(sim.now + election_timeout_max))
        Candidate -> {
          let #(rng, t) =
            rng.rand_in(sim.rng, election_timeout_min, election_timeout_max)
          #(Sim(..sim, rng:), Some(sim.now + t))
        }
      }
  }
  // InstallSnapshot は受信側スロットへ予約（停止中・一杯なら放棄）。
  // 送信側のログ位置と構成をこの時点で捕捉する（payload 無し Action のため）。
  let installs = plan_installs(sim, n, actions)
  let sim = apply_installs(sim, installs)
  let sn = get_node(sim, id)
  let sn = case timeout_at {
    Some(t) -> SimNode(..sn, node: n, timeout_at: Some(t))
    None -> SimNode(..sn, node: n)
  }
  let sim = set_node(sim, id, sn)
  let sends = collect_sends(sim, id, n, actions)
  case storage_count(actions) {
    0 -> {
      let sim =
        list.fold(sends, sim, fn(sim, send) {
          schedule_send(sim, send.0, send.1)
        })
      inject_snapshot(sim, id, n)
    }
    count -> {
      let unit = int.max(sim.opts.storage_delay, 1)
      let pending =
        PendingStorage(
          finish_at: sim.now + unit * count,
          durable: snap_live(n),
          held: sends,
        )
      let sim = set_node(sim, id, SimNode(..sn, pending: Some(pending)))
      // 保存待ちがあってもスナップショット抽選は続ける（leadership を
      // 維持している leader が busy というだけで抽選を飛ばすと、
      // snapshot_chance を上げたのに境界が全く進まない）。スナップショット
      // 自体は commit 位置までなので durable より進むことはない。
      inject_snapshot(sim, id, n)
    }
  }
}

/// drain した Action を優先度順に全部取り出す。
fn drain(n: Node(Int)) -> #(Node(Int), List(Action(Int))) {
  case node.take_action(n) {
    #(n, None) -> #(n, [])
    #(n, Some(taken)) -> {
      let #(n, rest) = drain(n)
      #(n, [taken, ..rest])
    }
  }
}

/// 保存単位の数：Save* は 1、AppendLogEntries は entries_len。
fn storage_count(actions: List(Action(Int))) -> Int {
  list.fold(actions, 0, fn(acc, a) {
    case a {
      SaveCurrentTerm -> acc + 1
      SaveVotedFor -> acc + 1
      AppendLogEntries(entries:) -> acc + entries_len(entries)
      _ -> acc
    }
  })
}

/// 送信 Action を (宛先, メッセージ) に展開。broadcast は peers 全員
/// （自ノードを除く）。
fn collect_sends(
  _sim: Sim,
  id: NodeId,
  n: Node(Int),
  actions: List(Action(Int)),
) -> List(#(NodeId, Message(Int))) {
  list.flat_map(actions, fn(a) {
    case a {
      SendMessage(to:, message:) -> [#(to, message)]
      BroadcastMessage(message:) ->
        n
        |> node.peers
        |> list.filter(fn(other) { other != id })
        |> list.map(fn(other) { #(other, message) })
      _ -> []
    }
  })
}

/// InstallSnapshot を受信側スロット用の予約リストへ変換する。受信側が
/// 停止中・一時停止中または既存受信中なら捨てる。捕捉する位置・構成は
/// 送信側の現在ログ。
fn plan_installs(
  sim: Sim,
  n: Node(Int),
  actions: List(Action(Int)),
) -> List(#(NodeId, SnapshotInstall)) {
  let position = log_snapshot_position(node.log(n))
  let config = node.log(n).snapshot_config
  list.filter_map(actions, fn(a) {
    case a {
      InstallSnapshot(to:) ->
        case dict.get(sim.nodes, to) {
          Ok(target)
            if target.running && !target.paused && { target.install == None }
          ->
            Ok(#(
              to,
              SnapshotInstall(
                finish_at: sim.now + snapshot_install_delay_ticks,
                position:,
                config:,
              ),
            ))
          _ -> Error(Nil)
        }
      _ -> Error(Nil)
    }
  })
}

fn apply_installs(sim: Sim, installs: List(#(NodeId, SnapshotInstall))) -> Sim {
  list.fold(installs, sim, fn(sim, entry) {
    let #(to, inst) = entry
    let target = get_node(sim, to)
    set_node(sim, to, SimNode(..target, install: Some(inst)))
  })
}

/// live ノード状態の durable スナップショット（fold 用）。
fn snap_live(n: Node(Int)) -> DurableState {
  DurableState(
    term: node.current_term(n),
    voted_for: node.voted_for(n),
    log: node.log(n),
    committed: node.commit_index(n),
  )
}

// ---- メッセージ配送 ----

/// 1 通送る：送出時に loss 抽選 → size 倍 latency で inbox 投入。
/// 抽選は逐次 rng へ引き継ぐ。同一状態から 2 回引くと同一 rand32 出力の
/// 再利用になって、生存枝の latency が loss_rate + 1 以上へ相関する。
fn schedule_send(sim: Sim, to: NodeId, message: Message(Int)) -> Sim {
  let #(rng_after_loss, lost) =
    rng.rand_ratio(sim.rng, sim.opts.link.loss_rate, 1000)
  case lost {
    True ->
      record_fault(
        Sim(..sim, rng: rng_after_loss),
        "dropped to " <> int.to_string(to) <> " " <> msg_kind(message),
      )
    False -> {
      // メッセージサイズ規則: AppendEntriesCall は entries.len()（空 =
      // ハートビートなので 0、latency 0 = 即時）、それ以外と
      // InstallSnapshot は 1。
      let size = case message {
        AppendEntriesCall(entries:, ..) -> entries_len(entries)
        _ -> 1
      }
      let #(rng, unit) =
        rng.rand_in(
          rng_after_loss,
          sim.opts.link.latency_min,
          sim.opts.link.latency_max,
        )
      // 配送先ノードを探すのは抽選後で、居なければ黙って破棄する（抽選
      // 自体は消費済み）。remove 済み宛の in-flight メッセージはここで
      // 消える。
      case dict.get(sim.nodes, to) {
        Error(_) -> Sim(..sim, rng:)
        Ok(target) -> {
          let at = sim.now + unit * size
          set_node(
            Sim(..sim, rng:),
            to,
            SimNode(
              ..target,
              inbox: list.append(target.inbox, [#(at, message)]),
            ),
          )
        }
      }
    }
  }
}

fn msg_kind(message: Message(Int)) -> String {
  case message {
    AppendEntriesCall(..) -> "AppendEntriesCall"
    _ -> "vote-message"
  }
}

// ---- ローカル・スナップショット注入 ----

/// snapshot_chance の抽選に当たった leader が自ノードの commit 位置へ
/// スナップショットを打つ（注入後は durable も即 fold。承認済みセマンティ
/// クス）。
fn inject_snapshot(sim: Sim, id: NodeId, n: Node(Int)) -> Sim {
  let #(rng, fire) = rng.rand_ratio(sim.rng, sim.opts.snapshot_chance, 1000)
  let sim = Sim(..sim, rng:)
  let commit = node.commit_index(n)
  let boundary = log_snapshot_position(node.log(n)).index
  case fire, node.role(n) == Leader, commit > boundary {
    True, True, True ->
      case log_get_position_and_config(node.log(n), commit) {
        Some(#(position, config)) -> {
          let #(n, ok) = node.handle_snapshot_installed(n, position, config)
          case ok {
            True -> {
              let sn = get_node(sim, id)
              let sim =
                record_fault(
                  sim,
                  "node "
                    <> int.to_string(id)
                    <> " snapshotted at index "
                    <> int.to_string(position.index),
                )
              // 保存待ちバッチの durable も同じ境界へ切り揃える。ここを
              // 放っておくと完成 tick で durable がスナップショット前の
              // ログへ戻り、live に無い entry が残って storage 反映検査に
              // 引っかかる。
              let sn = case sn.pending {
                Some(p) ->
                  SimNode(
                    ..sn,
                    node: n,
                    durable: snap_live(n),
                    pending: Some(PendingStorage(..p, durable: snap_live(n))),
                  )
                None -> SimNode(..sn, node: n, durable: snap_live(n))
              }
              set_node(sim, id, sn)
            }
            False -> sim
          }
        }
        None -> sim
      }
    _, _, _ -> sim
  }
}

// ---- tick 末尾の検証（4 系統、毎 tick 全稼働ノード走査） ----

fn verify_tick(sim: Sim) -> Result(Sim, String) {
  verify_each(sim, running_ids(sim))
}

fn verify_each(sim: Sim, ids: List(NodeId)) -> Result(Sim, String) {
  case ids {
    [] -> Ok(sim)
    [id, ..rest] ->
      case verify_node(sim, id) {
        Ok(sim) -> verify_each(sim, rest)
        Error(_) as e -> e
      }
  }
}

fn verify_node(sim: Sim, id: NodeId) -> Result(Sim, String) {
  let sn = get_node(sim, id)
  let n = sn.node
  let commit = node.commit_index(n)
  // 1. election safety
  case node.role(n) {
    Leader ->
      case check_election_safety(sim.leaders, #(node.current_term(n), id)) {
        False ->
          Error(
            "election safety: term "
            <> int.to_string(node.current_term(n))
            <> " claimed by node "
            <> int.to_string(id)
            <> " and another node",
          )
        True ->
          case dict.has_key(sim.leaders, node.current_term(n)) {
            False ->
              verify_tail(
                Sim(
                  ..sim,
                  leaders: dict.insert(sim.leaders, node.current_term(n), id),
                ),
                id,
                n,
                commit,
                sn,
              )
            True -> verify_tail(sim, id, n, commit, sn)
          }
      }
    _ -> verify_tail(sim, id, n, commit, sn)
  }
}

fn verify_tail(
  sim: Sim,
  id: NodeId,
  n: Node(Int),
  commit: LogIndex,
  sn: SimNode,
) -> Result(Sim, String) {
  // 2. state machine safety（commit 済み観測を oracle と突き合わせ、新観測
  //    を台帳へ入れる）
  let observations =
    node.log(n).entries
    |> entries_iter_with_positions
    |> list.filter(fn(p_e) { p_e.0.index <= commit })
    |> list.map(fn(p_e) { #(p_e.0.index, p_e) })
  case check_state_machine_safety(sim.oracle, observations) {
    False ->
      Error(
        "state machine safety: committed entry divergence observed at node "
        <> int.to_string(id),
      )
    True -> {
      let oracle =
        list.fold(observations, sim.oracle, fn(oracle, obs) {
          dict.insert(oracle, obs.0, obs.1)
        })
      let sim = Sim(..sim, oracle:)
      // 3. commit 単調性（生存期間内）
      case check_commit_monotonicity(sn.commit_base, commit) {
        False ->
          Error(
            "commit monotonicity: node "
            <> int.to_string(id)
            <> " commit "
            <> int.to_string(sn.commit_base)
            <> " -> "
            <> int.to_string(commit),
          )
        True -> {
          let sim = set_node(sim, id, SimNode(..sn, commit_base: commit))
          // 4. storage 反映
          case
            check_storage_reflection(
              sn.durable,
              node.current_term(n),
              node.log(n),
              commit,
            )
          {
            True -> Ok(sim)
            False ->
              Error(
                "storage reflection: node "
                <> int.to_string(id)
                <> " durable committed range is not a prefix of live",
              )
          }
        }
      }
    }
  }
}

// ---- 状態アクセス・ユーティリティ ----

fn boot_node(id: NodeId) -> SimNode {
  let n = node.start(id)
  SimNode(
    node: n,
    running: True,
    paused: False,
    start_at: 0,
    run_window: #(0, 0),
    down_window: #(0, 0),
    stop_at: None,
    durable: snap_live(n),
    pending: None,
    timeout_at: None,
    install: None,
    commit_base: node.commit_index(n),
    inbox: [],
  )
}

fn get_node(sim: Sim, id: NodeId) -> SimNode {
  case dict.get(sim.nodes, id) {
    Ok(sn) -> sn
    Error(_) -> panic as "simulation: unknown node"
  }
}

fn set_node(sim: Sim, id: NodeId, sn: SimNode) -> Sim {
  Sim(..sim, nodes: dict.insert(sim.nodes, id, sn))
}

fn running_node(sim: Sim, id: NodeId) -> Option(Node(Int)) {
  case dict.get(sim.nodes, id) {
    Ok(sn) ->
      case sn.running, sn.paused {
        True, False -> Some(sn.node)
        _, _ -> None
      }
    Error(_) -> None
  }
}

/// 現クラスタの全ノード id（昇順）。add_node 系の追加にも追従する。
fn node_ids(sim: Sim) -> List(NodeId) {
  sim.nodes
  |> dict.keys
  |> list.sort(by: int.compare)
}

fn id_list(count: Int) -> List(NodeId) {
  case count {
    c if c <= 0 -> []
    _ -> [count - 1, ..id_list(count - 1)]
  }
}

fn record_fault(sim: Sim, description: String) -> Sim {
  Sim(..sim, faults: [#(sim.now, description), ..sim.faults])
}

fn fail_msg(sim: Sim, message: String) -> String {
  "seed="
  <> int.to_string(sim.opts.seed)
  <> " tick="
  <> int.to_string(sim.now)
  <> ": "
  <> message
}
