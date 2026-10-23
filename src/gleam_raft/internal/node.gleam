// ノード状態機械。sans-IO なので入力はすべて純粋な状態変換であり、実行
// すべき I/O は `Actions` に集約される（DESIGN §10）。意図的な差分は
// DESIGN §2 の D 項目のみで、このモジュールでは D1（前条件を Result で返
// す）と D2（command のジェネリック化）が効く。
import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order.{Gt}
import gleam/set

import gleam_raft/action.{type Action, type Actions}
import gleam_raft/cluster_config.{
  type ClusterConfig, ClusterConfig, config_new, unique_nodes,
}
import gleam_raft/log as raft_log
import gleam_raft/message.{
  type Message, AppendEntriesCall, AppendEntriesReply, RequestVoteCall,
  RequestVoteReply, message_from, message_handle_snapshot_installed,
  message_term,
}
import gleam_raft/types.{
  type CreateClusterError, type HandleError, type LogIndex, type LogPosition,
  type NodeId, type Role, type Term, compare_position, zero_position,
}

/// Follower の追いつき状態。leader が追う match_index。
type FollowerState {
  FollowerState(match_index: LogIndex)
}

/// role ごとに必要な状態。node.gleam の私有（DESIGN §10.1）。
type RoleState(command) {
  Follower
  Candidate(granted_votes: set.Set(NodeId))
  Leader(followers: dict.Dict(NodeId, FollowerState), solo_voter: Bool)
}

/// 異常切り分けの手がかりとして公開するカウンタ集。全カウンタを素の
/// `Int` フィールドとして持つ（D6: saturating 加算は不要）。
pub type NodeMetrics {
  NodeMetrics(
    self_messages_ignored: Int,
    term_advances_from_messages: Int,
    request_vote_calls_rejected_by_old_term: Int,
    request_vote_calls_rejected_by_log: Int,
    request_vote_calls_rejected_by_existing_vote: Int,
    request_vote_replies_ignored_from_old_terms: Int,
    request_vote_replies_ignored_while_not_candidate: Int,
    append_entries_calls_rejected_by_old_term: Int,
    same_term_append_entries_calls_received_by_leader: Int,
    log_divergences_detected_by_follower: Int,
    append_entries_replies_ignored_from_old_terms: Int,
    append_entries_replies_ignored_while_not_leader: Int,
    append_entries_replies_ignored_from_unknown_nodes: Int,
    append_entries_replies_ignored_behind_match_index: Int,
    append_entries_replies_ahead_of_leader: Int,
    log_divergences_detected_by_leader: Int,
    elections_started: Int,
    leaderships_started: Int,
  )
}

/// 全カウンタ 0 の初期値。カウンタは Node インスタンスの寿命にわたって
/// 累積し、role 遷移ではリセットされない。`start` / `restart` でノードを
/// 作り直すときだけ初期値に戻る。
pub fn node_metrics_default() -> NodeMetrics {
  NodeMetrics(
    self_messages_ignored: 0,
    term_advances_from_messages: 0,
    request_vote_calls_rejected_by_old_term: 0,
    request_vote_calls_rejected_by_log: 0,
    request_vote_calls_rejected_by_existing_vote: 0,
    request_vote_replies_ignored_from_old_terms: 0,
    request_vote_replies_ignored_while_not_candidate: 0,
    append_entries_calls_rejected_by_old_term: 0,
    same_term_append_entries_calls_received_by_leader: 0,
    log_divergences_detected_by_follower: 0,
    append_entries_replies_ignored_from_old_terms: 0,
    append_entries_replies_ignored_while_not_leader: 0,
    append_entries_replies_ignored_from_unknown_nodes: 0,
    append_entries_replies_ignored_behind_match_index: 0,
    append_entries_replies_ahead_of_leader: 0,
    log_divergences_detected_by_leader: 0,
    elections_started: 0,
    leaderships_started: 0,
  )
}

/// Raft ノード。opaque なので呼び出し側は直接組み替えられず、公開関数だ
/// けが状態を作れる（DESIGN §10.1）。
pub opaque type Node(command) {
  Node(
    id: NodeId,
    current_term: Term,
    voted_for: Option(NodeId),
    log: raft_log.Log(command),
    commit_index: LogIndex,
    role_state: RoleState(command),
    actions: Actions(command),
    metrics: NodeMetrics,
  )
}

/// 新しいノードを起動する。まだどのクラスタにも属さない。新しいクラスタ
/// を作るなら `start` のあとに `create_cluster`、既存クラスタのメンバー
/// として復旧するなら `restart` を使う。
pub fn start(id: NodeId) -> Node(command) {
  Node(
    id: id,
    current_term: 0,
    voted_for: None,
    log: raft_log.log_new(config_new(), raft_log.entries_new(zero_position)),
    commit_index: 0,
    role_state: Follower,
    actions: action.actions_empty(),
    metrics: node_metrics_default(),
  )
}

/// 永続ストレージからノードを復旧する。Raft は復旧状態の信頼性に依存する
/// ので、この前条件は API の境界で明示する。検証はストレージを所有する
/// 呼び出し側の責務:
///
/// - `current_term` / `voted_for` / `log` は、最後に永続化を要求した状態
///   から無損失で復元したものを渡す。
/// - `take_action` が束ねて返す Action 束を外向きメッセージ送信より先に、
///   同じ束のストレージ系 Action（`SaveCurrentTerm` / `SaveVotedFor` /
///   `AppendLogEntries`）の永続化を完了させる。順序契約の詳細は
///   DESIGN §7 の `actions_take`。
/// - 永続状態の損失・破損が疑われるノードはクラスタに復旧させない。
///
/// `commit_index` はスナップショット位置の index に巻き戻る。role は
/// Follower、metrics は初期値で、`SetElectionTimeout` を 1 件積む。
pub fn restart(
  id: NodeId,
  current_term: Term,
  voted_for: Option(NodeId),
  log: raft_log.Log(command),
) -> Node(command) {
  let base = start(id)
  Node(
    ..base,
    current_term: current_term,
    voted_for: voted_for,
    log: log,
    commit_index: raft_log.log_snapshot_position(log).index,
    actions: action.actions_set(
      action.actions_empty(),
      action.SetElectionTimeout,
    ),
  )
}

/// このノードの識別子。
pub fn id(node: Node(command)) -> NodeId {
  node.id
}

/// 現在の role。
pub fn role(node: Node(command)) -> Role {
  case node.role_state {
    Follower -> types.Follower
    Candidate(_) -> types.Candidate
    Leader(..) -> types.Leader
  }
}

/// 現在の項。
pub fn current_term(node: Node(command)) -> Term {
  node.current_term
}

/// 現在の項で投票した相手。未投票なら `None`。
pub fn voted_for(node: Node(command)) -> Option(NodeId) {
  node.voted_for
}

/// 保持するログ（開いた record のコピー。検査に使える）。
pub fn log(node: Node(command)) -> raft_log.Log(command) {
  node.log
}

/// 適用済みとして確定している最大のログ index。
pub fn commit_index(node: Node(command)) -> LogIndex {
  node.commit_index
}

/// ログ中の最新クラスタ設定。
pub fn config(node: Node(command)) -> ClusterConfig {
  raft_log.log_latest_config(node.log)
}

/// 設定に現れるノードから自ノードを除いた昇順リスト。
pub fn peers(node: Node(command)) -> List(NodeId) {
  node
  |> config
  |> unique_nodes
  |> list.filter(fn(other) { other != node.id })
}

/// メトリクスの読み取りコピー。
pub fn metrics(node: Node(command)) -> NodeMetrics {
  node.metrics
}

/// 未実行 Action の読み取りコピー（DESIGN §3）。
pub fn actions(node: Node(command)) -> Actions(command) {
  node.actions
}

/// 優先順位で Action を 1 件取り出す。取り出しは Node の簿記であり、実際
/// の実行（ストレージ書き込み・メッセージ送信）は呼び出し側の責務。実行
/// 前に取り出しただけで効果が失われることはなく、`actions` として観察も
/// できる（DESIGN §7）。
pub fn take_action(
  node: Node(command),
) -> #(Node(command), Option(Action(command))) {
  case action.actions_take(node.actions) {
    #(taken, actions) -> #(Node(..node, actions: actions), taken)
  }
}

/// leader がコマンドを 1 件提案する。追加分の AE broadcast とストレージ系
/// Action を積んで、追加後の末尾位置を返す。leader でなければ何も積まず
/// `None`（D1: 非 leader は sentinel 値でなく Option で表す）。
pub fn propose_command(
  node: Node(command),
  command: command,
) -> #(Node(command), Option(LogPosition)) {
  case node.role_state {
    Leader(..) -> {
      let #(node, position) = propose(node, raft_log.CommandEntry(command:))
      #(node, Some(position))
    }
    _ -> #(node, None)
  }
}

/// leader が新しいクラスタ構成を提案する。
/// 成功時は `propose_command` と同じ積み方をする。joint 構成が commit さ
/// れると `apply_commit` 経由で最終構成が自動提案され、最終構成の voters
/// に自ノードが居なければ leader は同じ項で follower へ降格する。前条件違
/// 反は原因を区別して `types.ProposeConfigError` の変種として返す（D1。
/// 判定順は固定で NotLeader → VotersChanged → OverlappingNodes →
/// JointInProgress、DESIGN §6）。
pub fn propose_config(
  node: Node(command),
  new_config: ClusterConfig,
) -> #(Node(command), Result(LogPosition, types.ProposeConfigError)) {
  case node.role_state {
    Follower | Candidate(_) -> #(node, Error(types.NotLeader))
    Leader(..) ->
      case check_config_proposal(node, new_config) {
        Error(reason) -> #(node, Error(reason))
        Ok(_) -> {
          let #(next, position) =
            propose(node, raft_log.ConfigEntry(config: new_config))
          #(next, Ok(position))
        }
      }
  }
}

/// 提案済み位置の commit 状態。runner が数値比較でなく意味で適用可否を判
/// 断するための読み取り専用観測（DESIGN §6）。判定順は固定: anchor より前
/// なら Unknown。commit_index 以下なら断片に位置が残っているかで
/// Committed / Rejected。commit を越えていて commit 位置の項が
/// position.term より大きければ Rejected（その位置は二度と
/// commit されない）、それ以外（commit 位置が日志外など）は InProgress。
pub fn get_commit_status(
  node: Node(command),
  position: LogPosition,
) -> types.CommitStatus {
  let entries = node.log.entries
  case position.index < entries.prev_position.index {
    True -> types.Unknown
    False ->
      case position.index <= node.commit_index {
        True ->
          case raft_log.entries_contains(entries, position) {
            True -> types.Committed
            False -> types.Rejected
          }
        False ->
          case raft_log.entries_get_term(entries, node.commit_index) {
            None -> types.InProgress
            Some(term) ->
              case position.term < term {
                True -> types.Rejected
                False -> types.InProgress
              }
          }
      }
  }
}

/// leader が指定ノードについて確認済みの追いつき位置。自ノードが leader
/// でないか、相手を知りならなければ `None`。leader 就役中の match_index
/// は後退しないので、返信の新旧判定に使える。
pub fn follower_match_index(
  node: Node(command),
  id: NodeId,
) -> Option(LogIndex) {
  case node.role_state {
    Leader(followers:, ..) ->
      case dict.get(followers, id) {
        Ok(FollowerState(match_index: match_index)) -> Some(match_index)
        Error(_) -> None
      }
    Follower | Candidate(_) -> None
  }
}

/// 新しいクラスタを作る。自ノードを含む初期 voter を受け取り、構成エント
/// リをログ末尾に追加してから立候補する。単独 voter ならこの時点で leader
/// に就く。ログが空でなかったり既に voter を持つ構成だったりするノードで
/// は作れない（`NotFresh`）。
pub fn create_cluster(
  node: Node(command),
  initial_voters: List(NodeId),
) -> #(Node(command), Result(LogPosition, CreateClusterError)) {
  case raft_log.log_last_position(node.log) != zero_position {
    True -> #(node, Error(types.NotFresh))
    False ->
      case set.is_empty(config(node).voters) {
        False -> #(node, Error(types.NotFresh))
        True ->
          case initial_voters {
            [] -> #(node, Error(types.EmptyVoters))
            voters -> {
              let new_config =
                ClusterConfig(..config_new(), voters: set.from_list(voters))
              let entry = raft_log.ConfigEntry(config: new_config)
              let node =
                node
                |> act(
                  action.AppendLogEntries(
                    raft_log.entries_from_list(zero_position, [entry]),
                  ),
                )
                |> push_log_entry(entry)
                |> transition_to_candidate
              #(node, Ok(raft_log.log_last_position(node.log)))
            }
          }
      }
  }
}

/// 受信メッセージを処理する。自分宛のメッセージはカウンタだけ増やして無
/// 視し、現在項より高い項のメッセージはまず Follower へ降格してから種類
/// 別のハンドラに渡す（順は DESIGN §10.2 固定点 1・2 で固定）。
/// `Error(LocalSnapshotMismatch)` は AE Call の追従経路だけで発生し、
/// 意味は「永続化の破損疑い」（DESIGN §6）。
pub fn handle_message(
  node: Node(command),
  message: Message(command),
) -> Result(Node(command), HandleError) {
  case message_from(message) == node.id {
    True ->
      Ok(
        bump(node, fn(m) {
          NodeMetrics(..m, self_messages_ignored: m.self_messages_ignored + 1)
        }),
      )
    False -> {
      let node = case node.current_term < message_term(message) {
        True ->
          node
          |> bump(fn(m) {
            NodeMetrics(
              ..m,
              term_advances_from_messages: m.term_advances_from_messages + 1,
            )
          })
          |> transition_to_follower(message_term(message))
        False -> node
      }
      case message {
        RequestVoteCall(from:, term:, last_log_position:) ->
          Ok(handle_request_vote_call(node, from, term, last_log_position))
        RequestVoteReply(from:, term:, vote_granted:) ->
          Ok(handle_request_vote_reply(node, from, term, vote_granted))
        AppendEntriesCall(from:, term:, leader_commit:, entries:) ->
          handle_append_entries_call(node, from, term, leader_commit, entries)
        AppendEntriesReply(from:, term:, last_log_position:) ->
          Ok(handle_append_entries_reply(node, from, term, last_log_position))
      }
    }
  }
}

/// 選出タイマーの満了処理。Follower と Candidate は立候補へ進み、leader
/// はハートビートを打つ。
pub fn handle_election_timeout(node: Node(command)) -> Node(command) {
  case node.role_state {
    Leader(..) -> heartbeat(node).0
    _ -> transition_to_candidate(node)
  }
}

/// InstallSnapshot action の実行を runner が完了したことを適用する。
/// 受理時はスナップショット境界を anchor にログを張り替え、まだ実行されて
/// いない Action の位置を追いつかせ、commit_index を境界まで max で前進
/// させるだけ。拒否時は観測可能な状態を一切変えない。
///
/// 前提: 現項より高い term のスナップショットは拒否されるので、より高い
/// term のメッセージを先に処理して `current_term >= スナップショットの
/// term` にしておく。
pub fn handle_snapshot_installed(
  node: Node(command),
  last_included_position: LogPosition,
  last_included_config: ClusterConfig,
) -> #(Node(command), Bool) {
  case is_valid_snapshot(node, last_included_position, last_included_config) {
    False -> #(node, False)
    True -> {
      // 保持断片が位置を含むなら suffix を残して構成を差し替え、含まない
      // なら position 起点の空断片へ張り替える。返り値の Bool（suffix が
      // 保持されたか）はここでは使わない。
      let #(log, _retained) =
        raft_log.log_handle_snapshot_installed(
          node.log,
          last_included_position,
          last_included_config,
        )
      let node = Node(..node, log: log)
      let node =
        Node(
          ..node,
          actions: rebase_actions(node.actions, last_included_position),
        )
      // commit_index をスナップショット境界より後ろには戻さない。leader が
      // is_valid_snapshot を通すにはすでに commit >= 境界なので max は
      // no-op。この前条件を検証する assert は置かない（checked 演算を置か
      // ない D6 の方針）。
      let commit = int.max(node.commit_index, last_included_position.index)
      #(Node(..node, commit_index: commit), True)
    }
  }
}

/// leader がハートビートを 1 回打つ。leader でなければ `(node, False)`。
/// follower が 1 台以上いれば空の `AppendEntriesCall` を broadcast し、い
/// なくてもタイマーだけ積み直す。いずれにせよタイマーは積む。
pub fn heartbeat(node: Node(command)) -> #(Node(command), Bool) {
  case node.role_state {
    Leader(followers:, ..) ->
      case dict.is_empty(followers) {
        True -> #(act(node, action.SetElectionTimeout), True)
        False -> {
          let call =
            AppendEntriesCall(
              term: node.current_term,
              from: node.id,
              leader_commit: node.commit_index,
              entries: raft_log.entries_from_list(
                raft_log.log_last_position(node.log),
                [],
              ),
            )
          let node =
            node
            |> act(action.BroadcastMessage(call))
            |> act(action.SetElectionTimeout)
          #(node, True)
        }
      }
    _ -> #(node, False)
  }
}

/// 受信前に見て、この投票依頼が現 leader を降格させうるものかを判定する。
/// 中立的な第三候補への投票依頼は leader を巻き込まないので後回しにでき
/// るが、現項で既に他へ投票済みの相手からの依頼は無視すると選出の妨害に
/// なる（DESIGN §10.2 固定点 9）。
pub fn could_be_disruptive_request_vote(
  node: Node(command),
  message: Message(command),
) -> Bool {
  let higher_term = node.current_term < message_term(message)
  let is_call = case message {
    RequestVoteCall(..) -> True
    _ -> False
  }
  let not_candidate = case node.role_state {
    Candidate(_) -> False
    _ -> True
  }
  let voted_for_other = case node.voted_for {
    Some(voted) -> voted != message_from(message)
    None -> False
  }
  higher_term && is_call && not_candidate && voted_for_other
}

/// 未実行 Action 集合を丸ごと差し替えるテスト用 seam。Node は opaque で
/// 外から `actions` を直接組み替えられないため、送出待ちメッセージの
/// 間引きと保持を確かめるテストがここを通る。本モジュールは内部 modules
/// 既定（gleam.toml 参照）で gleam_raft の公開面から外れて doc にも
/// 載らず、root の `gleam_raft`（DESIGN §3）からも露出しない（§3 暫定
/// 例外記載の非公開化は M6 で実施済み）。ただし gleam は v1.17 時点、
/// 内部 module の import をコンパイルエラーにしない（公式 doc の明記）
/// ため、守りは規約である。
@internal
pub fn set_actions(
  node: Node(command),
  actions: Actions(command),
) -> Node(command) {
  Node(..node, actions:)
}

// ここからは私有。Action 積みの基本操作と、role 遷移・メッセージハンドラ。

/// Action を 1 件積む（同じ種類なら既存と束ねる）。
fn act(node: Node(command), perform: Action(command)) -> Node(command) {
  Node(..node, actions: action.actions_set(node.actions, perform))
}

/// metrics の更新関数を当てる。
fn bump(
  node: Node(command),
  update: fn(NodeMetrics) -> NodeMetrics,
) -> Node(command) {
  Node(..node, metrics: update(node.metrics))
}

/// 現在項を設定し、永続化 Action を積む。
fn set_current_term(node: Node(command), term: Term) -> Node(command) {
  Node(..node, current_term: term) |> act(action.SaveCurrentTerm)
}

/// 投票先を設定し、永続化 Action を積む。`None` へのリセットも積む（消去
/// も永続化が必要な状態変化）。
fn set_voted_for(
  node: Node(command),
  voted_for: Option(NodeId),
) -> Node(command) {
  Node(..node, voted_for: voted_for) |> act(action.SaveVotedFor)
}

/// ログ末尾にエントリを 1 件追加する。term / 構成マーカーの管理は
/// `entries_push` が持つ。
fn push_log_entry(
  node: Node(command),
  entry: raft_log.LogEntry(command),
) -> Node(command) {
  Node(
    ..node,
    log: raft_log.Log(..node.log, entries: {
      raft_log.entries_push(node.log.entries, entry)
    }),
  )
}

/// 自ノードが単独 voter か。台数は voters と new_voters の和集合で数える。
/// joint 中に voters が自分だけでも new_voters に他のノードがいれば new 側
/// の多数派相手が必要なので単独即時 commit はできない。ただし自己所属の
/// チェックは voters 集合だけを見る。
fn is_self_solo_voter(node: Node(command)) -> Bool {
  let current = config(node)
  set.size(set.union(current.voters, current.new_voters)) == 1
  && set.contains(current.voters, node.id)
}

/// Follower への降格。`term` は現在項以上を前提とする（dispatch の降格
/// と、構成 commit 時の自己降格が同項から来るだけ）。Follower 以外から来
/// たときだけ選出タイマーを積み直す。同 role のまま項だけ上げる降格でタイ
/// マーを鳴らし直すと、leader が自分のタイマーで自壊する経路が復活する
/// （Raft Figure 2 の注意。DESIGN §10.2 固定点 7）。
fn transition_to_follower(node: Node(command), term: Term) -> Node(command) {
  case term >= node.current_term {
    False -> panic as "transition_to_follower: 現在項より低い項への降格"
    True -> {
      let node = case node.role_state {
        Follower -> node
        _ -> act(node, action.SetElectionTimeout)
      }
      let node = node |> set_current_term(term) |> set_voted_for(None)
      let node = Node(..node, role_state: Follower)
      Node(..node, actions: purge_stale_calls(node.actions, term))
    }
  }
}

/// 新項より古い項の Call かどうか。Reply 系は相手が待っている可能性があ
/// るので Call だけを数える。比較は厳密な `<` で、同じ項の Call は落とさ
/// ない（DESIGN §10.2 固定点 6）。
fn is_stale_call(message: Message(command), term: Term) -> Bool {
  let call = case message {
    RequestVoteCall(..) | AppendEntriesCall(..) -> True
    RequestVoteReply(..) | AppendEntriesReply(..) -> False
  }
  call && message_term(message) < term
}

/// 送出待ち Action から古い項の Call を落とす。broadcast 1 件と node ごと
/// の送信候補が対象で、storage 系と InstallSnapshot 系はそのまま。
fn purge_stale_calls(
  actions: Actions(command),
  term: Term,
) -> Actions(command) {
  let broadcast_message = case actions.broadcast_message {
    Some(message) ->
      case is_stale_call(message, term) {
        True -> None
        False -> Some(message)
      }
    None -> None
  }
  action.Actions(
    ..actions,
    broadcast_message: broadcast_message,
    send_messages: dict.filter(
      actions.send_messages,
      fn(_to: NodeId, message: Message(command)) {
        !is_stale_call(message, term)
      },
    ),
  )
}

/// 立候補。voter でなければカウンタを積む前に何もせず戻る（非 voter は
/// 選出を開始しない）。単独 voter はこの場で leader へ就く。
fn transition_to_candidate(node: Node(command)) -> Node(command) {
  case cluster_config.is_voter(config(node), node.id) {
    False -> node
    True -> {
      let node =
        bump(node, fn(m) {
          NodeMetrics(..m, elections_started: m.elections_started + 1)
        })
      let node =
        node
        |> set_current_term(node.current_term + 1)
        |> set_voted_for(Some(node.id))
      case is_self_solo_voter(node) {
        True -> transition_to_leader(node)
        False -> {
          let node =
            Node(
              ..node,
              role_state: Candidate(granted_votes: {
                set.new() |> set.insert(node.id)
              }),
            )
          node
          |> act(
            action.BroadcastMessage(RequestVoteCall(
              term: node.current_term,
              from: node.id,
              last_log_position: raft_log.log_last_position(node.log),
            )),
          )
          |> act(action.SetElectionTimeout)
        }
      }
    }
  }
}

/// leader 就役。leadership カウンタを数え、follower 台帳を最新構成から作
/// り直して現在の項のマーカーを propose する。follower が 1 台いればこの
/// propose が AE の broadcast を兼ね、単独 voter なら commit 判定まで進む。
fn transition_to_leader(node: Node(command)) -> Node(command) {
  let node =
    bump(node, fn(m) {
      NodeMetrics(..m, leaderships_started: m.leaderships_started + 1)
    })
  let node =
    Node(
      ..node,
      role_state: Leader(
        followers: dict.new(),
        solo_voter: is_self_solo_voter(node),
      ),
    )
  let node = rebuild_followers(node)
  let #(node, _) = propose(node, raft_log.TermEntry(term: node.current_term))
  node
}

/// leader の follower 台帳を最新構成に合わせ込む。既知のノードは
/// match_index を保持し、新規は 0 で追加する。構成から外れたノードは
/// 落とす。
fn rebuild_followers(node: Node(command)) -> Node(command) {
  case node.role_state {
    Leader(followers:, ..) as leader -> {
      let current = config(node)
      let followers =
        current
        |> unique_nodes
        |> list.filter(fn(other) { other != node.id })
        |> list.fold(followers, fn(acc, other) {
          case dict.get(acc, other) {
            Ok(_) -> acc
            Error(_) -> dict.insert(acc, other, FollowerState(match_index: 0))
          }
        })
        |> dict.filter(fn(other, _) { cluster_config.contains(current, other) })
      Node(..node, role_state: Leader(..leader, followers:))
    }
    _ -> node
  }
}

/// 構成変化後に leader の単独 voter フラグを最新構成へ合わせ込む。
fn refresh_solo_voter(node: Node(command)) -> Node(command) {
  case node.role_state {
    Leader(followers:, ..) ->
      Node(
        ..node,
        role_state: Leader(
          followers: followers,
          solo_voter: is_self_solo_voter(node),
        ),
      )
    _ -> node
  }
}

/// スナップショットを受理できるかの検査。高 term は拒否。commit_index <
/// 境界は非 leader だけ受理する（follower・candidate の追いつきでは commit
/// 範囲がまだ確定していない）。log が位置を含まないなら拒否、その index
/// 時点の構成と不一致なら拒否（構成取得が Option なので `Some(config)` と
/// の等値比較で書く）。
fn is_valid_snapshot(
  node: Node(command),
  position: LogPosition,
  config: ClusterConfig,
) -> Bool {
  case position.term > node.current_term {
    True -> False
    False ->
      case node.commit_index < position.index {
        True ->
          case node.role_state {
            Leader(..) -> False
            _ -> True
          }
        False ->
          case raft_log.entries_contains(node.log.entries, position) {
            False -> False
            True ->
              raft_log.log_get_config(node.log, position.index) == Some(config)
          }
      }
  }
}

/// スナップショット受理後の未実行 Action の位置を追いつかせる。
/// AppendLogEntries は再接続して空になったら None に捨てる。メッセージは
/// message 側の張替に委譲。install_snapshots は NodeId 集合で位置を持たな
/// いので張替対象外。NOTE: match_index と followers の再構築は意図的に行わ
/// ない。leader の追跡状態は古いままでも Reply の張替で整合が保てる。
fn rebase_actions(
  actions: Actions(command),
  position: LogPosition,
) -> Actions(command) {
  let append_log_entries = case actions.append_log_entries {
    None -> None
    Some(entries) -> {
      let #(rebuilt, _retained) =
        raft_log.entries_handle_snapshot_installed(entries, position)
      case raft_log.entries_is_empty(rebuilt) {
        True -> None
        False -> Some(rebuilt)
      }
    }
  }
  let broadcast_message = case actions.broadcast_message {
    None -> None
    Some(message) -> Some(message_handle_snapshot_installed(message, position))
  }
  let send_messages =
    dict.map_values(actions.send_messages, fn(_to, message) {
      message_handle_snapshot_installed(message, position)
    })
  action.Actions(
    ..actions,
    append_log_entries: append_log_entries,
    broadcast_message: broadcast_message,
    send_messages: send_messages,
  )
}

/// leader がログ末尾に 1 件提案する。追加分の AE broadcast（follower が
/// 1 台以上いれば）、ストレージ系 Action とタイマーの再設定、単独 voter
/// なら commit 判定を積む。返り値は追加後の末尾位置。
fn propose(
  node: Node(command),
  entry: raft_log.LogEntry(command),
) -> #(Node(command), LogPosition) {
  let old_last = raft_log.log_last_position(node.log)
  let appended = raft_log.entries_from_list(old_last, [entry])
  let node =
    node
    |> act(action.AppendLogEntries(appended))
    |> push_log_entry(entry)
  let node = case entry {
    raft_log.ConfigEntry(_) -> node |> rebuild_followers |> refresh_solo_voter
    _ -> node
  }
  let node = case node.role_state {
    Leader(solo_voter: True, ..) -> update_commit_index_if_possible(node)
    _ -> node
  }
  let node =
    case node.role_state {
      // 構成 commit 中の自己降格で Follower へ変わっていた場合は
      // broadcast を積まない。通常の流れでは到達しないが、panic を避け
      // て無操作で受け止める。
      Leader(followers:, ..) ->
        case dict.is_empty(followers) {
          True -> node
          False ->
            act(
              node,
              action.BroadcastMessage(AppendEntriesCall(
                term: node.current_term,
                from: node.id,
                leader_commit: node.commit_index,
                entries: appended,
              )),
            )
        }
      _ -> node
    }
    |> act(action.SetElectionTimeout)
  #(node, raft_log.log_last_position(node.log))
}

/// commit 可能位置の再計算。D3 に従い quorum 台帳は持たず、その場の自己
/// 位置と follower の match_index から総当りで探す。joint 構成では旧・新
/// 両方の voter 集団の多数派に届く位置だけが候補になる。
fn update_commit_index_if_possible(node: Node(command)) -> Node(command) {
  case node.role_state {
    Leader(followers:, ..) -> {
      let current = config(node)
      let self_last = raft_log.log_last_position(node.log).index
      let match_of = fn(who: NodeId) -> LogIndex {
        case who == node.id {
          True -> self_last
          False ->
            case dict.get(followers, who) {
              Ok(FollowerState(match_index: match_index)) -> match_index
              Error(_) -> 0
            }
        }
      }
      let candidates =
        set.union(current.voters, current.new_voters)
        |> set.to_list
        |> list.map(match_of)
        |> list.unique
        |> list.sort(fn(a, b) { int.compare(b, a) })
      let reaches_majority = fn(index: LogIndex) -> Bool {
        majority_reached(
          current.voters,
          cluster_config.voter_majority_count(current),
          match_of,
          index,
        )
        && majority_reached(
          current.new_voters,
          cluster_config.new_voter_majority_count(current),
          match_of,
          index,
        )
      }
      case list.find(candidates, reaches_majority) {
        Ok(index) -> commit_at(node, index, current)
        Error(_) -> node
      }
    }
    _ -> node
  }
}

/// voter 集団のうち追いつき済みの台数が必要数に届くか。非 joint の
/// new_voters 集団は必要数 0 なので常に真。
fn majority_reached(
  members: set.Set(NodeId),
  need: Int,
  match_of: fn(NodeId) -> LogIndex,
  index: LogIndex,
) -> Bool {
  members
  |> set.to_list
  |> list.filter(fn(member) { match_of(member) >= index })
  |> list.length
  >= need
}

/// commit 位置としての候補を検査して適用する。現在より進んでおらず、現在
/// 項のマーカー位置でなければ拒否。項をまたいだ commit は次の項の AE で
/// しか進められない。
fn commit_at(
  node: Node(command),
  index: LogIndex,
  current: ClusterConfig,
) -> Node(command) {
  case index <= node.commit_index {
    True -> node
    False ->
      case raft_log.entries_get_term(node.log.entries, index) {
        Some(term) ->
          case term == node.current_term {
            True -> apply_commit(node, index, current)
            False -> node
          }
        None -> node
      }
  }
}

/// commit 位置を進め、最新構成が commit 範囲に収まっていれば構成適用後
/// の後始末をする。joint 構成が commit されれば最終構成をその場で自動提案
/// する（`finalize_joint_consensus`）。最終構成の voters に自ノードがい
/// なければ同じ項のまま Follower へ降格する（自己降格）。ここのチェックは
/// 和集合でなく voters 集合だけを見る（joint 判定を通過した後なので
/// new_voters はもう空）。
fn apply_commit(
  node: Node(command),
  index: LogIndex,
  current: ClusterConfig,
) -> Node(command) {
  let node = Node(..node, commit_index: index)
  case index < raft_log.log_latest_config_index(node.log) {
    True -> node
    False ->
      case cluster_config.is_joint_consensus(current) {
        True -> finalize_joint_consensus(node)
        False ->
          case set.contains(current.voters, node.id) {
            True -> node
            False -> transition_to_follower(node, node.current_term)
          }
      }
  }
}

/// joint 構成が commit されたので、voters 集合を new_voters に置き、
/// new_voters を空にした最終構成を提案する。
fn finalize_joint_consensus(node: Node(command)) -> Node(command) {
  let current = config(node)
  let final =
    cluster_config.ClusterConfig(
      voters: current.new_voters,
      new_voters: set.new(),
      non_voters: current.non_voters,
    )
  let #(node, _) = propose(node, raft_log.ConfigEntry(config: final))
  node
}

/// `propose_config` の前条件チェック。ゲート順を
/// VotersChanged → OverlappingNodes → JointInProgress の順で固定する
/// （3 つの Bool を先に計算し、case の節順で判定順を決める）。
fn check_config_proposal(
  node: Node(command),
  new_config: ClusterConfig,
) -> Result(Nil, types.ProposeConfigError) {
  let latest = config(node)
  let voters_changed = latest.voters != new_config.voters
  let overlapping =
    !set.is_empty(set.intersection(new_config.voters, new_config.non_voters))
    || !set.is_empty(set.intersection(
      new_config.new_voters,
      new_config.non_voters,
    ))
  case voters_changed, overlapping, cluster_config.is_joint_consensus(latest) {
    True, _, _ -> Error(types.VotersChanged)
    _, True, _ -> Error(types.OverlappingNodes)
    _, _, True -> Error(types.JointInProgress)
    _, _, _ -> Ok(Nil)
  }
}

/// 投票依頼のハンドラ。古い項は false 返信を添えて、自分のログの方が新しく
/// ても既存投票が他人でも返信なしで黙って拒否する。返信を返すのは古い項
/// の拒否と受理のときだけ（DESIGN §10.2 固定点 1・2）。
fn handle_request_vote_call(
  node: Node(command),
  from: NodeId,
  term: Term,
  last_log_position: LogPosition,
) -> Node(command) {
  case term < node.current_term {
    True ->
      node
      |> bump(fn(m) {
        NodeMetrics(
          ..m,
          request_vote_calls_rejected_by_old_term: m.request_vote_calls_rejected_by_old_term
            + 1,
        )
      })
      |> act(action.SendMessage(
        to: from,
        message: RequestVoteReply(
          term: node.current_term,
          from: node.id,
          vote_granted: False,
        ),
      ))
    False ->
      case
        compare_position(
          raft_log.log_last_position(node.log),
          last_log_position,
        )
        == Gt
      {
        True ->
          bump(node, fn(m) {
            NodeMetrics(
              ..m,
              request_vote_calls_rejected_by_log: m.request_vote_calls_rejected_by_log
                + 1,
            )
          })
        False -> {
          let node = case node.voted_for {
            None -> set_voted_for(node, Some(from))
            Some(_) -> node
          }
          case node.voted_for == Some(from) {
            False ->
              bump(node, fn(m) {
                NodeMetrics(
                  ..m,
                  request_vote_calls_rejected_by_existing_vote: m.request_vote_calls_rejected_by_existing_vote
                    + 1,
                )
              })
            True ->
              node
              |> act(action.SendMessage(
                to: from,
                message: RequestVoteReply(
                  term: node.current_term,
                  from: node.id,
                  vote_granted: True,
                ),
              ))
              |> act(action.SetElectionTimeout)
          }
        }
      }
  }
}

/// 投票返信のハンドラ。Candidate でなければ数えて終わり。未受理の返信は
/// 項の新旧を見ずに捨てる（捨てログで古い項カウンタを汚さない）。受理
/// 済みの投票が voter・new_voter 双方の多数派に揃え
/// ば leader へ就く。
fn handle_request_vote_reply(
  node: Node(command),
  from: NodeId,
  term: Term,
  vote_granted: Bool,
) -> Node(command) {
  case node.role_state {
    Candidate(granted_votes:) ->
      case vote_granted {
        False -> node
        True ->
          case term < node.current_term {
            True ->
              bump(node, fn(m) {
                NodeMetrics(
                  ..m,
                  request_vote_replies_ignored_from_old_terms: m.request_vote_replies_ignored_from_old_terms
                    + 1,
                )
              })
            False -> {
              let granted = set.insert(granted_votes, from)
              let node =
                Node(..node, role_state: Candidate(granted_votes: granted))
              let current = config(node)
              let granted_of = fn(members: set.Set(NodeId)) -> Int {
                members
                |> set.to_list
                |> list.filter(fn(member) { set.contains(granted, member) })
                |> list.length
              }
              case
                granted_of(current.voters)
                >= cluster_config.voter_majority_count(current)
                && granted_of(current.new_voters)
                >= cluster_config.new_voter_majority_count(current)
              {
                True -> transition_to_leader(node)
                False -> node
              }
            }
          }
      }
    _ ->
      bump(node, fn(m) {
        NodeMetrics(
          ..m,
          request_vote_replies_ignored_while_not_candidate: m.request_vote_replies_ignored_while_not_candidate
            + 1,
        )
      })
  }
}

/// 現在の末尾位置を AE Reply で送り返す。旧 term の拒否時にも送るのは、
/// 送手に自ノードの項を気づかせるため。
fn reply_append_entries(node: Node(command), to: NodeId) -> Node(command) {
  act(
    node,
    action.SendMessage(
      to: to,
      message: AppendEntriesReply(
        term: node.current_term,
        from: node.id,
        last_log_position: raft_log.log_last_position(node.log),
      ),
    ),
  )
}

/// AE Call の追従。返り値の Bool は「発散がなく commit を進めてよいか」。
///
/// 分岐しているときは尾部を切り捨てるだけ切り、`AppendLogEntries` は積ま
/// ない（発散の根が特定できるまで永続化要求を出さない）。切り捨て位置が
/// スナップショット位置より前を向いたときは永続化の破損疑いとして
/// `LocalSnapshotMismatch`（DESIGN §6）。
fn append_log_entries_from_leader(
  node: Node(command),
  incoming: raft_log.LogEntries(command),
) -> Result(#(Node(command), Bool), HandleError) {
  case raft_log.entries_contains(node.log.entries, incoming.last_position) {
    True ->
      Ok(#(node, raft_log.log_last_position(node.log) == incoming.last_position))
    False ->
      case raft_log.entries_contains(node.log.entries, incoming.prev_position) {
        False ->
          case
            raft_log.entries_contains_index(
              node.log.entries,
              incoming.prev_position.index,
            )
          {
            False -> Ok(#(node, False))
            True -> {
              let node =
                bump(node, fn(m) {
                  NodeMetrics(
                    ..m,
                    log_divergences_detected_by_follower: m.log_divergences_detected_by_follower
                      + 1,
                  )
                })
              let snapshot_index =
                raft_log.log_snapshot_position(node.log).index
              let new_len = incoming.prev_position.index - snapshot_index - 1
              case new_len < 0 {
                True -> Error(types.LocalSnapshotMismatch)
                False -> {
                  let #(fragment, _) =
                    raft_log.entries_truncate(node.log.entries, new_len)
                  Ok(#(
                    Node(
                      ..node,
                      log: raft_log.Log(..node.log, entries: fragment),
                    ),
                    False,
                  ))
                }
              }
            }
          }
        True -> {
          let stripped =
            raft_log.entries_strip_common_prefix(incoming, node.log.entries)
          let #(log, appended) = raft_log.log_append_suffix(node.log, stripped)
          let node = Node(..node, log:)
          // anchor が見つからず appended が false のままの形は通常の
          // 流れでは到達しない。アクションを積まず無操作で受け止める。
          let node = case appended {
            True -> act(node, action.AppendLogEntries(stripped))
            False -> node
          }
          Ok(#(node, True))
        }
      }
  }
}

/// 投票依頼と同じく、古い項は返信を添えて黙って拒否し、leader が同項の
/// Call を受けたら衝突として数えるだけ。candidate の受理は role だけ
/// Follower に変える（固定点 4。項は動かさず、末尾の SetElectionTimeout
/// がタイマーを担う）。追従が発散を検出したら commit は進めない。
fn handle_append_entries_call(
  node: Node(command),
  from: NodeId,
  term: Term,
  leader_commit: LogIndex,
  entries: raft_log.LogEntries(command),
) -> Result(Node(command), HandleError) {
  case term < node.current_term {
    True ->
      Ok(
        node
        |> bump(fn(m) {
          NodeMetrics(
            ..m,
            append_entries_calls_rejected_by_old_term: m.append_entries_calls_rejected_by_old_term
              + 1,
          )
        })
        |> reply_append_entries(from),
      )
    False ->
      case node.role_state {
        Leader(..) ->
          Ok(
            bump(node, fn(m) {
              NodeMetrics(
                ..m,
                same_term_append_entries_calls_received_by_leader: m.same_term_append_entries_calls_received_by_leader
                  + 1,
              )
            }),
          )
        _ -> {
          // candidate はこの項の正当な leader として送り手を受け入れる。
          let node = case node.role_state {
            Follower -> node
            _ -> Node(..node, role_state: Follower)
          }
          let node = case node.voted_for == Some(from) {
            True -> node
            False -> set_voted_for(node, Some(from))
          }
          case append_log_entries_from_leader(node, entries) {
            Error(error) -> Error(error)
            Ok(#(node, no_divergence)) -> {
              let node = case no_divergence {
                True -> {
                  let next_commit_index =
                    int.min(
                      leader_commit,
                      raft_log.log_last_position(node.log).index,
                    )
                  case node.commit_index < next_commit_index {
                    True -> Node(..node, commit_index: next_commit_index)
                    False -> node
                  }
                }
                False -> node
              }
              Ok(
                node
                |> reply_append_entries(from)
                |> act(action.SetElectionTimeout),
              )
            }
          }
        }
      }
  }
}

/// AE Reply のハンドラ。検査順は固定点 3 の固定点どおり、古い項 → 非
/// leader → 未知のノード → match_index より遅れた返信 → log に含まれな
/// い位置（発散修復 / leader より先行 / スナップショット要請の三通り）→
/// match_index 前進と commit 試行 → 追補送信。
fn handle_append_entries_reply(
  node: Node(command),
  from: NodeId,
  term: Term,
  follower_last_position: LogPosition,
) -> Node(command) {
  case term < node.current_term {
    True ->
      bump(node, fn(m) {
        NodeMetrics(
          ..m,
          append_entries_replies_ignored_from_old_terms: m.append_entries_replies_ignored_from_old_terms
            + 1,
        )
      })
    False ->
      case node.role_state {
        Leader(followers:, ..) as leader_state ->
          case dict.get(followers, from) {
            Error(_) ->
              bump(node, fn(m) {
                NodeMetrics(
                  ..m,
                  append_entries_replies_ignored_from_unknown_nodes: m.append_entries_replies_ignored_from_unknown_nodes
                    + 1,
                )
              })
            Ok(FollowerState(match_index:)) ->
              case follower_last_position.index < match_index {
                True ->
                  bump(node, fn(m) {
                    NodeMetrics(
                      ..m,
                      append_entries_replies_ignored_behind_match_index: m.append_entries_replies_ignored_behind_match_index
                        + 1,
                    )
                  })
                False ->
                  case
                    raft_log.entries_contains(
                      node.log.entries,
                      follower_last_position,
                    )
                  {
                    False ->
                      reply_position_not_contained(
                        node,
                        from,
                        follower_last_position,
                      )
                    True -> {
                      // commit 更新は末尾位置を変えることがある（設定確定
                      // など）ので、追いつき判定はその前に行う。
                      let is_follower_up_to_date =
                        follower_last_position.index
                        == raft_log.log_last_position(node.log).index
                      let node = case
                        match_index < follower_last_position.index
                      {
                        True -> {
                          let followers =
                            dict.insert(
                              followers,
                              from,
                              FollowerState(
                                match_index: follower_last_position.index,
                              ),
                            )
                          let node =
                            Node(
                              ..node,
                              role_state: Leader(..leader_state, followers:),
                            )
                          case
                            node.commit_index < follower_last_position.index
                          {
                            True -> update_commit_index_if_possible(node)
                            False -> node
                          }
                        }
                        False -> node
                      }
                      case is_follower_up_to_date {
                        True -> node
                        False ->
                          case
                            raft_log.entries_since(
                              node.log.entries,
                              follower_last_position,
                            )
                          {
                            // 上に `contains` を通った位置なので本来到達
                            // しない。panic を避けて無操作で受け止める。
                            None -> node
                            Some(delta) ->
                              act(
                                node,
                                action.SendMessage(
                                  to: from,
                                  message: AppendEntriesCall(
                                    term: node.current_term,
                                    from: node.id,
                                    leader_commit: node.commit_index,
                                    entries: delta,
                                  ),
                                ),
                              )
                          }
                      }
                    }
                  }
              }
          }
        _ ->
          bump(node, fn(m) {
            NodeMetrics(
              ..m,
              append_entries_replies_ignored_while_not_leader: m.append_entries_replies_ignored_while_not_leader
                + 1,
            )
          })
      }
  }
}

/// 返信位置が断片に含まれないときの分岐。その index の項が自 log に存在
/// するなら発散なので、leader 自身の項でその 1 件をもう一度送らせて根を
/// 確定させる。index が leader の末尾より後なら黙って数えるだけ（leader
/// の log が伸びればいずれ発散点が検出される）。
/// それより前はスナップショットを配るしかない。
fn reply_position_not_contained(
  node: Node(command),
  from: NodeId,
  follower_last_position: LogPosition,
) -> Node(command) {
  case
    raft_log.entries_get_term(node.log.entries, follower_last_position.index)
  {
    Some(divergence_term) ->
      node
      |> bump(fn(m) {
        NodeMetrics(
          ..m,
          log_divergences_detected_by_leader: m.log_divergences_detected_by_leader
            + 1,
        )
      })
      |> act(action.SendMessage(
        to: from,
        message: AppendEntriesCall(
          term: node.current_term,
          from: node.id,
          leader_commit: node.commit_index,
          entries: raft_log.entries_new(types.position(
            divergence_term,
            follower_last_position.index,
          )),
        ),
      ))
    None ->
      case
        raft_log.log_last_position(node.log).index
        < follower_last_position.index
      {
        True ->
          bump(node, fn(m) {
            NodeMetrics(
              ..m,
              append_entries_replies_ahead_of_leader: m.append_entries_replies_ahead_of_leader
                + 1,
            )
          })
        False -> act(node, action.InstallSnapshot(to: from))
      }
  }
}
