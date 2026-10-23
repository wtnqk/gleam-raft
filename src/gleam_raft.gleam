//// sans-IO の Raft ライブラリ。ライブラリ本体は I/O を持たない。
////
//// `Node` への入力（メッセージ受信・選挙タイムアウト・ローカル提案）は
//// 純粋な状態変換であり、効果は優先順を持つ `Action` として呼び出し側に
//// 返される。Action を実行してはじめてクラスタとして振る舞う。
////
//// 公開 API の全体は docs/DESIGN.md §3 に置く。API は BEAM を前提に再設計
//// しており、言語と実行環境に由来する変更は DESIGN §2 に D1–D6 として
//// 記録する。D 項目のない箇所は DESIGN の記述する挙動を既定とする。実装は
//// `gleam_raft/internal/node` にあり、本モジュールは §3 の集合だけを型 alias
//// と委譲で露出する。DESIGN §3 に暫定例外として記載されていたテスト専用
//// シームは露出しない（M6 で非公開化済み）。`internal` を含む名前の
//// サブモジュールは gleam.toml の `internal_modules` の既定値に入り、生成
//// doc から除かれ LSP の補完にも出ない。ただし v1.17 のコンパイラは
//// 外部 package からの import をエラーにしない（公式 doc 明記）ので、
//// §3 外への到達は「サポート外」規約で抑える。関数シグネチャの型
//// （`Log`・`ClusterConfig`・`Message` など）は DESIGN §4–§10 の公開
//// サブモジュールから import する。

import gleam/option.{type Option}
import gleam_raft/action.{type Action, type Actions}
import gleam_raft/cluster_config.{type ClusterConfig}
import gleam_raft/internal/node as raft_node
import gleam_raft/log.{type Log}
import gleam_raft/message.{type Message}
import gleam_raft/types.{
  type CommitStatus, type CreateClusterError, type HandleError, type LogIndex,
  type LogPosition, type NodeId, type ProposeConfigError, type Role, type Term,
}

/// ノード状態機械そのもの。DESIGN §10。`internal/node` の opaque 型への
/// alias であり、外部からは構造を見ず関数経由でのみ操作する。
pub type Node(command) =
  raft_node.Node(command)

/// 観測用メトリクス（DESIGN §10）。全カウンタの初期値は
/// `node_metrics_default`。
pub type NodeMetrics =
  raft_node.NodeMetrics

/// 全カウンタ 0・空状態のメトリクス初期値（DESIGN §3 脚注）。
pub fn node_metrics_default() -> NodeMetrics {
  raft_node.node_metrics_default()
}

/// 指定 id の新規ノードを作る（初期項 0・Follower・空日志）。
pub fn start(id: NodeId) -> Node(command) {
  raft_node.start(id)
}

/// 永続化ストアから復元してノードを再起動する（DESIGN §10.1）。
pub fn restart(
  id: NodeId,
  current_term: Term,
  voted_for: Option(NodeId),
  log: Log(command),
) -> Node(command) {
  raft_node.restart(id, current_term, voted_for, log)
}

/// 初回のみ、指定 voter 群から初期クラスタ構成を提案して合意を結ぶ。
/// 成功時は構成エントリ commit 済みのノードと構成位置を返す。
pub fn create_cluster(
  node: Node(command),
  initial_voters: List(NodeId),
) -> #(Node(command), Result(LogPosition, CreateClusterError)) {
  raft_node.create_cluster(node, initial_voters)
}

/// ノード id。
pub fn id(node: Node(command)) -> NodeId {
  raft_node.id(node)
}

/// 現在の役割（`gleam_raft/types` の `Role`）。
pub fn role(node: Node(command)) -> Role {
  raft_node.role(node)
}

/// 現在の項。
pub fn current_term(node: Node(command)) -> Term {
  raft_node.current_term(node)
}

/// 現在の項で投票した相手。未投票なら `None`。
pub fn voted_for(node: Node(command)) -> Option(NodeId) {
  raft_node.voted_for(node)
}

/// 現在の日志（DESIGN §5 の公開ログ）。
pub fn log(node: Node(command)) -> Log(command) {
  raft_node.log(node)
}

/// commit 済みのログ位置番号（`LogIndex`。`LogPosition` と違い term を
/// 含まない）。
pub fn commit_index(node: Node(command)) -> LogIndex {
  raft_node.commit_index(node)
}

/// 日志中の最新クラスタ構成（log の latest config、DESIGN §9）。
pub fn config(node: Node(command)) -> ClusterConfig {
  raft_node.config(node)
}

/// 自ノードを除く構成メンバーの集合（DESIGN §3 の注記どおり
/// `unique_nodes` から自己を引いたもの）。
pub fn peers(node: Node(command)) -> List(NodeId) {
  raft_node.peers(node)
}

/// 観測用メトリクスの読み取り専用コピー。
pub fn metrics(node: Node(command)) -> NodeMetrics {
  raft_node.metrics(node)
}

/// 未実行 Action の観測（読み取り専用コピー）。実行は `take_action`。
pub fn actions(node: Node(command)) -> Actions(command) {
  raft_node.actions(node)
}

/// 実行待ち Action を優先順に 1 件取り出す。呼び出し側ループの形は
/// DESIGN §3 の README 予定サンプル（`list.fold` で空になるまで回す）を参照。
pub fn take_action(
  node: Node(command),
) -> #(Node(command), Option(Action(command))) {
  raft_node.take_action(node)
}

/// ローカルコマンド提案。leader でなければ `None`。
pub fn propose_command(
  node: Node(command),
  command: command,
) -> #(Node(command), Option(LogPosition)) {
  raft_node.propose_command(node, command)
}

/// クラスタ構成変更の提案（DESIGN §9 の joint consensus 手順）。
pub fn propose_config(
  node: Node(command),
  new_config: ClusterConfig,
) -> #(Node(command), Result(LogPosition, ProposeConfigError)) {
  raft_node.propose_config(node, new_config)
}

/// 提案位置の commit 状態（DESIGN §3）。
pub fn get_commit_status(
  node: Node(command),
  position: LogPosition,
) -> CommitStatus {
  raft_node.get_commit_status(node, position)
}

/// leader が周期で打つ heartbeat。leader でなければ `(node, False)`。
pub fn heartbeat(node: Node(command)) -> #(Node(command), Bool) {
  raft_node.heartbeat(node)
}

/// 受信メッセージが現状の leader を追いやりうる投票依頼か（pre-Vote 相当の
/// 破壊可能性判定、DESIGN §7）。
pub fn could_be_disruptive_request_vote(
  node: Node(command),
  message: Message(command),
) -> Bool {
  raft_node.could_be_disruptive_request_vote(node, message)
}

/// 受信メッセージを処理する。前条件違反は `HandleError` を返す（D1）。
pub fn handle_message(
  node: Node(command),
  message: Message(command),
) -> Result(Node(command), HandleError) {
  raft_node.handle_message(node, message)
}

/// 選挙タイムアウトの発生を伝える（`Action.SetElectionTimeout` の期限到来）。
pub fn handle_election_timeout(node: Node(command)) -> Node(command) {
  raft_node.handle_election_timeout(node)
}

/// スナップショットインストール完了の通知（DESIGN §5）。
pub fn handle_snapshot_installed(
  node: Node(command),
  last_included_position: LogPosition,
  last_included_config: ClusterConfig,
) -> #(Node(command), Bool) {
  raft_node.handle_snapshot_installed(
    node,
    last_included_position,
    last_included_config,
  )
}

/// leader が追う特定 follower の一致位置（`None` は未知ノード）。
pub fn follower_match_index(
  node: Node(command),
  id: NodeId,
) -> Option(LogIndex) {
  raft_node.follower_match_index(node, id)
}
