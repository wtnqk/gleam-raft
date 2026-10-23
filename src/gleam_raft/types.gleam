//// Raft が扱う基本型と、値の比較・演算を置くモジュール。
////
//// 公開 API の全体像は docs/DESIGN.md §4 に置く。このモジュールは
//// sans-IO の契約のうち「型」だけを担い、効果も状態も持たない。
////
//// `Term` / `NodeId` / `LogIndex` は `Int` の alias であり、混同を
//// コンパイラには検出させない（DESIGN §2 D5）。値がモジュール間を
//// 渡る経路は public 関数のシグネチャに型注釈を付け、alias 名を
//// 注釈で明示することで discipline を担保する。

import gleam/int
import gleam/order

/// 選挙の項。0 以上で単調増加する。
pub type Term =
  Int

/// クラスタ内のノード識別子。
pub type NodeId =
  Int

/// ログの位置番号。0 は「ログなし」の意味として扱う。
pub type LogIndex =
  Int

/// ログ末尾の位置。term と index の組で、全順序を持つ。
pub type LogPosition {
  LogPosition(term: Term, index: LogIndex)
}

/// 位置を構成する。
pub fn position(term: Term, index: LogIndex) -> LogPosition {
  LogPosition(term: term, index: index)
}

/// 空ログの位置。スナップショット前の起点に使う。
pub const zero_position: LogPosition = LogPosition(term: 0, index: 0)

/// term 優先、term が同じなら index で比較する全順序。
pub fn compare_position(a: LogPosition, b: LogPosition) -> order.Order {
  case int.compare(a.term, b.term) {
    order.Eq -> int.compare(a.index, b.index)
    other -> other
  }
}

/// 同一項内の次の位置。index を 1 増やし、term は据え置く。
pub fn position_next(p: LogPosition) -> LogPosition {
  LogPosition(term: p.term, index: p.index + 1)
}

/// ノードの役割。Raft 論文では state と呼ばれる概念。
pub type Role {
  Follower
  Candidate
  Leader
}

/// 提案の commit 状況。
pub type CommitStatus {
  InProgress
  Committed
  Rejected
  Unknown
}

/// `create_cluster` の前条件違反。
pub type CreateClusterError {
  /// 起動済みノードへの再適用。
  NotFresh

  /// 投票者集合が空。
  EmptyVoters
}

/// `propose_config` の前条件違反。
pub type ProposeConfigError {
  /// 提案時点で自ノードが leader でない。
  NotLeader

  /// 現在の voters と new_voters が食い違っている。
  VotersChanged

  /// voters と new_voters が交わっている。
  OverlappingNodes

  /// 前回の joint 構成が未完。
  JointInProgress
}

/// `handle_snapshot_installed` の前条件違反。
pub type HandleError {
  /// 適用済みスナップショットとノードの状態が合わない。
  LocalSnapshotMismatch
}
