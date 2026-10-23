//// 多数派到達インデックスの計算。
////
//// top-k 集合を保持せず、入力から毎回 k 番目に大きいマッチインデックスを
//// 総当りで再計算する（D3）。現実的なクラスタサイズ（7 前後）では
//// O(v log v) で足り、状態を持たない分バグが入りにくい。正しさは
//// TESTPLAN §1.4 の総当り一致テストで担保する。
////
//// 投票者集合が空のときは None を返す。前条件違反を core で panic 相当の
//// 失敗にしない方針（D1）により、返り値で返す。

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set
import gleam_raft/cluster_config.{type ClusterConfig}
import gleam_raft/types.{type LogIndex, type NodeId}

/// 多数派に到達した最小のインデックス。
///
/// `voters` のマッチ集合から `voter_majority_count` 番目に大きい値を返す。
/// `new_voters` が非空なら同種の計算を両方行い、その min を返す
/// （片方でも None なら None）。
///
/// 各 voter のマッチは自ノードなら `self_match`、それ以外なら `followers`
/// からの参照、無ければ 0。自ノードが voters に含まれない場合
/// `self_match` は結果に影響せず、`followers` に自ノードの鍵が混じっても
/// `self_match` が一度だけ計上される。
pub fn smallest_majority_index(
  config: ClusterConfig,
  self_id: NodeId,
  self_match: LogIndex,
  followers: Dict(NodeId, LogIndex),
) -> Option(LogIndex) {
  let old =
    config.voters
    |> set.to_list
    |> list.map(match_index(_, self_id, self_match, followers))
    |> kth_largest(cluster_config.voter_majority_count(config))
  case cluster_config.is_joint_consensus(config) {
    False -> old
    True ->
      case
        config.new_voters
        |> set.to_list
        |> list.map(match_index(_, self_id, self_match, followers))
        |> kth_largest(cluster_config.new_voter_majority_count(config))
      {
        Some(new) -> min_option(old, Some(new))
        None -> None
      }
  }
}

/// voter 1 台のマッチインデックス。自ノードは `self_match`、他は
/// `followers` 参照で、無ければ 0。
fn match_index(
  id: NodeId,
  self_id: NodeId,
  self_match: LogIndex,
  followers: Dict(NodeId, LogIndex),
) -> LogIndex {
  case id == self_id {
    True -> self_match
    False ->
      case dict.get(followers, id) {
        Ok(matched) -> matched
        Error(_) -> 0
      }
  }
}

/// k 番目に大きい値。同一値のノードは別枠として数える。
/// 要素が k 個に満たなければ None。
fn kth_largest(indices: List(LogIndex), k: Int) -> Option(LogIndex) {
  let ascending = list.sort(indices, int.compare)
  let offset = list.length(ascending) - k
  case offset >= 0 {
    True ->
      case drop(ascending, offset) {
        [kth, ..] -> Some(kth)
        [] -> None
      }
    False -> None
  }
}

fn drop(items: List(a), count: Int) -> List(a) {
  case items, count {
    _, 0 -> items
    [], _ -> []
    [_, ..rest], _ -> drop(rest, count - 1)
  }
}

fn min_option(a: Option(LogIndex), b: Option(LogIndex)) -> Option(LogIndex) {
  case a, b {
    Some(x), Some(y) -> Some(int.min(x, y))
    _, _ -> None
  }
}
