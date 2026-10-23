//// クラスタ構成（メンバシップ）を置くモジュール。
////
//// 公開 API は docs/DESIGN.md §9 に置く。3 集合は `gleam/set` で
//// 保持し、昇順が必要るところ（`unique_nodes`）は明示的にソートする。
////
//// 構造は開いた record のままで、永続化・検査から中身を読める。
//// 妥当性の前条件は `to_joint_consensus` が Bool で返し、
//// `propose_config` 経由の呼び出し側は型付きエラーへ変換する。

import gleam/int
import gleam/list
import gleam/set
import gleam_raft/types

/// 投票者・新投票者・非投票者の 3 集合。`new_voters` が非空なら
/// joint consensus 中であり、旧構成と新構成の両方の多数派が必要になる。
pub type ClusterConfig {
  ClusterConfig(
    voters: set.Set(types.NodeId),
    new_voters: set.Set(types.NodeId),
    non_voters: set.Set(types.NodeId),
  )
}

/// 空の構成を作る。単独ノードのクラスタ形成前にこれを起点にする。
pub fn config_new() -> ClusterConfig {
  ClusterConfig(voters: set.new(), new_voters: set.new(), non_voters: set.new())
}

/// 3 集合のいずれかに含まれるか。
pub fn contains(config: ClusterConfig, id: types.NodeId) -> Bool {
  set.contains(config.voters, id)
  || set.contains(config.new_voters, id)
  || set.contains(config.non_voters, id)
}

/// joint consensus 中か。
pub fn is_joint_consensus(config: ClusterConfig) -> Bool {
  !set.is_empty(config.new_voters)
}

/// 構成に現れる全ノードの昇順リスト。`set.to_list` の出力順は文書化
/// された保証ではないため、`int.compare` で明示的にソートする。
pub fn unique_nodes(config: ClusterConfig) -> List(types.NodeId) {
  set.to_list({
    set.union(set.union(config.voters, config.new_voters), config.non_voters)
  })
  |> list.sort(int.compare)
}

/// 現構成か新構成のいずれかで投票者か。voters と new_voters の和集合を
/// 見る。joint 中に new_voters だけのノードも立候補に参加できる。例外は
/// 自己降格の判定で、ここは voters 集合だけを見るため `apply_commit` は
/// 本関数を通さず `set.contains` で直接読む。
pub fn is_voter(config: ClusterConfig, id: types.NodeId) -> Bool {
  set.contains(config.voters, id) || set.contains(config.new_voters, id)
}

/// 新構成の投票者か。
pub fn is_new_voter(config: ClusterConfig, id: types.NodeId) -> Bool {
  set.contains(config.new_voters, id)
}

/// 非投票者（learner）か。
pub fn is_non_voter(config: ClusterConfig, id: types.NodeId) -> Bool {
  set.contains(config.non_voters, id)
}

/// 旧構成の多数派に必要な台数。
pub fn voter_majority_count(config: ClusterConfig) -> Int {
  set.size(config.voters) / 2 + 1
}

/// 新構成の多数派に必要な台数。非 joint なら 0。
pub fn new_voter_majority_count(config: ClusterConfig) -> Int {
  case set.is_empty(config.new_voters) {
    True -> 0
    False -> set.size(config.new_voters) / 2 + 1
  }
}

/// 構成を joint consensus へ変換する。
///
/// `adding` は new_voters に加え、既存の non_voter なら non_voters から
/// 外して昇格させる。`removing` は new_voters から除く。voters は不変。
///
/// 前条件は `False` で返す。いずれかに違反している場合、構成は
/// 変換しない（第 1 要素は入力のまま）。
///
/// - 非 joint 中であること
/// - `adding` と `removing` が交わらないこと
/// - `adding` が現在の voters に含まれないこと
/// - `removing` が現在の voters に含まれること
pub fn to_joint_consensus(
  config: ClusterConfig,
  adding: List(types.NodeId),
  removing: List(types.NodeId),
) -> #(ClusterConfig, Bool) {
  let adding_set = set.from_list(adding)
  let removing_set = set.from_list(removing)
  let valid = {
    !is_joint_consensus(config)
    && set.is_empty(set.intersection(adding_set, removing_set))
    && set.is_empty(set.intersection(adding_set, config.voters))
    && set.is_subset(removing_set, of: config.voters)
  }
  case valid {
    False -> #(config, False)
    True -> {
      let new_voters =
        set.difference(set.union(config.voters, adding_set), removing_set)
      let non_voters = set.difference(config.non_voters, new_voters)
      #(
        ClusterConfig(
          voters: config.voters,
          new_voters: new_voters,
          non_voters: non_voters,
        ),
        True,
      )
    }
  }
}
