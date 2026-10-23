import gleam/list
import gleam/set
import gleam_raft/cluster_config.{ClusterConfig, config_new}
import gleeunit/should

pub fn unique_nodes_test() -> Nil {
  let config =
    ClusterConfig(
      voters: set.from_list([1, 2]),
      new_voters: set.from_list([2, 3]),
      non_voters: set.from_list([4, 5, 6]),
    )
  should.equal(cluster_config.unique_nodes(config), [1, 2, 3, 4, 5, 6])
}

pub fn to_joint_consensus_promotes_existing_non_voter_test() -> Nil {
  let config =
    ClusterConfig(
      voters: set.from_list([1, 2, 3]),
      new_voters: set.new(),
      non_voters: set.from_list([4, 5]),
    )
  let #(joint, ok) = cluster_config.to_joint_consensus(config, [4], [])
  should.be_true(ok)
  should.equal(joint.voters, config.voters)
  should.be_true(cluster_config.is_new_voter(joint, 4))
  should.equal(joint.non_voters, set.from_list([5]))
  should.equal(
    set.is_empty(set.intersection(joint.new_voters, joint.non_voters)),
    True,
  )
}

// 自前の追加: 前条件の 4 分岐が False を返し、入力をそのまま返す。
pub fn to_joint_consensus_preconditions_test() -> Nil {
  let voters3 =
    ClusterConfig(
      voters: set.from_list([1, 2, 3]),
      new_voters: set.new(),
      non_voters: set.from_list([4]),
    )
  let joint =
    ClusterConfig(
      voters: set.from_list([1, 2, 3]),
      new_voters: set.from_list([1, 2, 3, 4]),
      non_voters: set.new(),
    )
  // 非 joint 中であること
  let #(_, ok_joint) = cluster_config.to_joint_consensus(joint, [5], [])
  should.be_false(ok_joint)
  // adding が既存 voter と交わる
  let #(same, ok_existing) = cluster_config.to_joint_consensus(voters3, [2], [])
  should.be_false(ok_existing)
  should.equal(same, voters3)
  // adding と removing が交わる
  let #(_, ok_overlap) = cluster_config.to_joint_consensus(voters3, [4], [4])
  should.be_false(ok_overlap)
  // removing が voters に無い
  let #(_, ok_remove_unknown) =
    cluster_config.to_joint_consensus(voters3, [], [9])
  should.be_false(ok_remove_unknown)
}

// 自前の追加: 多数派台数と joint 判定の境界。
pub fn majority_and_joint_flags_test() -> Nil {
  let one =
    ClusterConfig(
      voters: set.from_list([1]),
      new_voters: set.new(),
      non_voters: set.new(),
    )
  let two =
    ClusterConfig(
      voters: set.from_list([1, 2]),
      new_voters: set.new(),
      non_voters: set.new(),
    )
  should.equal(cluster_config.voter_majority_count(one), 1)
  should.equal(cluster_config.voter_majority_count(two), 2)
  should.equal(cluster_config.new_voter_majority_count(two), 0)
  should.be_false(cluster_config.is_joint_consensus(one))
  let joint =
    ClusterConfig(
      voters: set.from_list([1, 2]),
      new_voters: set.from_list([1, 2, 3, 4]),
      non_voters: set.new(),
    )
  should.equal(cluster_config.new_voter_majority_count(joint), 3)
  should.be_true(cluster_config.is_joint_consensus(joint))
}

// config_new は全集合が空。
pub fn config_new_is_empty_test() -> Nil {
  let fresh = config_new()
  should.equal(cluster_config.unique_nodes(fresh), list.new())
  should.be_false(cluster_config.contains(fresh, 1))
}
