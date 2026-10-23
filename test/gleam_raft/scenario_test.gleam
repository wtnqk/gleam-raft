//
// M4 のシナリオテスト本体。ハーネスは gleam_raft/harness に置く。
import gleam/option
import gleam/set
import gleam_raft/action
import gleam_raft/cluster_config
import gleam_raft/harness
import gleam_raft/internal/node
import gleam_raft/log
import gleam_raft/message
import gleam_raft/types
import gleeunit/should

// 自作テスト: M4-1 のハーネス煙テスト。three_node_cluster → init_cluster →
// propose_command の全流れで、ハーネスの検証関数が正しく働くことを確認する。
pub fn harness_smoke_init_cluster_and_commit_test() -> Nil {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()
  should.equal(node.role(cluster.node0), types.Leader)
  should.equal(node.role(cluster.node1), types.Follower)
  should.equal(node.role(cluster.node2), types.Follower)
  let #(cluster, position) = harness.propose_command(cluster, "c")
  should.equal(node.commit_index(cluster.node0), position.index)
  should.equal(node.commit_index(cluster.node1), 2)
  should.equal(node.commit_index(cluster.node2), 2)
  // Action はすべて検証済みで、どのノードにも残っていない。
  let #(_, actions) = harness.drain(cluster.node0)
  let assert [] = harness.assert_no_action(actions)
  let #(_, actions) = harness.drain(cluster.node1)
  let assert [] = harness.assert_no_action(actions)
  let #(_, actions) = harness.drain(cluster.node2)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn single_node_start_test() -> Nil {
  let _node = harness.asserted_start(0, [0])
  Nil
}

pub fn create_two_nodes_cluster_test() -> Nil {
  let initial_voters = [0, 1]
  let node0_ = harness.asserted_start(0, initial_voters)
  let node1_ = harness.asserted_start(1, [])

  // クラスタ形成。
  let n0 = node.handle_election_timeout(node0_)
  should.equal(node.role(n0), types.Candidate)
  let #(n0, actions) = harness.drain(n0)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let actions = harness.assert_action(actions, action.SaveCurrentTerm)
  let actions = harness.assert_action(actions, action.SaveVotedFor)
  let rvc =
    message.RequestVoteCall(
      term: node.current_term(n0),
      from: node.id(n0),
      last_log_position: log.log_last_position(node.log(n0)),
    )
  let actions = harness.assert_action(actions, action.BroadcastMessage(rvc))
  let assert [] = harness.assert_no_action(actions)

  let #(n1, reply) =
    harness.asserted_handle_request_vote_call_success(node1_, rvc)
  let #(n0, call) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(n0, reply)
  let #(n1, reply) =
    harness.asserted_handle_append_entries_call_failure(n1, call)
  let #(n0, retry) =
    harness.asserted_handle_append_entries_reply_failure(n0, reply)

  should.be_false(cluster_config.is_joint_consensus(node.config(n0)))
  should.equal(node.config(n0).voters, set.from_list(initial_voters))
  should.equal(cluster_config.unique_nodes(node.config(n1)), [])

  let #(n1, reply) =
    harness.asserted_handle_append_entries_call_success(n1, retry)
  let n0 =
    harness.asserted_handle_append_entries_reply_success(n0, reply, True, False)
  should.equal(node.config(n0), node.config(n1))
}

pub fn create_three_nodes_cluster_test() -> Nil {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()

  should.be_false(cluster_config.is_joint_consensus(node.config(cluster.node0)))
  should.equal(node.config(cluster.node0), node.config(cluster.node1))
  should.equal(node.config(cluster.node0), node.config(cluster.node2))
}

pub fn self_request_vote_call_is_ignored_test() -> Nil {
  let node_ = harness.asserted_start(0, [0, 1])
  should.equal(node.role(node_), types.Candidate)

  let prev_term = node.current_term(node_)
  let prev_voted_for = node.voted_for(node_)
  let prev_role = node.role(node_)
  let msg =
    message.RequestVoteCall(
      term: prev_term,
      from: node.id(node_),
      last_log_position: log.log_last_position(node.log(node_)),
    )

  let assert Ok(node_) = node.handle_message(node_, msg)

  should.equal(node.current_term(node_), prev_term)
  should.equal(node.voted_for(node_), prev_voted_for)
  should.equal(node.role(node_), prev_role)
  let #(_, actions) = harness.drain(node_)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

// gleam core の could_be_disruptive_request_vote は runner 向けの観測 API で、
// core 内部の prefilter は存在しない（DESIGN D 群）。同一入力で判定 true の RVC が
// 判定 true の RVC が通常処理されて投票されることを検証する。
pub fn disruptive_request_vote_is_processed_without_prefilter_test() -> Nil {
  let base = node.start(0)
  let node_ = node.restart(0, 2, option.Some(1), node.log(base))
  let #(node_, actions) = harness.drain(node_)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  let msg =
    message.RequestVoteCall(
      term: 3,
      from: 2,
      last_log_position: log.log_last_position(node.log(node_)),
    )
  should.be_true(node.could_be_disruptive_request_vote(node_, msg))

  let assert Ok(node_) = node.handle_message(node_, msg)

  should.equal(node.role(node_), types.Follower)
  should.equal(node.current_term(node_), 3)
  should.equal(node.voted_for(node_), option.Some(2))
  let #(_, actions) = harness.drain(node_)
  let actions = harness.assert_action(actions, action.SaveCurrentTerm)
  let actions = harness.assert_action(actions, action.SaveVotedFor)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let #(actions, sent) = harness.assert_send_message(actions, 2)
  let assert message.RequestVoteReply(term: 3, vote_granted: True, ..) = sent
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn high_term_candidate_with_stale_log_is_rejected_test() -> Nil {
  // follower のログ末尾は (term=5, index=4)。
  let entries =
    log.entries_from_list(types.zero_position, [
      log.TermEntry(term: 1),
      log.CommandEntry(command: ""),
      log.TermEntry(term: 5),
      log.CommandEntry(command: ""),
    ])
  should.equal(entries.last_position, types.position(5, 4))

  let log_ = log.Log(snapshot_config: cluster_config.config_new(), entries:)
  let node_ = node.restart(0, 5, option.None, log_)
  let #(node_, actions) = harness.drain(node_)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  // candidate はより高い term だがログ末尾は (term=3, index=100) —
  // index では長く term では古い。follower は拒否しなければならない。
  let stale_but_long = types.position(3, 100)
  let msg =
    message.RequestVoteCall(term: 7, from: 1, last_log_position: stale_but_long)

  let assert Ok(node_) = node.handle_message(node_, msg)

  // 高 term の term 更新は勝つが、投票してはいけない。
  // vote_granted: true の SendMessage が無いことを検証する。
  // ここでは拒否返信そのものを内容検証する。
  should.equal(node.current_term(node_), 7)
  should.equal(node.role(node_), types.Follower)
  should.equal(node.voted_for(node_), option.None)
  let #(_, actions) = harness.drain(node_)
  // log 劣位での拒否は投票返信を送らないので SendMessage は積まれない。
  let actions = harness.assert_action(actions, action.SaveCurrentTerm)
  let actions = harness.assert_action(actions, action.SaveVotedFor)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn election_test() -> Nil {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()

  // 新しい選挙のトリガ。
  let #(node1, _call) =
    harness.asserted_follower_election_timeout(cluster.node1)
  let #(node2, _call) =
    harness.asserted_follower_election_timeout(cluster.node2)
  let #(node1, call) = harness.asserted_candidate_election_timeout(node1)

  let #(node2, reply) =
    harness.asserted_handle_request_vote_call_success(node2, call)

  let #(node1, call) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      node1,
      reply,
    )
  let #(node2, reply_from_node2) =
    harness.asserted_handle_append_entries_call_success(node2, call)
  let #(node0, reply_from_node0) =
    harness.asserted_handle_append_entries_call_success_new_leader(
      cluster.node0,
      call,
    )

  let node1 =
    harness.asserted_handle_append_entries_reply_success(
      node1,
      reply_from_node0,
      True,
      False,
    )
  let node1 =
    harness.asserted_handle_append_entries_reply_success(
      node1,
      reply_from_node2,
      False,
      False,
    )

  // 手動ハートビート。
  let #(node1, call) = harness.asserted_heartbeat(node1)
  let #(_node0, reply) =
    harness.asserted_handle_append_entries_call_success(node0, call)
  let assert Ok(node1) = node.handle_message(node1, reply)
  let #(_, actions) = harness.drain(node1)
  let assert [] = harness.assert_no_action(actions)

  // 周期ハートビート。
  let node1 = node.handle_election_timeout(node1)
  let call =
    harness.append_entries_call(
      node1,
      log.entries_new(log.log_last_position(node.log(node1))),
    )
  let #(node1, actions) = harness.drain(node1)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let actions = harness.assert_action(actions, action.BroadcastMessage(call))
  let assert [] = harness.assert_no_action(actions)

  let #(_node2, reply) =
    harness.asserted_handle_append_entries_call_success(node2, call)
  let assert Ok(node1) = node.handle_message(node1, reply)
  let #(_, actions) = harness.drain(node1)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn solo_voter_with_non_voter_yields_log_append_before_committed_broadcast_test() -> Nil {
  let leader = harness.asserted_start(0, [0])
  let config = node.config(leader)
  let config =
    cluster_config.ClusterConfig(
      ..config,
      non_voters: set.insert(config.non_voters, 1),
    )
  let #(leader, proposed) = node.propose_config(leader, config)
  // 前条件違反の番兵値は D1 で Result に化けたので、Ok がそのまま
  // 「INVALID ではない」の主張を兼ねる。
  let assert Ok(position) = proposed
  should.equal(node.commit_index(leader), position.index)

  // 期待は [SetElectionTimeout, AppendLogEntries, BroadcastMessage] の
  // 配列厳密一致。drain 済みリストの完全一致で検証する。
  let #(_, actions) = harness.drain(leader)
  let assert [
    action.SetElectionTimeout,
    action.AppendLogEntries(log_entries),
    action.BroadcastMessage(broadcast),
  ] = actions
  let assert message.AppendEntriesCall(leader_commit:, entries:, ..) = broadcast
  should.equal(log_entries.last_position, position)
  should.equal(leader_commit, position.index)
  should.equal(entries.last_position, position)
  Nil
}

pub fn restart_test() -> Nil {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()
  let #(cluster, _position) = harness.propose_command(cluster, "c1")

  // node1 を再起動する（項・voted_for・ログから復元し、
  // commit_index は 0 に巻き戻る）。
  should.equal(node.role(cluster.node1), types.Follower)
  let node1 =
    node.restart(
      node.id(cluster.node1),
      node.current_term(cluster.node1),
      node.voted_for(cluster.node1),
      node.log(cluster.node1),
    )
  let cluster =
    harness.ThreeNodeCluster(node0: cluster.node0, node1:, node2: cluster.node2)
  // 再起動後のノードも含めて次の提案が commit まで到達することを確認する。
  let #(cluster, position) = harness.propose_command(cluster, "c2")
  should.equal(node.commit_index(cluster.node0), position.index)
  Nil
}

pub fn truncate_log_test() -> Nil {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()
  let #(cluster, _position) = harness.propose_command(cluster, "c1")
  should.equal(node.role(cluster.node0), types.Leader)

  // 1 件提案するが、メッセージはブロードキャストしない。
  let #(node0, proposed) = node.propose_command(cluster.node0, "c2")
  let assert option.Some(commit_position) = proposed
  should.equal(commit_position, node.log(node0).entries.last_position)
  // 内容を見ずに消費だけする
  // （積まれる [SET, AE, Broadcast] は意図通り。空の主張は後で行う）。
  let #(node0, _actions) = harness.drain(node0)

  // node2 を leader にする。
  let #(node2, _call) =
    harness.asserted_follower_election_timeout(cluster.node2)
  let #(node2, call) = harness.asserted_candidate_election_timeout(node2)

  // 呼び出し側は dispersive な RequestVote を見送れる（見送り判断だけを見て
  // 実際には処理しないので node0 は leader のまま、Action も積まない）。
  should.equal(node.could_be_disruptive_request_vote(node0, call), True)
  should.equal(node.role(node0), types.Leader)
  let #(_node0, actions) = harness.drain(node0)
  let assert [] = harness.assert_no_action(actions)

  // node1 のログ index は node2 と等しいので投票する。
  let #(node1, _call) =
    harness.asserted_follower_election_timeout(cluster.node1)
  let #(node1, reply) =
    harness.asserted_handle_request_vote_call_success(node1, call)
  let #(node2, call) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      node2,
      reply,
    )
  should.equal(node.role(node2), types.Leader)

  // node0 の未 commit ログが切り詰められる。
  let #(node0, reply) =
    harness.asserted_handle_append_entries_call_success(node0, call)
  should.equal(node.get_commit_status(node0, commit_position), types.InProgress)
  let node2 =
    harness.asserted_handle_append_entries_reply_success(
      node2,
      reply,
      True,
      False,
    )

  let #(node2, call) = harness.asserted_heartbeat(node2)
  let #(node0, _reply) =
    harness.asserted_handle_append_entries_call_success(node0, call)
  should.equal(node.get_commit_status(node0, commit_position), types.Rejected)

  let #(_, actions) = harness.drain(node0)
  let assert [] = harness.assert_no_action(actions)
  let #(_, actions) = harness.drain(node1)
  let assert [] = harness.assert_no_action(actions)
  let #(_, actions) = harness.drain(node2)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn same_index_different_term_tail_is_truncated_before_replication_test() -> Nil {
  let config = harness.joint([0, 1, 2], [])
  let leader_prefix =
    log.entries_from_list(types.zero_position, [
      log.ConfigEntry(config:),
      log.TermEntry(term: 2),
      log.CommandEntry(command: ""),
    ])
  let leader =
    node.restart(
      0,
      3,
      option.None,
      log.Log(
        snapshot_config: cluster_config.config_new(),
        entries: leader_prefix,
      ),
    )
  let #(leader, actions) = harness.drain(leader)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  let #(leader, _call) = harness.asserted_follower_election_timeout(leader)
  should.equal(node.current_term(leader), 4)
  let vote_reply =
    message.RequestVoteReply(term: 4, from: 2, vote_granted: True)
  let #(leader, _initial_append) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      leader,
      vote_reply,
    )
  should.equal(node.role(leader), types.Leader)
  should.equal(node.log(leader).entries.last_position, harness.pos(4, 4))

  // 末尾だけ別 term に進んだフォロワ（同 index 異 term）。
  let follower =
    node.restart(
      1,
      3,
      option.None,
      log.Log(
        snapshot_config: cluster_config.config_new(),
        entries: log.entries_from_list(types.zero_position, [
          log.ConfigEntry(config:),
          log.TermEntry(term: 2),
          log.CommandEntry(command: ""),
          log.TermEntry(term: 3),
        ]),
      ),
    )
  let #(follower, actions) = harness.drain(follower)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)
  should.equal(node.log(follower).entries.last_position, harness.pos(3, 4))

  let divergent_reply =
    message.AppendEntriesReply(
      from: node.id(follower),
      term: node.current_term(leader),
      last_log_position: node.log(follower).entries.last_position,
    )
  let #(leader, truncate_call) =
    harness.asserted_handle_append_entries_reply_failure(
      leader,
      divergent_reply,
    )
  let assert message.AppendEntriesCall(entries:, ..) = truncate_call
  should.equal(log.entries_is_empty(entries), True)
  should.equal(entries.prev_position, harness.pos(4, 4))
  should.equal(entries.last_position, harness.pos(4, 4))

  let #(follower, truncated_reply) =
    harness.asserted_handle_append_entries_call_failure(follower, truncate_call)
  should.equal(node.log(follower).entries.last_position, harness.pos(2, 3))

  // 切り詰め報告に対する修復 Call は共通 prefix 越しの 1 語だけ。
  let assert Ok(leader) = node.handle_message(leader, truncated_reply)
  let #(leader, actions) = harness.drain(leader)
  let #(actions, repair_call) = harness.assert_send_message(actions, 1)
  let assert [] = harness.assert_no_action(actions)
  let assert message.AppendEntriesCall(entries:, ..) = repair_call
  should.equal(entries.prev_position, harness.pos(2, 3))
  should.equal(entries.last_position, harness.pos(4, 4))
  should.equal(entries.entries, [log.TermEntry(term: 4)])

  let #(follower, repaired_reply) =
    harness.asserted_handle_append_entries_call_success(follower, repair_call)
  should.equal(
    node.log(follower).entries.last_position,
    node.log(leader).entries.last_position,
  )

  let leader =
    harness.asserted_handle_append_entries_reply_success(
      leader,
      repaired_reply,
      True,
      False,
    )
  should.equal(node.commit_index(leader), 4)
  Nil
}

pub fn follower_rewrites_from_common_prefix_when_repair_term_is_outside_local_log_test() -> Nil {
  let follower =
    node.restart(
      1,
      4,
      option.Some(0),
      log.Log(
        snapshot_config: cluster_config.config_new(),
        entries: log.entries_from_list(types.zero_position, [
          log.TermEntry(term: 1),
          log.CommandEntry(command: ""),
          log.CommandEntry(command: ""),
          log.TermEntry(term: 3),
        ]),
      ),
    )
  let #(follower, actions) = harness.drain(follower)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  // 修復 Call の断片はローカルログの項範囲（項 1〜3）に収まらない項 2 を
  // 運んでくる。anchor (1, 3) 越しに書換わることを確認する。
  let repair_entries =
    log.entries_from_list(harness.pos(1, 3), [
      log.CommandEntry(command: ""),
      log.CommandEntry(command: ""),
      log.TermEntry(term: 2),
    ])
  let repair_call =
    message.AppendEntriesCall(
      from: 0,
      term: 4,
      leader_commit: 6,
      entries: repair_entries,
    )
  let assert Ok(follower) = node.handle_message(follower, repair_call)
  let reply = harness.append_entries_reply(repair_call, follower)
  should.equal(node.commit_index(follower), 6)
  should.equal(log.entries_iter_with_positions(node.log(follower).entries), [
    #(harness.pos(1, 1), log.TermEntry(term: 1)),
    #(harness.pos(1, 2), log.CommandEntry(command: "")),
    #(harness.pos(1, 3), log.CommandEntry(command: "")),
    #(harness.pos(1, 4), log.CommandEntry(command: "")),
    #(harness.pos(1, 5), log.CommandEntry(command: "")),
    #(harness.pos(2, 6), log.TermEntry(term: 2)),
  ])

  let #(_follower, actions) = harness.drain(follower)
  let actions =
    harness.assert_action(actions, action.AppendLogEntries(repair_entries))
  let #(actions, sent) = harness.assert_send_message(actions, 0)
  should.equal(sent, reply)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn candidate_accepts_same_term_append_entries_from_leader_test() -> Nil {
  let config = harness.joint([0, 1, 2], [])
  let prefix =
    log.entries_from_list(types.zero_position, [
      log.ConfigEntry(config:),
      log.TermEntry(term: 2),
      log.CommandEntry(command: ""),
    ])
  let empty_log = fn(entries: log.LogEntries(String)) -> log.Log(String) {
    log.Log(snapshot_config: cluster_config.config_new(), entries:)
  }
  let leader = node.restart(0, 3, option.None, empty_log(prefix))
  let #(leader, actions) = harness.drain(leader)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  let #(leader, _call) = harness.asserted_follower_election_timeout(leader)
  let vote_reply =
    message.RequestVoteReply(term: 4, from: 2, vote_granted: True)
  let #(leader, append_entries) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      leader,
      vote_reply,
    )
  should.equal(node.role(leader), types.Leader)

  let candidate = node.restart(1, 3, option.None, empty_log(prefix))
  let #(candidate, actions) = harness.drain(candidate)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  let #(candidate, _call) =
    harness.asserted_follower_election_timeout(candidate)
  should.equal(node.role(candidate), types.Candidate)
  should.equal(node.current_term(candidate), node.current_term(leader))

  // 同項の leader の AE Call を candidate が受理して follower に下りる。
  let #(candidate, _reply) =
    harness.asserted_handle_append_entries_call_success(
      candidate,
      append_entries,
    )
  should.equal(node.role(candidate), types.Follower)
  should.equal(node.voted_for(candidate), option.Some(node.id(leader)))
  should.equal(
    node.log(candidate).entries.last_position,
    node.log(leader).entries.last_position,
  )
  Nil
}

pub fn follower_accepts_same_term_append_entries_after_voting_for_another_candidate_test() -> Nil {
  let config = harness.joint([0, 1, 2, 3, 4], [])
  let prefix =
    log.entries_from_list(types.zero_position, [
      log.ConfigEntry(config:),
      log.TermEntry(term: 2),
      log.CommandEntry(command: ""),
    ])
  let empty_log = fn(entries: log.LogEntries(String)) -> log.Log(String) {
    log.Log(snapshot_config: cluster_config.config_new(), entries:)
  }
  let leader = node.restart(0, 3, option.None, empty_log(prefix))
  let #(leader, actions) = harness.drain(leader)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  let #(leader, _call) = harness.asserted_follower_election_timeout(leader)
  // 1 票目だけでは 5 voter の過半数に届かず candidate のまま。
  let first_vote_reply =
    message.RequestVoteReply(
      term: node.current_term(leader),
      from: 1,
      vote_granted: True,
    )
  let assert Ok(leader) = node.handle_message(leader, first_vote_reply)
  should.equal(node.role(leader), types.Candidate)
  let #(_leader, actions) = harness.drain(leader)
  let assert [] = harness.assert_no_action(actions)

  let vote_reply =
    message.RequestVoteReply(
      term: node.current_term(leader),
      from: 2,
      vote_granted: True,
    )
  let #(leader, append_entries) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      leader,
      vote_reply,
    )
  should.equal(node.role(leader), types.Leader)

  // 別候補に投票済みの follower（voted_for = 4）。
  let follower =
    node.restart(
      3,
      node.current_term(leader),
      option.Some(4),
      empty_log(prefix),
    )
  let #(follower, actions) = harness.drain(follower)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)
  should.equal(node.role(follower), types.Follower)
  should.not_equal(node.voted_for(follower), option.Some(node.id(leader)))

  // 同項の AE Call 受理で voted_for が leader に書き換わる。
  let #(follower, _reply) =
    harness.asserted_handle_append_entries_call_success(
      follower,
      append_entries,
    )
  should.equal(node.role(follower), types.Follower)
  should.equal(node.voted_for(follower), option.Some(node.id(leader)))
  should.equal(
    node.log(follower).entries.last_position,
    node.log(leader).entries.last_position,
  )
  Nil
}

pub fn snapshot_preserves_pending_append_suffix_after_snapshot_position_test() {
  let leader = harness.asserted_start(0, [0])
  // 生 propose を 2 回（harness.propose_command は応答給餌付きなので使わない）
  let #(leader, first) = node.propose_command(leader, "c1")
  let assert option.Some(first_position) = first
  let #(leader, second) = node.propose_command(leader, "c2")
  let assert option.Some(second_position) = second
  // 単独 voter は提案が即 commit に届く
  should.equal(node.commit_index(leader), second_position.index)
  // 同種マージで連続断片が連結された保留 AE（固定点: 同一種のAppendLogEntries は連結）
  let merged =
    log.entries_from_list(harness.log_prev(first_position), [
      log.CommandEntry(command: "c1"),
      log.CommandEntry(command: "c2"),
    ])
  let #(leader, actions) = harness.drain(leader)
  let _actions = harness.assert_action(actions, action.AppendLogEntries(merged))
  // 保留 AE スロットを覗いたまま install し install 側の「スロット置換
  // による非連続断片」を主張する検証。gleam の drain は破壊的なので set_actions
  // シーム（DESIGN §3 例外注記。M6 で @internal 化済み。除去するときこの
  // テストも運命を共にする）で再注入してから install して同じ遷移を再現する。
  let leader =
    node.set_actions(
      leader,
      action.Actions(
        ..action.actions_empty(),
        append_log_entries: option.Some(merged),
      ),
    )
  let #(leader, accepted) =
    node.handle_snapshot_installed(leader, first_position, node.config(leader))
  should.be_true(accepted)
  // commit 非後退: 境界はすでに commit 済みなので install で下がらない
  should.equal(node.commit_index(leader), second_position.index)
  let entries = node.log(leader).entries
  should.equal(entries.prev_position, first_position)
  should.equal(entries.last_position, second_position)
  // 置換後の保留 AE は suffix 断片だけ
  let #(_leader, actions) = harness.drain(leader)
  let suffix =
    log.entries_from_list(first_position, [log.CommandEntry(command: "c2")])
  let _actions = harness.assert_action(actions, action.AppendLogEntries(suffix))
  // スロット内容のみを主張するので他 kind は不問（assert_no_action しない）
  Nil
}

pub fn snapshot_discards_incompatible_pending_append_entries_test() {
  // install は current_term を超える項を拒否するので項 12 で起動する
  let empty_config = harness.voters_config([])
  let follower =
    node.restart(
      0,
      12,
      option.None,
      log.Log(
        snapshot_config: empty_config,
        entries: log.entries_new(types.zero_position),
      ),
    )
  // 内容を見ずに消費だけする
  let #(follower, _actions) = harness.drain(follower)
  // 保留 AE スロットへシームで直接注入する。
  // prev がスナップショット境界と非連続なので install 時に破棄される。
  let incompatible =
    log.entries_from_list(harness.pos(10, 2), [log.CommandEntry(command: "c")])
  let follower =
    node.set_actions(
      follower,
      action.Actions(
        ..action.actions_empty(),
        append_log_entries: option.Some(incompatible),
      ),
    )
  let snapshot_position = harness.pos(12, 2)
  let #(follower, accepted) =
    node.handle_snapshot_installed(follower, snapshot_position, empty_config)
  should.be_true(accepted)
  // 新規 follower は commit 0 なので install が境界まで進める
  should.equal(node.commit_index(follower), snapshot_position.index)
  should.equal(node.log(follower).entries.prev_position, snapshot_position)
  let #(_follower, actions) = harness.drain(follower)
  let assert [] = harness.assert_no_action(actions)
  Nil
}

pub fn local_snapshot_mismatch_returns_error_test() {
  let snapshot_position = harness.pos(1, 5)
  let follower =
    node.restart(
      0,
      2,
      option.Some(1),
      log.Log(
        snapshot_config: harness.voters_config([]),
        entries: log.entries_from_list(snapshot_position, [
          log.CommandEntry(command: "c"),
        ]),
      ),
    )
  let #(follower, actions) = harness.drain(follower)
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)
  let call =
    message.AppendEntriesCall(
      from: 1,
      term: 2,
      leader_commit: 5,
      entries: log.entries_from_list(harness.pos(2, 5), [
        log.CommandEntry(command: "c"),
      ]),
    )
  // D1 の型付きエラー: 「Err 後もログ不変・
  // Action 空」の 2 主張は、gleam では handle_message が Error 時にノードを
  // 返さない値意味論で自動成立するため、エラー値一致 1 件に集約する。
  should.equal(
    node.handle_message(follower, call),
    Error(types.LocalSnapshotMismatch),
  )
  Nil
}

/// snapshot 注入ヘルパ。3 ノードループの本体で、install 前後の anchor と
/// commit 境界の不変条件を検証して更新後ノードを返す。
fn install_at_snapshot_boundary(node_: node.Node(String)) -> node.Node(String) {
  let entries = node.log(node_).entries
  should.equal(entries.prev_position.index, 0)
  let boundary = entries.last_position
  let #(node_, accepted) =
    node.handle_snapshot_installed(node_, boundary, node.config(node_))
  should.be_true(accepted)
  let entries = node.log(node_).entries
  should.not_equal(entries.prev_position.index, 0)
  // 「全ノードが末尾まで commit 済み」とは限らない。gleam の
  // init_cluster は非対称（node1 は commit 2）でも、install の前進同期で
  // 同じ主張が成立する。
  should.equal(node.commit_index(node_), boundary.index)
  node_
}

pub fn snapshot_test() {
  let cluster =
    harness.three_node_cluster()
    |> harness.init_cluster()
  let #(cluster, _position) = harness.propose_command(cluster, "c")
  should.equal(node.role(cluster.node0), types.Leader)

  // スナップショットを撮る
  let node0 = install_at_snapshot_boundary(cluster.node0)
  let node1 = install_at_snapshot_boundary(cluster.node1)
  let node2 = install_at_snapshot_boundary(cluster.node2)

  // 新しいノードを加え、2 ノードを除名する
  let config = harness.joint([0, 1, 2], [0, 3])
  let #(node0, call) = harness.asserted_change_cluster_config(node0, config)
  let #(_node1, reply) =
    harness.asserted_handle_append_entries_call_success(node1, call)
  let node0 =
    harness.asserted_handle_append_entries_reply_success(
      node0,
      reply,
      False,
      False,
    )
  let #(_node2, reply) =
    harness.asserted_handle_append_entries_call_success(node2, call)
  let node0 =
    harness.asserted_handle_append_entries_reply_success(
      node0,
      reply,
      False,
      False,
    )

  // 追従不能（snapshot が必要）
  let node3 = harness.asserted_start(3, [])
  let #(node3, reply) =
    harness.asserted_handle_append_entries_call_failure(node3, call)
  let #(node0, #(snapshot_config, snapshot_position)) =
    harness.asserted_handle_append_entries_reply_failure_need_snapshot(
      node0,
      reply,
    )
  let #(node3, accepted) =
    node.handle_snapshot_installed(node3, snapshot_position, snapshot_config)
  should.be_true(accepted)
  // snapshot install で参加した新規ノードは commit が境界に着地する
  should.equal(node.commit_index(node3), snapshot_position.index)

  // snapshot の後の追記
  let #(node0, heartbeat) = harness.asserted_heartbeat(node0)
  let #(node3, reply) =
    harness.asserted_handle_append_entries_call_failure(node3, heartbeat)
  let #(node0, repair) =
    harness.asserted_handle_append_entries_reply_failure(node0, reply)
  let #(_node3, reply) =
    harness.asserted_handle_append_entries_call_success(node3, repair)
  let #(_node0, _reply) =
    harness.asserted_handle_append_entries_reply_success_with_joint_config_committed(
      node0,
      reply,
    )
  Nil
}

pub fn leader_commits_after_shrinking_to_self_only_voter_test() -> Nil {
  // 元テストの選挙部は生 handle_election_timeout と 4 種の assert だが、
  // asserted_candidate_election_timeout は同一 4 kind + 他なしを主張する
  // 上位セット（term +1 と voted_for の追加検証つき）なので helper で書く。
  let leader = harness.asserted_start(0, [0, 1])
  let follower = harness.asserted_start(1, [])
  let #(leader, call) = harness.asserted_candidate_election_timeout(leader)
  let #(follower, reply) =
    harness.asserted_handle_request_vote_call_success(follower, call)
  let #(leader, call) =
    harness.asserted_handle_request_vote_reply_majority_vote_granted(
      leader,
      reply,
    )
  let #(follower, reply) =
    harness.asserted_handle_append_entries_call_failure(follower, call)
  let #(leader, call) =
    harness.asserted_handle_append_entries_reply_failure(leader, reply)
  let #(follower, reply) =
    harness.asserted_handle_append_entries_call_success(follower, call)
  let leader =
    harness.asserted_handle_append_entries_reply_success(
      leader,
      reply,
      True,
      False,
    )
  should.equal(node.config(leader), node.config(follower))

  // follower を voter から除く joint 化（adding 空・removing [1]）。
  // 前条件違反は panic 相当ではなく、D1 に従い Bool を返すのでここで明示検証する。
  let #(joint, valid) =
    cluster_config.to_joint_consensus(node.config(leader), [], [1])
  should.be_true(valid)
  let #(leader, call) = harness.asserted_change_cluster_config(leader, joint)
  let #(_follower, reply) =
    harness.asserted_handle_append_entries_call_success(follower, call)

  // ここは元テスト同様 helper を使わず handle_message へ生給餌し、縮小の
  // 完了を Action と合わせて検証する。joint entry の commit で finalize が
  // 即提案され、自己のみ voter の quorum ですぐ commit されるので
  // commit_index が log 末尾に追いつく（固定点 10 の前進側）。
  let final_config_prev_position = node.log(leader).entries.last_position
  let assert Ok(leader) = node.handle_message(leader, reply)
  let final_config = node.config(leader)
  should.equal(final_config.voters, set.from_list([0]))
  should.be_false(cluster_config.is_joint_consensus(final_config))
  should.equal(
    node.commit_index(leader),
    node.log(leader).entries.last_position.index,
  )
  // drain が返す「Action を消費したノード」を後段の propose に引き継ぐ。
  // drain 前のノードを使うと finalize の AE が保持されたままマージされる。
  let #(leader, actions) = harness.drain(leader)
  let finalize =
    log.entries_from_list(final_config_prev_position, [
      log.ConfigEntry(final_config),
    ])
  let actions =
    harness.assert_action(actions, action.AppendLogEntries(finalize))
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)

  // 自己のみ voter では提案が append と同時に commit される。
  let #(leader, proposed) = node.propose_command(leader, "c")
  let assert option.Some(position) = proposed
  should.equal(node.commit_index(leader), position.index)
  let #(_leader, actions) = harness.drain(leader)
  let command_entries =
    log.entries_from_list(harness.log_prev(position), [
      log.CommandEntry(command: "c"),
    ])
  let actions =
    harness.assert_action(actions, action.AppendLogEntries(command_entries))
  let actions = harness.assert_action(actions, action.SetElectionTimeout)
  let assert [] = harness.assert_no_action(actions)
  Nil
}
