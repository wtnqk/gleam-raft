// DESIGN §11.2 のシナリオハーネス共通部品。gleam の node は純関数で観測
// 関数も公開されているため、ラッパ型は持たず関数群として実装する。
//
// 実装上の取り決め（実施記録に注記済み）:
// - `assert_action` は同種の Action だけを先頭から取り出し、内容まで一致を
//   検証する。取り出し順は優先度ではなく、テストが検証する種類と内容の
//   一致を見る。
// - Action 集合は Node が一元的に持つので、取り出し用の写しは持たない。
import gleam/int
import gleam/option.{None, Some}
import gleam/set

import gleam_raft/action.{type Action}
import gleam_raft/cluster_config.{type ClusterConfig, is_joint_consensus}
import gleam_raft/internal/node.{type Node}
import gleam_raft/log.{
  type LogEntries, entries_from_list, entries_is_empty, entries_since,
  log_last_position,
}
import gleam_raft/message.{
  type Message, AppendEntriesCall, AppendEntriesReply, RequestVoteCall,
  RequestVoteReply,
}
import gleam_raft/types.{
  type LogPosition, type NodeId, Candidate, Follower, Leader, position,
}
import gleeunit/should

// ---- 位置・構成・メッセージの生成ヘルパー ----

/// `types.position` の別名。
pub fn pos(term: Int, index: Int) -> LogPosition {
  position(term, index)
}

/// 1 つ前の位置。境界下位をどう作るかはテスト側の責務なので、素の
/// 減算だけ行う。
pub fn log_prev(p: LogPosition) -> LogPosition {
  position(p.term, p.index - 1)
}

/// 旧・新投票者から joint 構成を作る。
pub fn joint(old: List(NodeId), new: List(NodeId)) -> ClusterConfig {
  cluster_config_new(set.from_list(old), set.from_list(new), set.new())
}

/// 投票者だけを持つ構成。
pub fn voters_config(voters: List(NodeId)) -> ClusterConfig {
  cluster_config_new(set.from_list(voters), set.new(), set.new())
}

fn cluster_config_new(
  voters: set.Set(NodeId),
  new_voters: set.Set(NodeId),
  non_voters: set.Set(NodeId),
) -> ClusterConfig {
  cluster_config.ClusterConfig(voters:, new_voters:, non_voters:)
}

/// 単一エントリの追記 Action。
pub fn append_log_entry(
  prev: LogPosition,
  entry: log.LogEntry(command),
) -> Action(command) {
  action.AppendLogEntries(entries_from_list(prev, [entry]))
}

/// leader の現在状態から作った AE Call。
pub fn append_entries_call(
  leader: Node(command),
  entries: LogEntries(command),
) -> Message(command) {
  AppendEntriesCall(
    term: node.current_term(leader),
    from: node.id(leader),
    leader_commit: node.commit_index(leader),
    entries:,
  )
}

/// AE Call への返信を node の現在状態から作る。
pub fn append_entries_reply(
  call: Message(command),
  node_: Node(command),
) -> Message(command) {
  let assert AppendEntriesCall(..) = call
  AppendEntriesReply(
    term: node.current_term(node_),
    from: node.id(node_),
    last_log_position: log_last_position(node.log(node_)),
  )
}

// ---- Action キューの検証 primitive ----

/// 全 Action を take_action の優先度順に消費し尽くす。
pub fn drain(node_: Node(command)) -> #(Node(command), List(Action(command))) {
  case node.take_action(node_) {
    #(node_, None) -> #(node_, [])
    #(node_, Some(taken)) -> {
      let #(node_, rest) = drain(node_)
      #(node_, [taken, ..rest])
    }
  }
}

fn same_kind(a: Action(command), b: Action(command)) -> Bool {
  case a, b {
    action.SetElectionTimeout, action.SetElectionTimeout -> True
    action.SaveCurrentTerm, action.SaveCurrentTerm -> True
    action.SaveVotedFor, action.SaveVotedFor -> True
    action.AppendLogEntries(..), action.AppendLogEntries(..) -> True
    action.BroadcastMessage(..), action.BroadcastMessage(..) -> True
    action.SendMessage(to: ta, ..), action.SendMessage(to: tb, ..) -> ta == tb
    action.InstallSnapshot(to: ta), action.InstallSnapshot(to: tb) -> ta == tb
    _, _ -> False
  }
}

/// `expected` と同種の Action を先頭で取り除き、内容まで一致を検証する。
/// 同種がなければ失敗、内容が違えば失敗する。
pub fn assert_action(
  actions: List(Action(command)),
  expected: Action(command),
) -> List(Action(command)) {
  case actions {
    [] -> panic as "assert_action: 期待された Action がキューにありません"
    [first, ..rest] ->
      case same_kind(first, expected) {
        True -> {
          should.equal(first, expected)
          rest
        }
        False -> {
          let rest = assert_action(rest, expected)
          [first, ..rest]
        }
      }
  }
}

/// SendMessage の中身を内容指定なしで取り出す。無ければ失敗。
pub fn assert_send_message(
  actions: List(Action(command)),
  to: NodeId,
) -> #(List(Action(command)), Message(command)) {
  case actions {
    [] -> panic as "assert_send_message: 宛先への SendMessage がありません"
    [first, ..rest] ->
      case first {
        action.SendMessage(to: t, message: msg) if t == to -> #(rest, msg)
        _ -> {
          let #(rest, msg) = assert_send_message(rest, to)
          #([first, ..rest], msg)
        }
      }
  }
}

/// 残 Action が空であることを検証する。
pub fn assert_no_action(
  actions: List(Action(command)),
) -> List(Action(command)) {
  should.equal(actions, [])
  actions
}

// ---- asserted_* 群（単一ノード操作・応答検証） ----

/// 新規起動とクラスタ形成を検証しながら行い、Action を消費し尽くした
/// ノードを返す。
pub fn asserted_start(
  id: NodeId,
  initial_voters: List(NodeId),
) -> Node(command) {
  let started = node.start(id)
  should.equal(node.role(started), Follower)
  should.equal(node.current_term(started), 0)
  should.equal(node.voted_for(started), None)
  let #(started, actions) = drain(started)
  let assert [] = assert_no_action(actions)
  case initial_voters {
    [] -> started
    voters -> {
      let #(node_, created) = node.create_cluster(started, voters)
      let #(node_, actions) = drain(node_)
      let actions = assert_action(actions, action.SetElectionTimeout)
      let actions = assert_action(actions, action.SaveCurrentTerm)
      let actions = assert_action(actions, action.SaveVotedFor)
      let config = node.config(node_)
      case voters == [id] {
        True -> {
          // 単独 voter は create_cluster のまま leader になり、config entry
          // の追加後に term entry まで積む。返るのは遷移後の最終位置なので
          // (1, 2)。
          should.equal(created, Ok(pos(1, 2)))
          should.equal(node.role(node_), Leader)
          let actions =
            assert_action(
              actions,
              action.AppendLogEntries(
                entries_from_list(types.zero_position, [
                  log.ConfigEntry(config),
                  log.TermEntry(term: 1),
                ]),
              ),
            )
          let assert [] = assert_no_action(actions)
          node_
        }
        False -> {
          // 複数 voter は candidate 止まり。term entry を積まないので
          // 返り値は config entry の位置 (0, 1) のまま。
          should.equal(created, Ok(pos(0, 1)))
          should.equal(node.role(node_), Candidate)
          let actions =
            assert_action(
              actions,
              action.AppendLogEntries(
                entries_from_list(types.zero_position, [log.ConfigEntry(config)]),
              ),
            )
          let rvc =
            RequestVoteCall(
              term: node.current_term(node_),
              from: id,
              last_log_position: log_last_position(node.log(node_)),
            )
          let actions = assert_action(actions, action.BroadcastMessage(rvc))
          let assert [] = assert_no_action(actions)
          node_
        }
      }
    }
  }
}

/// フォロワの選挙タイムアウトを検証し、立候補したノードと RVC を返す。
pub fn asserted_follower_election_timeout(
  node_: Node(command),
) -> #(Node(command), Message(command)) {
  should.equal(node.role(node_), Follower)
  let prev_term = node.current_term(node_)
  let node_ = node.handle_election_timeout(node_)
  should.equal(node.role(node_), Candidate)
  should.equal(node.current_term(node_), prev_term + 1)
  let rvc =
    RequestVoteCall(
      term: node.current_term(node_),
      from: node.id(node_),
      last_log_position: log_last_position(node.log(node_)),
    )
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.SaveCurrentTerm)
  let actions = assert_action(actions, action.SaveVotedFor)
  should.equal(node.voted_for(node_), Some(node.id(node_)))
  let actions = assert_action(actions, action.BroadcastMessage(rvc))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  #(node_, rvc)
}

/// キャニデイトの選挙タイムアウトを検証し、立候補を続けたノードと
/// RVC を返す。
pub fn asserted_candidate_election_timeout(
  node_: Node(command),
) -> #(Node(command), Message(command)) {
  should.equal(node.role(node_), Candidate)
  let prev_term = node.current_term(node_)
  let node_ = node.handle_election_timeout(node_)
  should.equal(node.role(node_), Candidate)
  should.equal(node.current_term(node_), prev_term + 1)
  let rvc =
    RequestVoteCall(
      term: node.current_term(node_),
      from: node.id(node_),
      last_log_position: log_last_position(node.log(node_)),
    )
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.SaveCurrentTerm)
  let actions = assert_action(actions, action.SaveVotedFor)
  should.equal(node.voted_for(node_), Some(node.id(node_)))
  let actions = assert_action(actions, action.BroadcastMessage(rvc))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  #(node_, rvc)
}

/// RVC の受理成功を検証し、投票したノードと投票返信を返す。
pub fn asserted_handle_request_vote_call_success(
  node_: Node(command),
  call: Message(command),
) -> #(Node(command), Message(command)) {
  let assert RequestVoteCall(term: term, from: from, ..) = call
  let assert Ok(node_) = node.handle_message(node_, call)
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.SaveCurrentTerm)
  should.equal(node.current_term(node_), term)
  let actions = assert_action(actions, action.SaveVotedFor)
  should.equal(node.voted_for(node_), Some(from))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let reply = RequestVoteReply(term:, from: node.id(node_), vote_granted: True)
  let actions = assert_action(actions, action.SendMessage(from, reply))
  let assert [] = assert_no_action(actions)
  #(node_, reply)
}

/// 過半数の投票獲得を処理し、就任を検証する。返り値は就任 AE の
/// broadcast。
pub fn asserted_handle_request_vote_reply_majority_vote_granted(
  node_: Node(command),
  reply: Message(command),
) -> #(Node(command), Message(command)) {
  let assert RequestVoteReply(..) = reply
  let tail = log_last_position(node.log(node_))
  let assert Ok(node_) = node.handle_message(node_, reply)
  let term = node.current_term(node_)
  let entries = entries_from_list(tail, [log.TermEntry(term:)])
  let call = append_entries_call(node_, entries)
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.AppendLogEntries(entries))
  let actions = assert_action(actions, action.BroadcastMessage(call))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  #(node_, call)
}

/// AE Call の受理成功を検証し、追記と返信を確かめたノードと返信を
/// 返す。
pub fn asserted_handle_append_entries_call_success(
  node_: Node(command),
  msg: Message(command),
) -> #(Node(command), Message(command)) {
  let assert AppendEntriesCall(
    from: from,
    leader_commit: leader_commit,
    entries: entries,
    ..,
  ) = msg
  let old_role = node.role(node_)
  let prev_commit = node.commit_index(node_)
  let prev_voted_for = node.voted_for(node_)
  let assert Ok(node_) = node.handle_message(node_, msg)
  let #(node_, actions) = drain(node_)
  let last = log_last_position(node.log(node_))
  should.equal(last, entries.last_position)
  let actions = case prev_voted_for == Some(from) {
    True -> actions
    False -> {
      let actions = assert_action(actions, action.SaveVotedFor)
      should.equal(node.voted_for(node_), Some(from))
      actions
    }
  }
  let reply =
    AppendEntriesReply(
      term: node.current_term(node_),
      from: node.id(node_),
      last_log_position: last,
    )
  let actions = case entries_is_empty(entries) {
    True -> actions
    False -> assert_action(actions, action.AppendLogEntries(entries))
  }
  case prev_commit < leader_commit, prev_commit <= last.index {
    True, True ->
      should.equal(node.commit_index(node_), int.min(last.index, leader_commit))
    _, _ -> Nil
  }
  let actions = assert_action(actions, action.SendMessage(from, reply))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let actions = case old_role == Leader {
    True -> assert_action(actions, action.SaveCurrentTerm)
    False -> actions
  }
  let assert [] = assert_no_action(actions)
  #(node_, reply)
}

/// AE Call の受理失敗を検証し、変わらない追記位置と返信を返す。
pub fn asserted_handle_append_entries_call_failure(
  node_: Node(command),
  msg: Message(command),
) -> #(Node(command), Message(command)) {
  let assert AppendEntriesCall(from: from, term: term, entries: entries, ..) =
    msg
  let prev_voted_for = node.voted_for(node_)
  let prev_term = node.current_term(node_)
  let assert Ok(node_) = node.handle_message(node_, msg)
  let #(node_, actions) = drain(node_)
  let last = log_last_position(node.log(node_))
  should.not_equal(last, entries.last_position)
  let actions = case prev_term < term {
    True -> {
      let actions = assert_action(actions, action.SaveCurrentTerm)
      should.equal(node.current_term(node_), term)
      actions
    }
    False -> actions
  }
  let actions = case prev_voted_for == Some(from) {
    True -> actions
    False -> {
      let actions = assert_action(actions, action.SaveVotedFor)
      should.equal(node.voted_for(node_), Some(from))
      actions
    }
  }
  let actions = assert_action(actions, action.SetElectionTimeout)
  let reply =
    AppendEntriesReply(
      term: node.current_term(node_),
      from: node.id(node_),
      last_log_position: last,
    )
  let actions = assert_action(actions, action.SendMessage(from, reply))
  let assert [] = assert_no_action(actions)
  #(node_, reply)
}

/// 新リーダーの AE Call 受理を検証し、項の更新と term entry の追記を
/// 確かめる。
pub fn asserted_handle_append_entries_call_success_new_leader(
  node_: Node(command),
  msg: Message(command),
) -> #(Node(command), Message(command)) {
  let assert AppendEntriesCall(from: from, term: term, ..) = msg
  let tail = log_last_position(node.log(node_))
  let assert Ok(node_) = node.handle_message(node_, msg)
  let reply = append_entries_reply(msg, node_)
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.SaveCurrentTerm)
  should.equal(node.current_term(node_), term)
  let actions = assert_action(actions, action.SaveVotedFor)
  should.equal(node.voted_for(node_), Some(from))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let actions =
    assert_action(actions, append_log_entry(tail, log.TermEntry(term: term)))
  let actions = assert_action(actions, action.SendMessage(from, reply))
  let assert [] = assert_no_action(actions)
  #(node_, reply)
}

/// 失敗返信を処理する。leader が積んだ再送 AE Call を中身込みで
/// 取り出して返す。
pub fn asserted_handle_append_entries_reply_failure(
  node_: Node(command),
  reply: Message(command),
) -> #(Node(command), Message(command)) {
  let assert AppendEntriesReply(from: from, ..) = reply
  let assert Ok(node_) = node.handle_message(node_, reply)
  let #(node_, actions) = drain(node_)
  let #(actions, retry) = assert_send_message(actions, from)
  let assert [] = assert_no_action(actions)
  #(node_, retry)
}

/// スナップショットが必要な失敗返信を処理する。返り値は
/// (スナップショット時構成, 保持ログの anchor)。
pub fn asserted_handle_append_entries_reply_failure_need_snapshot(
  node_: Node(command),
  reply: Message(command),
) -> #(Node(command), #(ClusterConfig, LogPosition)) {
  let assert AppendEntriesReply(from: from, last_log_position: last, ..) = reply
  should.equal(entries_since(node.log(node_).entries, last), None)
  let assert Ok(node_) = node.handle_message(node_, reply)
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.InstallSnapshot(from))
  let assert [] = assert_no_action(actions)
  #(
    node_,
    #(node.log(node_).snapshot_config, node.log(node_).entries.prev_position),
  )
}

/// 成功返信を処理し、コミット前進と joint 構成の finalize を検証する。
pub fn asserted_handle_append_entries_reply_success(
  node_: Node(command),
  reply: Message(command),
  commit_index_will_be_updated: Bool,
  joint_consensus_will_be_finalized: Bool,
) -> Node(command) {
  let assert AppendEntriesReply(last_log_position: reply_last, ..) = reply
  let old_last_position = log_last_position(node.log(node_))
  let assert Ok(node_) = node.handle_message(node_, reply)
  let #(node_, actions) = drain(node_)
  case commit_index_will_be_updated {
    True -> should.equal(node.commit_index(node_), reply_last.index)
    False -> Nil
  }
  let actions = case joint_consensus_will_be_finalized {
    True -> {
      let actions = assert_action(actions, action.SetElectionTimeout)
      let config = node.config(node_)
      let entries =
        entries_from_list(old_last_position, [log.ConfigEntry(config)])
      let actions = assert_action(actions, action.AppendLogEntries(entries))
      assert_action(
        actions,
        action.BroadcastMessage(append_entries_call(node_, entries)),
      )
    }
    False -> actions
  }
  let assert [] = assert_no_action(actions)
  node_
}

/// joint 構成がコミット済みの成功返信を処理する。返り値は
/// finalization の broadcast AE Call。
pub fn asserted_handle_append_entries_reply_success_with_joint_config_committed(
  node_: Node(command),
  reply: Message(command),
) -> #(Node(command), Message(command)) {
  let assert AppendEntriesReply(last_log_position: reply_last, ..) = reply
  should.be_true(is_joint_consensus(node.config(node_)))
  let prev_entry = log_last_position(node.log(node_))
  let joint_config = node.config(node_)
  let new_config =
    cluster_config.ClusterConfig(
      voters: joint_config.new_voters,
      new_voters: set.new(),
      non_voters: joint_config.non_voters,
    )
  let assert Ok(node_) = node.handle_message(node_, reply)
  let entries = entries_from_list(prev_entry, [log.ConfigEntry(new_config)])
  let call = append_entries_call(node_, entries)
  let #(node_, actions) = drain(node_)
  should.equal(node.commit_index(node_), reply_last.index)
  let actions = assert_action(actions, action.AppendLogEntries(entries))
  let actions = assert_action(actions, action.BroadcastMessage(call))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  #(node_, call)
}

/// ハートビートの送信を検証する。返り値は broadcast AE Call。
pub fn asserted_heartbeat(
  node_: Node(command),
) -> #(Node(command), Message(command)) {
  let #(node_, sent) = node.heartbeat(node_)
  should.be_true(sent)
  let call =
    append_entries_call(
      node_,
      log.entries_new(log_last_position(node.log(node_))),
    )
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.SetElectionTimeout)
  let actions = assert_action(actions, action.BroadcastMessage(call))
  let assert [] = assert_no_action(actions)
  #(node_, call)
}

/// 構成変更の提案を検証する。返り値は broadcast AE Call。
pub fn asserted_change_cluster_config(
  node_: Node(command),
  new_config: ClusterConfig,
) -> #(Node(command), Message(command)) {
  let prev_entry = log_last_position(node.log(node_))
  let next_position = position(prev_entry.term, prev_entry.index + 1)
  let #(node_, proposed) = node.propose_config(node_, new_config)
  should.equal(proposed, Ok(next_position))
  let entries = entries_from_list(prev_entry, [log.ConfigEntry(new_config)])
  let msg = append_entries_call(node_, entries)
  let #(node_, actions) = drain(node_)
  let actions = assert_action(actions, action.AppendLogEntries(entries))
  let actions = assert_action(actions, action.BroadcastMessage(msg))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  #(node_, msg)
}

// ---- ThreeNodeCluster（3 ノードの共通シナリオ） ----

/// 3 ノードのクラスタ素性。ノード番号 0 が voter 付きで起動し、1 と 2 は
/// 空構成で起動する。あえて非対称な初期化にしておくと、後続の修復経路を
/// 必ず通る。
pub type ThreeNodeCluster(command) {
  ThreeNodeCluster(
    node0: Node(command),
    node1: Node(command),
    node2: Node(command),
  )
}

/// 3 ノードを起動した初期状態を作る。
pub fn three_node_cluster() -> ThreeNodeCluster(command) {
  ThreeNodeCluster(
    node0: asserted_start(0, [0, 1, 2]),
    node1: asserted_start(1, []),
    node2: asserted_start(2, []),
  )
}

/// node0 の再立候補、投票、就任、乖離修復（failure AE → 返信 → 修復 AE）
/// までを検証し、全ノードの構成一致で締める。
pub fn init_cluster(
  cluster: ThreeNodeCluster(command),
) -> ThreeNodeCluster(command) {
  let node0 = node.handle_election_timeout(cluster.node0)
  should.equal(node.role(node0), Candidate)
  let #(node0, actions) = drain(node0)
  let rvc =
    RequestVoteCall(
      term: node.current_term(node0),
      from: 0,
      last_log_position: log_last_position(node.log(node0)),
    )
  let actions = assert_action(actions, action.SetElectionTimeout)
  let actions = assert_action(actions, action.SaveCurrentTerm)
  let actions = assert_action(actions, action.SaveVotedFor)
  let actions = assert_action(actions, action.BroadcastMessage(rvc))
  let assert [] = assert_no_action(actions)
  // 1・2 が投票する。過半数の返信は node1 分からnode0 に届ける。
  let #(node1, reply1) =
    asserted_handle_request_vote_call_success(cluster.node1, rvc)
  let #(node2, _reply2) =
    asserted_handle_request_vote_call_success(cluster.node2, rvc)
  let #(node0, call) =
    asserted_handle_request_vote_reply_majority_vote_granted(node0, reply1)
  // node1: 乖離 → failure 返信 → 修復 AE 受理 → success 返信（commit 前進）。
  let #(node1, reply) = asserted_handle_append_entries_call_failure(node1, call)
  let #(node0, retry) =
    asserted_handle_append_entries_reply_failure(node0, reply)
  let #(node1, reply) =
    asserted_handle_append_entries_call_success(node1, retry)
  let node0 =
    asserted_handle_append_entries_reply_success(node0, reply, True, False)
  // node2: 同じ往復。返信は破棄する。
  let #(node2, reply) = asserted_handle_append_entries_call_failure(node2, call)
  let #(node0, retry) =
    asserted_handle_append_entries_reply_failure(node0, reply)
  let #(node2, _reply) =
    asserted_handle_append_entries_call_success(node2, retry)
  should.equal(node.config(node0), node.config(node1))
  should.equal(node.config(node0), node.config(node2))
  ThreeNodeCluster(node0:, node1:, node2:)
}

/// leader での提案、両フォロワへの複製、commit 前進までを検証する。
pub fn propose_command(
  cluster: ThreeNodeCluster(command),
  command: command,
) -> #(ThreeNodeCluster(command), LogPosition) {
  // init_cluster 直後の leader は node0 のはず。
  should.equal(node.role(cluster.node0), Leader)
  let #(node0, proposed) = node.propose_command(cluster.node0, command)
  let assert Some(position) = proposed
  let entries =
    entries_from_list(log_prev(position), [log.CommandEntry(command)])
  let msg = append_entries_call(node0, entries)
  let #(node0, actions) = drain(node0)
  let actions = assert_action(actions, action.AppendLogEntries(entries))
  let actions = assert_action(actions, action.BroadcastMessage(msg))
  let actions = assert_action(actions, action.SetElectionTimeout)
  let assert [] = assert_no_action(actions)
  let #(node1, reply1) =
    asserted_handle_append_entries_call_success(cluster.node1, msg)
  let #(node2, reply2) =
    asserted_handle_append_entries_call_success(cluster.node2, msg)
  let node0 =
    asserted_handle_append_entries_reply_success(node0, reply1, True, False)
  let node0 =
    asserted_handle_append_entries_reply_success(node0, reply2, False, False)
  should.equal(node.commit_index(node0), position.index)
  #(ThreeNodeCluster(node0:, node1:, node2:), position)
}
