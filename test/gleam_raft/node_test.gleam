import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set
import gleam_raft/action
import gleam_raft/cluster_config
import gleam_raft/internal/node
import gleam_raft/log
import gleam_raft/message
import gleam_raft/types
import gleeunit/should

fn voters_config(nodes: List(Int)) -> cluster_config.ClusterConfig {
  cluster_config.ClusterConfig(
    voters: set.from_list(nodes),
    new_voters: set.new(),
    non_voters: set.new(),
  )
}

// テストヘルパ follower()。設定エントリを 1 件持つ
// ログからノードを復旧する。
fn follower(
  id: Int,
  voters: List(Int),
  term: Int,
  voted_for: Option(Int),
) -> node.Node(String) {
  let cfg = voters_config(voters)
  let entries =
    log.entries_from_list(types.zero_position, [log.ConfigEntry(cfg)])
  node.restart(id, term, voted_for, log.log_new(cfg, entries))
}

pub fn metrics_default_after_start_test() -> Nil {
  node.start(0)
  |> node.metrics
  |> should.equal(node.node_metrics_default())
}

pub fn metrics_default_after_restart_test() -> Nil {
  follower(0, [0], 3, Some(1))
  |> node.metrics
  |> should.equal(node.node_metrics_default())
}

pub fn start_skeleton_test() -> Nil {
  // 起動直後は全観測値が初期値。Action も積まれていない。
  let n = node.start(7)
  node.id(n) |> should.equal(7)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(0)
  node.voted_for(n) |> should.equal(None)
  node.log(n) |> log.log_last_position |> should.equal(types.zero_position)
  node.commit_index(n) |> should.equal(0)
  node.config(n) |> cluster_config.unique_nodes |> should.equal([])
  node.peers(n) |> should.equal([])
  node.metrics(n) |> should.equal(node.node_metrics_default())
  node.actions(n) |> action.actions_is_empty |> should.be_true
  // leader でないので match_index は常に None。
  node.follower_match_index(n, 1) |> should.equal(None)
  let #(n2, taken) = node.take_action(n)
  taken |> should.equal(None)
  node.id(n2) |> should.equal(7)
}

pub fn restart_skeleton_test() -> Nil {
  let n = follower(0, [0, 1], 3, Some(1))
  node.id(n) |> should.equal(0)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(3)
  node.voted_for(n) |> should.equal(Some(1))
  node.config(n) |> cluster_config.unique_nodes |> should.equal([0, 1])
  node.peers(n) |> should.equal([1])

  // commit_index はスナップショット位置（entries の anchor）へ巻き戻る。
  node.commit_index(n) |> should.equal(0)
  node.follower_match_index(n, 1) |> should.equal(None)

  // 積まれる Action は SetElectionTimeout 1 件だけ。
  let #(n1, taken) = node.take_action(n)
  taken |> should.equal(Some(action.SetElectionTimeout))
  let #(n2, taken) = node.take_action(n1)
  taken |> should.equal(None)
  node.metrics(n2) |> should.equal(node.node_metrics_default())
  node.actions(n2) |> action.actions_is_empty |> should.be_true
}

// テストヘルパ drain()。積まれた Action を取り切る。
fn drain(n: node.Node(String)) -> node.Node(String) {
  let #(next, taken) = node.take_action(n)
  case taken {
    Some(_) -> drain(next)
    None -> n
  }
}

// candidate_with()。立候補直後の候補者を作る。
// metrics は外部からリセットできないため、ヘルパ経由で確定値になるカウンタ
// （elections_started など）だけを検証に使う。
fn candidate_with(id: Int, others: List(Int)) -> node.Node(String) {
  let n =
    follower(id, [id, ..others], 0, None)
    |> drain
    |> node.handle_election_timeout
  node.role(n) |> should.equal(types.Candidate)
  drain(n)
}

// leader になるまで他 voter から投票受理を返す。
// majority 到達（leader 化）以降の投票は送らない。
fn promote(n: node.Node(String), remaining: List(Int)) -> node.Node(String) {
  case node.role(n), remaining {
    types.Leader, _ -> n
    _, [] -> n
    _, [from, ..rest] -> {
      let reply =
        message.RequestVoteReply(
          term: node.current_term(n),
          from: from,
          vote_granted: True,
        )
      case node.handle_message(n, reply) {
        Ok(next) -> promote(next, rest)
        Error(_) -> panic as "handle_message は HandleError を返さない"
      }
    }
  }
}

// leader_with()。create_cluster と投票受理で
// leader になったノードを作る（elections_started=1, leaderships_started=1）。
fn leader_with(id: Int, others: List(Int)) -> node.Node(String) {
  let #(n, created) = node.create_cluster(node.start(id), [id, ..others])
  let assert Ok(_) = created
  let n =
    n
    |> drain
    |> promote(others)
  node.role(n) |> should.equal(types.Leader)
  drain(n)
}

pub fn self_messages_ignored_counter_test() -> Nil {
  let n = node.start(0)
  let msg =
    message.RequestVoteCall(
      term: 1,
      from: 0,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).self_messages_ignored |> should.equal(1)
  node.metrics(n).term_advances_from_messages |> should.equal(0)
}

pub fn term_advances_from_messages_counter_test() -> Nil {
  let n = follower(0, [0, 1], 1, None) |> drain
  let msg =
    message.RequestVoteCall(
      term: 5,
      from: 1,
      last_log_position: types.position(5, 1),
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).term_advances_from_messages |> should.equal(1)
}

pub fn request_vote_calls_rejected_by_old_term_counter_test() -> Nil {
  let n = follower(0, [0, 1], 5, None) |> drain
  let msg =
    message.RequestVoteCall(
      term: 2,
      from: 1,
      last_log_position: types.position(2, 5),
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).request_vote_calls_rejected_by_old_term
  |> should.equal(1)
  node.metrics(n).request_vote_calls_rejected_by_log |> should.equal(0)
  node.metrics(n).request_vote_calls_rejected_by_existing_vote
  |> should.equal(0)
}

pub fn request_vote_calls_rejected_by_log_counter_test() -> Nil {
  let n = follower(0, [0, 1], 1, None) |> drain
  // フォロアのログは (term=0, index=1) に構成エントリを持つ。候補者は
  // (0, 0) を報告するので候補者側が劣る。
  let msg =
    message.RequestVoteCall(
      term: 1,
      from: 1,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).request_vote_calls_rejected_by_log |> should.equal(1)
  node.metrics(n).request_vote_calls_rejected_by_old_term |> should.equal(0)
}

pub fn request_vote_calls_rejected_by_existing_vote_counter_test() -> Nil {
  let n = follower(0, [0, 1, 2], 3, Some(1)) |> drain
  // 同一項。ログ判定は通る（候補者の位置 (3, 5) は自ノード (0, 1) より新しい）。
  let msg =
    message.RequestVoteCall(
      term: 3,
      from: 2,
      last_log_position: types.position(3, 5),
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).request_vote_calls_rejected_by_existing_vote
  |> should.equal(1)
  // 却下時は返信を送らない（サイレント却下）。
  node.actions(n).send_messages |> dict.is_empty |> should.be_true
  node.voted_for(n) |> should.equal(Some(1))
}

pub fn request_vote_replies_ignored_from_old_terms_counter_test() -> Nil {
  let n = candidate_with(0, [1, 2])
  // 立候補後の項は 1。返信は項 0 を運ぶ。
  node.current_term(n) |> should.equal(1)
  let msg = message.RequestVoteReply(term: 0, from: 1, vote_granted: True)
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).request_vote_replies_ignored_from_old_terms
  |> should.equal(1)
  node.metrics(n).request_vote_replies_ignored_while_not_candidate
  |> should.equal(0)
}

pub fn request_vote_replies_ignored_while_not_candidate_counter_test() -> Nil {
  let n = follower(0, [0, 1], 3, None) |> drain
  let msg = message.RequestVoteReply(term: 3, from: 1, vote_granted: True)
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).request_vote_replies_ignored_while_not_candidate
  |> should.equal(1)
  node.metrics(n).request_vote_replies_ignored_from_old_terms
  |> should.equal(0)
}

pub fn elections_started_counter_test() -> Nil {
  let n = follower(0, [0, 1], 0, None) |> drain
  node.metrics(n).elections_started |> should.equal(0)
  let n = node.handle_election_timeout(n)
  node.metrics(n).elections_started |> should.equal(1)
}

pub fn elections_started_not_counted_for_non_voter_test() -> Nil {
  // 自ノード (9) が voter でない構成では立候補しない。
  let cfg = voters_config([0])
  let entries =
    log.entries_from_list(types.zero_position, [log.ConfigEntry(cfg)])
  let n =
    node.restart(9, 0, None, log.log_new(cfg, entries))
    |> drain
    |> node.handle_election_timeout
  node.metrics(n).elections_started |> should.equal(0)
  node.role(n) |> should.equal(types.Follower)
}

pub fn leaderships_started_counter_test() -> Nil {
  let #(n, created) = node.create_cluster(node.start(0), [0])
  let assert Ok(_) = created
  node.role(n) |> should.equal(types.Leader)
  node.metrics(n).leaderships_started |> should.equal(1)
  // 単独 voter でも立候補は 1 回計上されている。
  node.metrics(n).elections_started |> should.equal(1)
}

pub fn counters_persist_through_role_transitions_test() -> Nil {
  let n = follower(0, [0, 1], 0, None) |> drain
  // Follower: 高項の RVC を受けて項が進む。
  let msg =
    message.RequestVoteCall(
      term: 5,
      from: 1,
      last_log_position: types.position(5, 1),
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.metrics(n).term_advances_from_messages |> should.equal(1)
  let n = drain(n)
  // Follower -> Candidate。
  let n = node.handle_election_timeout(n)
  node.role(n) |> should.equal(types.Candidate)
  node.metrics(n).elections_started |> should.equal(1)
  // Candidate -> Follower（さらに高い項への追従）。
  let msg =
    message.RequestVoteCall(
      term: 10,
      from: 1,
      last_log_position: types.position(10, 1),
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.role(n) |> should.equal(types.Follower)
  node.metrics(n).elections_started |> should.equal(1)
  node.metrics(n).term_advances_from_messages |> should.equal(2)
}

pub fn transition_to_follower_purges_stale_outbound_calls_test() -> Nil {
  // 旧項の broadcast ハートビートとあて先付き AppendEntriesCall を
  // set_actions シームで仕込み、より高い項へ降格させて両方をパージする。
  let leader = leader_with(0, [1])
  let term = node.current_term(leader)
  let stale =
    message.AppendEntriesCall(
      term: term,
      from: 0,
      leader_commit: 0,
      entries: log.entries_from_list(types.zero_position, []),
    )
  let planted =
    action.Actions(
      ..action.actions_empty(),
      broadcast_message: Some(stale),
      send_messages: dict.from_list([#(1, stale)]),
    )
  let n = node.set_actions(leader, planted)
  let trigger =
    message.AppendEntriesReply(
      term: term + 3,
      from: 1,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, trigger)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(term + 3)
  node.actions(n).broadcast_message |> should.equal(None)
  node.actions(n).send_messages |> dict.is_empty |> should.be_true
}

pub fn transition_to_follower_retains_stale_reply_variants_test() -> Nil {
  // パージ条件の Call/Reply 非対称を固定する。旧項の Call は捨て、Reply
  // 系は残す。あて先 3 つに Call と 2 種の Reply を仕込んで降格させる。
  let leader = leader_with(0, [1, 2, 3])
  let stale_term = node.current_term(leader)
  let stale_call =
    message.AppendEntriesCall(
      term: stale_term,
      from: 0,
      leader_commit: 0,
      entries: log.entries_from_list(types.zero_position, []),
    )
  let stale_ae_reply =
    message.AppendEntriesReply(
      term: stale_term,
      from: 0,
      last_log_position: types.zero_position,
    )
  let stale_rv_reply =
    message.RequestVoteReply(term: stale_term, from: 0, vote_granted: True)
  let planted =
    action.Actions(
      ..action.actions_empty(),
      send_messages: dict.from_list([
        #(1, stale_call),
        #(2, stale_ae_reply),
        #(3, stale_rv_reply),
      ]),
    )
  let n = node.set_actions(leader, planted)
  let trigger =
    message.AppendEntriesReply(
      term: stale_term + 3,
      from: 2,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, trigger)
  let messages = node.actions(n).send_messages
  messages |> dict.get(1) |> should.equal(Error(Nil))
  messages |> dict.get(2) |> should.equal(Ok(stale_ae_reply))
  messages |> dict.get(3) |> should.equal(Ok(stale_rv_reply))
}

pub fn transition_to_follower_keeps_election_timer_on_same_role_term_bump_test() -> Nil {
  // Follower が自分のログより劣る候補者から高項 RVC を受ける経路。
  // 同一ロールの項上げでは SetElectionTimeout を積まない。
  // AppendEntries 経由ケースと同じ不変条件を、ログ劣位によるサイレント
  // 却下を使って検証する。
  let n = follower(0, [0, 1], 5, None) |> drain
  node.actions(n).set_election_timeout |> should.be_false
  // 前提: 自分のログ末尾は (0, 0) より新しい。
  node.log(n) |> log.log_last_position |> should.not_equal(types.zero_position)
  let msg =
    message.RequestVoteCall(
      term: 10,
      from: 1,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, msg)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(10)
  node.voted_for(n) |> should.equal(None)
  node.actions(n).set_election_timeout |> should.be_false
}

pub fn transition_to_follower_resets_election_timer_when_granting_vote_test() -> Nil {
  let n = follower(0, [0, 1], 5, None) |> drain
  node.actions(n).set_election_timeout |> should.be_false
  // 候補者のログが自ノードと同等以上なら投票を許可する。
  let up_to_date = node.log(n) |> log.log_last_position
  let msg =
    message.RequestVoteCall(term: 10, from: 1, last_log_position: up_to_date)
  let assert Ok(n) = node.handle_message(n, msg)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(10)
  node.voted_for(n) |> should.equal(Some(1))
  node.actions(n).set_election_timeout |> should.be_true
}

pub fn transition_to_follower_resets_election_timer_from_candidate_test() -> Nil {
  let n = candidate_with(0, [1])
  node.actions(n).set_election_timeout |> should.be_false
  let higher = node.current_term(n) + 1
  let msg = message.RequestVoteReply(term: higher, from: 1, vote_granted: True)
  let assert Ok(n) = node.handle_message(n, msg)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(higher)
  node.actions(n).set_election_timeout |> should.be_true
}

pub fn transition_to_follower_resets_election_timer_from_leader_test() -> Nil {
  let leader = leader_with(0, [1])
  node.actions(leader).set_election_timeout |> should.be_false
  let higher = node.current_term(leader) + 1
  let msg =
    message.AppendEntriesReply(
      term: higher,
      from: 1,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(leader, msg)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(higher)
  node.actions(n).set_election_timeout |> should.be_true
}

pub fn follower_match_index_returns_none_for_non_leader_roles_test() -> Nil {
  let f = follower(0, [0, 1], 1, None)
  node.follower_match_index(f, 0) |> should.equal(None)
  node.follower_match_index(f, 1) |> should.equal(None)

  let c = candidate_with(0, [1])
  node.role(c) |> should.equal(types.Candidate)
  node.follower_match_index(c, 0) |> should.equal(None)
  node.follower_match_index(c, 1) |> should.equal(None)
}

// 自作テスト: create_cluster の前提検証（前条件違反経路の単体テストを
// 補うもの）。
pub fn create_cluster_rejects_non_fresh_and_empty_voters_test() -> Nil {
  // ログが空でないノードは NotFresh。
  let #(stale, result) =
    node.create_cluster(follower(0, [0, 1], 1, None), [0, 1])
  result |> should.equal(Error(types.NotFresh))
  node.role(stale) |> should.equal(types.Follower)

  // 初期 voter が空なら EmptyVoters。
  let #(_, result) = node.create_cluster(node.start(0), [])
  result |> should.equal(Error(types.EmptyVoters))
}

// 自作テスト: heartbeat() の非 leader 無効と単独 leader のタイマーリセット。
pub fn heartbeat_ignores_non_leader_and_resets_solo_leader_timer_test() -> Nil {
  let f = follower(0, [0, 1], 1, None) |> drain
  let #(f2, sent) = node.heartbeat(f)
  sent |> should.be_false
  node.actions(f2) |> action.actions_is_empty |> should.be_true

  let #(solo, created) = node.create_cluster(node.start(0), [0])
  let assert Ok(_) = created
  let solo = drain(solo)
  let #(solo2, sent) = node.heartbeat(solo)
  sent |> should.be_true
  // 単独 leader の heartbeat が積むのは SetElectionTimeout だけ。
  let #(solo3, taken) = node.take_action(solo2)
  taken |> should.equal(Some(action.SetElectionTimeout))
  let #(_, taken) = node.take_action(solo3)
  taken |> should.equal(None)
}

// 自作テスト: could_be_disruptive_request_vote() の判定表。
pub fn could_be_disruptive_request_vote_matrix_test() -> Nil {
  let leader = leader_with(0, [1])
  let term = node.current_term(leader)
  let call = fn(t: Int, from: Int) -> message.Message(String) {
    message.RequestVoteCall(
      term: t,
      from: from,
      last_log_position: types.zero_position,
    )
  }
  // leader は voted_for=Some(self)。より高い項の他ノードの RVC は disrupt 候補。
  leader
  |> node.could_be_disruptive_request_vote(call(term + 1, 1))
  |> should.be_true
  // ただし voted_for と同一ノードからの要求は disrupt にならない。
  leader
  |> node.could_be_disruptive_request_vote(call(term + 1, 0))
  |> should.be_false
  // 項が進まない RVC は対象外。
  leader
  |> node.could_be_disruptive_request_vote(call(term, 1))
  |> should.be_false
  // Reply 系は対象外。
  let reply =
    message.RequestVoteReply(term: term + 1, from: 1, vote_granted: True)
  leader |> node.could_be_disruptive_request_vote(reply) |> should.be_false
  // （AE Call も素通しでなければならない。見送り判断の根拠となる腕）。
  let aec =
    message.AppendEntriesCall(
      term: term + 1,
      from: 1,
      leader_commit: 0,
      entries: log.entries_new(types.zero_position),
    )
  leader |> node.could_be_disruptive_request_vote(aec) |> should.be_false

  // voted_for が未設定の follower は対象外。
  let f = follower(0, [0, 1], 1, None) |> drain
  f |> node.could_be_disruptive_request_vote(call(2, 1)) |> should.be_false

  // candidate は対象外。
  let c = candidate_with(0, [1, 2])
  c |> node.could_be_disruptive_request_vote(call(2, 1)) |> should.be_false
}

// 積まれた Action を取り出し順に並べて返す。順序アサート用。
fn take_all(
  n: node.Node(String),
  acc: List(action.Action(String)),
) -> #(node.Node(String), List(action.Action(String))) {
  case node.take_action(n) {
    #(next, Some(taken)) -> take_all(next, [taken, ..acc])
    #(done, None) -> #(done, list.reverse(acc))
  }
}

pub fn append_entries_calls_rejected_by_old_term_counter_test() -> Nil {
  let n = follower(0, [0, 1], 5, None) |> drain
  let call =
    message.AppendEntriesCall(
      term: 2,
      from: 1,
      leader_commit: 0,
      entries: log.entries_new(types.position(2, 1)),
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.metrics(n).append_entries_calls_rejected_by_old_term
  |> should.equal(1)
  // 補強: 数えるだけでなく、返信を返して送り手に自ノードの項を気づかせる
  // この経路でタイマーは積まない。
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesReply(
        term: 5,
        from: 0,
        last_log_position: types.position(0, 1),
      ),
    ),
  ])
}

pub fn same_term_append_entries_calls_received_by_leader_counter_test() -> Nil {
  let n = leader_with(0, [1])
  // 別のノードが同項の leader を名乗ってきたという設定。
  let call =
    message.AppendEntriesCall(
      term: node.current_term(n),
      from: 1,
      leader_commit: 0,
      entries: log.entries_new(log.log_last_position(node.log(n))),
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.metrics(n).same_term_append_entries_calls_received_by_leader
  |> should.equal(1)
  // 補強: 正常系で衝突は起こり得ないので、数えるだけでアクションは積まない。
  node.actions(n) |> action.actions_is_empty |> should.be_true
}

pub fn log_divergences_detected_by_follower_counter_test() -> Nil {
  let n = follower(0, [0, 1], 2, None) |> drain
  // follower は (0,1) に ClusterConfig を持つ。prev を (999,1) にした
  // Call は index が一致して term が違うので発散として扱う。
  let call =
    message.AppendEntriesCall(
      term: 2,
      from: 1,
      leader_commit: 0,
      entries: log.entries_from_list(types.position(999, 1), [
        log.CommandEntry("x"),
      ]),
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.metrics(n).log_divergences_detected_by_follower |> should.equal(1)
  // 補強: 発散の根が特定できるまで AppendLogEntries は積まない。
  // 尾部を切り捨てて末尾位置は anchor に戻る。
  let #(n, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SetElectionTimeout,
    action.SaveVotedFor,
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesReply(
        term: 2,
        from: 0,
        last_log_position: types.zero_position,
      ),
    ),
  ])
  node.log(n) |> log.log_last_position |> should.equal(types.zero_position)
}

pub fn append_entries_replies_ignored_from_old_terms_counter_test() -> Nil {
  let n = leader_with(0, [1])
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n) - 1,
      from: 1,
      last_log_position: log.log_last_position(node.log(n)),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.metrics(n).append_entries_replies_ignored_from_old_terms
  |> should.equal(1)
}

pub fn append_entries_replies_ignored_while_not_leader_counter_test() -> Nil {
  let n = follower(0, [0, 1], 3, None) |> drain
  let reply =
    message.AppendEntriesReply(
      term: 3,
      from: 1,
      last_log_position: types.position(3, 1),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.metrics(n).append_entries_replies_ignored_while_not_leader
  |> should.equal(1)
}

pub fn append_entries_replies_ignored_from_unknown_nodes_counter_test() -> Nil {
  let n = leader_with(0, [1])
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 9,
      last_log_position: log.log_last_position(node.log(n)),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.metrics(n).append_entries_replies_ignored_from_unknown_nodes
  |> should.equal(1)
}

// match_index を直接書き換えて遅延返信に見せる whitebox は採れない。
// Gleam 側に書き込みシームはないので、正常な Reply で match_index を 2 へ
// 進めた後に遅れた Reply を送る正当な手順で置き換える。
pub fn append_entries_replies_ignored_behind_match_index_counter_test() -> Nil {
  let n = leader_with(0, [1])
  let advance =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(1, 2),
    )
  let assert Ok(n) = node.handle_message(n, advance)
  node.follower_match_index(n, 1) |> should.equal(Some(2))
  let #(n, _) = take_all(n, [])
  let commit_before = node.commit_index(n)
  let stale =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(1, 1),
    )
  let assert Ok(n) = node.handle_message(n, stale)
  node.metrics(n).append_entries_replies_ignored_behind_match_index
  |> should.equal(1)
  // 遅延返信の破棄は commit を進めず、新しいアクションも積まない。
  node.commit_index(n) |> should.equal(commit_before)
  node.actions(n) |> action.actions_is_empty |> should.be_true
}

pub fn append_entries_replies_ahead_of_leader_counter_test() -> Nil {
  let n = leader_with(0, [1])
  let leader_last = log.log_last_position(node.log(n)).index
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(node.current_term(n), leader_last + 10),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.metrics(n).append_entries_replies_ahead_of_leader |> should.equal(1)
}

pub fn log_divergences_detected_by_leader_counter_test() -> Nil {
  let n = leader_with(0, [1])
  let last_index = log.log_last_position(node.log(n)).index
  // 末尾の index そのままに違う term を報告すると、contains は失敗して
  // get_term は Some を返す。
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(999, last_index),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.metrics(n).log_divergences_detected_by_leader |> should.equal(1)
  // 補強: 発散の修復として、leader 自身の項で該当 1 件を再送させる。
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesCall(
        term: 1,
        from: 0,
        leader_commit: 0,
        entries: log.entries_new(types.position(1, 2)),
      ),
    ),
  ])
}

pub fn follower_match_index_returns_none_for_unknown_and_self_on_leader_test() -> Nil {
  let leader = leader_with(0, [1])
  node.follower_match_index(leader, 0) |> should.equal(None)
  node.follower_match_index(leader, 99) |> should.equal(None)
}

pub fn follower_match_index_starts_at_zero_for_tracked_voters_test() -> Nil {
  let leader = leader_with(0, [1, 2])
  node.follower_match_index(leader, 1) |> should.equal(Some(0))
  node.follower_match_index(leader, 2) |> should.equal(Some(0))
}

pub fn follower_match_index_advances_on_successful_reply_test() -> Nil {
  let n = leader_with(0, [1])
  let last = log.log_last_position(node.log(n))
  should.be_true(last.index > 0)
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: last,
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.follower_match_index(n, 1) |> should.equal(Some(last.index))
}

// 置き換えた手順は append_entries_replies_ignored_behind_match_index_counter
// と同じ二段構え（私有フィールド書き換えへの whitebox 依存を避ける）。
pub fn follower_match_index_does_not_regress_on_delayed_reply_test() -> Nil {
  let n = leader_with(0, [1])
  let advance =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(1, 2),
    )
  let assert Ok(n) = node.handle_message(n, advance)
  node.follower_match_index(n, 1) |> should.equal(Some(2))
  let stale =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(1, 1),
    )
  let assert Ok(n) = node.handle_message(n, stale)
  // 遅延返信でも match_index は後退しない。
  node.follower_match_index(n, 1) |> should.equal(Some(2))
}

pub fn follower_match_index_returns_none_after_stepdown_test() -> Nil {
  let n = leader_with(0, [1])
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: log.log_last_position(node.log(n)),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.follower_match_index(n, 1) |> should.equal(Some(2))

  // より高い項の RequestVoteCall で降格させる。
  let higher = node.current_term(n) + 10
  let call =
    message.RequestVoteCall(
      term: higher,
      from: 1,
      last_log_position: types.position(higher, 1),
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.role(n) |> should.equal(types.Follower)
  node.follower_match_index(n, 1) |> should.equal(None)
}

pub fn follower_match_index_resets_to_zero_on_reelection_test() -> Nil {
  let n = leader_with(0, [1])
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: log.log_last_position(node.log(n)),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.follower_match_index(n, 1) |> should.equal(Some(2))

  // 高項の RVC で降格し、選挙をやり直す。
  let higher = node.current_term(n) + 10
  let call =
    message.RequestVoteCall(
      term: higher,
      from: 1,
      last_log_position: types.position(higher, 1),
    )
  let assert Ok(n) = node.handle_message(n, call)
  let n = n |> drain |> node.handle_election_timeout |> drain
  node.role(n) |> should.equal(types.Candidate)
  let vote =
    message.RequestVoteReply(
      term: node.current_term(n),
      from: 1,
      vote_granted: True,
    )
  let assert Ok(n) = node.handle_message(n, vote)
  node.role(n) |> should.equal(types.Leader)

  // 新しい任期は同じ follower の match_index も 0 からやり直す。
  node.follower_match_index(n, 1) |> should.equal(Some(0))
}

// 自作テスト: 固定点 8 の本丸。follower を持つ leader の heartbeat は
// 全 peer への AEC broadcast と SetElectionTimeout を積む。
pub fn heartbeat_broadcasts_append_entries_to_followers_test() -> Nil {
  let n = leader_with(0, [1, 2])
  let #(n, heartbeat_ok) = node.heartbeat(n)
  heartbeat_ok |> should.be_true
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SetElectionTimeout,
    action.BroadcastMessage(message.AppendEntriesCall(
      term: 1,
      from: 0,
      leader_commit: 0,
      entries: log.entries_new(types.position(1, 2)),
    )),
  ])
}

// 自作テスト: propose_command の正常系。AppendLogEntries → BroadcastMessage
// の順でアクションを積み、追加後の末尾位置を返す。
pub fn propose_command_appends_and_broadcasts_delta_test() -> Nil {
  let n = leader_with(0, [1])
  let #(n, proposed) = node.propose_command(n, "write")
  proposed |> should.equal(Some(types.position(1, 3)))
  let delta =
    log.entries_from_list(types.position(1, 2), [log.CommandEntry("write")])
  let #(n, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SetElectionTimeout,
    action.AppendLogEntries(delta),
    action.BroadcastMessage(message.AppendEntriesCall(
      term: 1,
      from: 0,
      leader_commit: 0,
      entries: delta,
    )),
  ])
  node.log(n) |> log.log_last_position |> should.equal(types.position(1, 3))
  // 自ノードの match だけでは多数派に届かないので commit は動かない。
  node.commit_index(n) |> should.equal(0)
}

// 自作テスト: propose_command は leader 以外だと無操作・None（D1）。
pub fn propose_command_rejected_while_not_leader_test() -> Nil {
  let n = follower(0, [0, 1], 1, None) |> drain
  let #(n2, proposed) = node.propose_command(n, "write")
  proposed |> should.equal(None)
  node.log(n2) |> should.equal(node.log(n))
  node.metrics(n2) |> should.equal(node.metrics(n))
  node.actions(n2) |> action.actions_is_empty |> should.be_true
}

// 自作テスト: AEC 正常系（固定点 1・4）。 follower は追記して commit を
// 進め、返信と SetElectionTimeout を積む。高項 Call による dispatch 側の
// 項更新も同時に走る。
pub fn append_entries_call_appends_and_advances_commit_test() -> Nil {
  let n = follower(0, [0, 1], 0, None) |> drain
  let delta =
    log.entries_from_list(types.position(0, 1), [
      log.TermEntry(1),
      log.CommandEntry("a"),
    ])
  let call =
    message.AppendEntriesCall(
      term: 1,
      from: 1,
      leader_commit: 2,
      entries: delta,
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.current_term(n) |> should.equal(1)
  node.role(n) |> should.equal(types.Follower)
  node.voted_for(n) |> should.equal(Some(1))
  node.log(n) |> log.log_last_position |> should.equal(types.position(1, 3))
  node.commit_index(n) |> should.equal(2)
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SetElectionTimeout,
    action.SaveCurrentTerm,
    action.SaveVotedFor,
    action.AppendLogEntries(delta),
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesReply(
        term: 1,
        from: 0,
        last_log_position: types.position(1, 3),
      ),
    ),
  ])
}

// 自作テスト: AER での追いつき（固定点 3）。追いついていない返信には
// 差分 delta を SendMessage で再送し、commit は多数派の位置へ進む。
pub fn append_entries_reply_sends_catchup_delta_test() -> Nil {
  let n = leader_with(0, [1])
  let #(n, _) = node.propose_command(n, "write")
  let n = drain(n)
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.position(1, 2),
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.follower_match_index(n, 1) |> should.equal(Some(2))
  // 自ノード末尾 3、相手 match 2 の多数派は 2 なので commit は 2 に進む。
  node.commit_index(n) |> should.equal(2)
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesCall(
        term: 1,
        from: 0,
        leader_commit: 2,
        entries: log.entries_from_list(types.position(1, 2), [
          log.CommandEntry("write"),
        ]),
      ),
    ),
  ])
}

// 自作テスト: anchor 位置（(0,0)）の返信は「含まれる」扱いなので snapshot
// 要求にならず、ログ全件の再送になる。contains は anchor を含み、leader
// 側にスナップショットがない限り InstallSnapshot は届かない
// （snapshot 整備は M3-5）。
pub fn append_entries_reply_at_anchor_replays_whole_log_test() -> Nil {
  let n = leader_with(0, [1])
  let reply =
    message.AppendEntriesReply(
      term: node.current_term(n),
      from: 1,
      last_log_position: types.zero_position,
    )
  let assert Ok(n) = node.handle_message(n, reply)
  // match_index は 0 のまま、commit も動かない。
  node.follower_match_index(n, 1) |> should.equal(Some(0))
  node.commit_index(n) |> should.equal(0)
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesCall(
        term: 1,
        from: 0,
        leader_commit: 0,
        entries: log.entries_from_list(types.zero_position, [
          log.ConfigEntry(voters_config([0, 1])),
          log.TermEntry(1),
        ]),
      ),
    ),
  ])
}

// 自作テスト: 固定点 4。candidate は同項の AEC で項を動かさず role だけ
// Follower に変え、追記と返信をこなす（transition_to_follower は通らない）。
pub fn candidate_recognises_leader_from_same_term_append_entries_call_test() -> Nil {
  let n = candidate_with(0, [1])
  let delta = log.entries_from_list(types.position(0, 1), [log.TermEntry(1)])
  let call =
    message.AppendEntriesCall(
      term: 1,
      from: 1,
      leader_commit: 0,
      entries: delta,
    )
  let assert Ok(n) = node.handle_message(n, call)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(1)
  node.voted_for(n) |> should.equal(Some(1))
  node.log(n) |> log.log_last_position |> should.equal(types.position(1, 2))
  let #(_, taken) = take_all(n, [])
  taken
  |> should.equal([
    action.SetElectionTimeout,
    action.SaveVotedFor,
    action.AppendLogEntries(delta),
    action.SendMessage(
      to: 1,
      message: message.AppendEntriesReply(
        term: 1,
        from: 0,
        last_log_position: types.position(1, 2),
      ),
    ),
  ])
}

pub fn propose_config_promotes_existing_non_voter_to_voter_test() -> Nil {
  let n = leader_with(0, [])
  let promoting =
    cluster_config.ClusterConfig(
      voters: set.from_list([0]),
      new_voters: set.new(),
      non_voters: set.from_list([1]),
    )
  let #(n, proposed) = node.propose_config(n, promoting)
  proposed |> should.equal(Ok(types.position(1, 3)))
  let n = drain(n)
  // non_voters の変更だけによる昇格は joint 化しないので、単独 leader は
  // その場で commit する。
  node.commit_index(n) |> should.equal(3)

  let #(joint, converted) =
    node.config(n) |> cluster_config.to_joint_consensus([1], [])
  should.be_true(converted)
  let #(n, proposed) = node.propose_config(n, joint)
  proposed |> should.be_ok
  let n = drain(n)
  // new_voters が居る時点でもう単独ではない（台数は和集合）。ノード 1 の応
  // 信が来るまで joint 構成は commit されない。
  node.config(n) |> cluster_config.is_joint_consensus |> should.be_true
  let joint_position = node.log(n) |> log.log_last_position
  let reply =
    message.AppendEntriesReply(
      term: 1,
      from: 1,
      last_log_position: joint_position,
    )
  let assert Ok(n) = node.handle_message(n, reply)
  let n = drain(n)
  // joint 構成が commit されると最終構成が自動提案される。最終 voters に
  // 自ノードが入っているので leader は続投する。
  let current = node.config(n)
  cluster_config.is_joint_consensus(current) |> should.be_false
  set.contains(current.voters, 1) |> should.be_true
  set.contains(current.non_voters, 1) |> should.be_false
  node.commit_index(n) |> should.equal(4)
  node.role(n) |> should.equal(types.Leader)
}

pub fn follower_match_index_starts_at_zero_for_non_voter_and_new_voter_test() -> Nil {
  let n = leader_with(0, [])
  let learner =
    cluster_config.ClusterConfig(
      voters: set.from_list([0]),
      new_voters: set.new(),
      non_voters: set.from_list([1]),
    )
  let #(n, proposed) = node.propose_config(n, learner)
  proposed |> should.be_ok
  let n = drain(n)
  // learner（non_voters）も提案の時点で台帳に乗り、返信なしなら match は
  // 0 から始まる。
  node.follower_match_index(n, 1) |> should.equal(Some(0))

  let #(joint, converted) =
    node.config(n) |> cluster_config.to_joint_consensus([2], [])
  should.be_true(converted)
  let #(n, proposed) = node.propose_config(n, joint)
  proposed |> should.be_ok
  let n = drain(n)
  // 新しい投票者も同様に、提案の時点で 0。
  node.follower_match_index(n, 2) |> should.equal(Some(0))
  // joint 構成が commit 待ちの間、自ノードは単独でないので joint のまま
  // 残る。
  node.config(n) |> cluster_config.is_joint_consensus |> should.be_true
}

pub fn follower_match_index_returns_none_after_removed_from_config_test() -> Nil {
  let n = leader_with(0, [])
  let learner =
    cluster_config.ClusterConfig(
      voters: set.from_list([0]),
      new_voters: set.new(),
      non_voters: set.from_list([1]),
    )
  let #(n, proposed) = node.propose_config(n, learner)
  proposed |> should.be_ok
  let n = drain(n)
  node.follower_match_index(n, 1) |> should.be_some

  let #(n, proposed) = node.propose_config(n, voters_config([0]))
  proposed |> should.be_ok
  let n = drain(n)
  // 構成から外れると rebuild_followers が台帳から落とすので None に戻る。
  node.follower_match_index(n, 1) |> should.equal(None)
}

// ローカライズ: node.rs::transition_to_follower_keeps_same_term_outbound_calls.
// 私有の transition_to_follower は直接呼べないので、ここでは自分を除
// く最終構成の commit から来る同項自己降格の経路を通す。
pub fn transition_to_follower_keeps_same_term_outbound_calls_test() -> Nil {
  let n = leader_with(0, [1])
  let #(joint, converted) =
    node.config(n) |> cluster_config.to_joint_consensus([], [0])
  should.be_true(converted)
  let #(n, proposed) = node.propose_config(n, joint)
  let assert Ok(joint_position) = proposed
  let n = drain(n)

  let reply =
    message.AppendEntriesReply(
      term: 1,
      from: 1,
      last_log_position: joint_position,
    )
  let assert Ok(n) = node.handle_message(n, reply)
  let n = drain(n)
  // 最終構成（voters {1}）はまだ commit されていないので自ノードは leader
  // のまま。ここで同項の AEC を送信待ちに差し込む（set_actions は M6 で
  // @internal 化されたシーム）。
  node.role(n) |> should.equal(types.Leader)
  let pending =
    message.AppendEntriesCall(
      term: 1,
      from: 0,
      leader_commit: 0,
      entries: log.entries_from_list(types.zero_position, []),
    )
  let n =
    node.set_actions(
      n,
      action.Actions(
        ..action.actions_empty(),
        broadcast_message: Some(pending),
        send_messages: dict.from_list([#(1, pending)]),
      ),
    )

  // 最終構成が commit されると自ノードは voters に居ないので同項で
  // follower へ降格する。同項の Call はパージ（厳密な `<`）を通過して残る。
  let last_position = node.log(n) |> log.log_last_position
  let reply =
    message.AppendEntriesReply(
      term: 1,
      from: 1,
      last_log_position: last_position,
    )
  let assert Ok(n) = node.handle_message(n, reply)
  node.role(n) |> should.equal(types.Follower)
  node.current_term(n) |> should.equal(1)
  node.commit_index(n) |> should.equal(4)
  let left = node.actions(n)
  left.set_election_timeout |> should.be_true
  left.broadcast_message |> should.equal(Some(pending))
  left.send_messages |> should.equal(dict.from_list([#(1, pending)]))
}

// 自作テスト: propose_config の前条件違反 4 変種と判定順。gleam では変種に区別する（D1）。判定順は
// NotLeader → VotersChanged → OverlappingNodes → JointInProgress
// （DESIGN §6）。
pub fn propose_config_precondition_errors_test() -> Nil {
  let n = follower(0, [0, 1], 1, None) |> drain
  let #(n, result) = node.propose_config(n, voters_config([0, 1]))
  result |> should.equal(Error(types.NotLeader))
  node.actions(n) |> action.actions_is_empty |> should.be_true

  let n = leader_with(0, [1])
  // VotersChanged: voters 集合が commit 済み構成と違う。
  let #(n, result) = node.propose_config(n, voters_config([2]))
  result |> should.equal(Error(types.VotersChanged))

  // VotersChanged と OverlappingNodes が同時に成るときは VotersChanged が
  // 先（判定順の固定）。
  let both_wrong =
    cluster_config.ClusterConfig(
      voters: set.from_list([2]),
      new_voters: set.new(),
      non_voters: set.from_list([2]),
    )
  let #(_, result) = node.propose_config(n, both_wrong)
  result |> should.equal(Error(types.VotersChanged))

  // OverlappingNodes: voters と non_voters が交わる。
  let overlap_voters =
    cluster_config.ClusterConfig(
      voters: set.from_list([0, 1]),
      new_voters: set.new(),
      non_voters: set.from_list([0]),
    )
  let #(_, result) = node.propose_config(n, overlap_voters)
  result |> should.equal(Error(types.OverlappingNodes))

  // OverlappingNodes: new_voters と non_voters が交わる。
  let overlap_new_voters =
    cluster_config.ClusterConfig(
      voters: set.from_list([0, 1]),
      new_voters: set.from_list([2]),
      non_voters: set.from_list([2]),
    )
  let #(_, result) = node.propose_config(n, overlap_new_voters)
  result |> should.equal(Error(types.OverlappingNodes))

  // JointInProgress: joint 構成が commit 待ちの間は次の提案を拒否する。
  let #(joint, converted) =
    node.config(n) |> cluster_config.to_joint_consensus([2], [])
  should.be_true(converted)
  let #(n, result) = node.propose_config(n, joint)
  let assert Ok(position) = result
  position |> should.equal(types.position(1, 3))
  let #(_, result) = node.propose_config(n, voters_config([0, 1]))
  result |> should.equal(Error(types.JointInProgress))
  // 拒否経路ではログが進まない。
  node.log(n) |> log.log_last_position |> should.equal(types.position(1, 3))
}

// 自作テスト: get_commit_status の 4 状態。
pub fn get_commit_status_reports_four_states_test() -> Nil {
  let n = leader_with(0, [1])
  // InProgress: follower の返信が無く未 commit。
  let #(n, position) = node.propose_command(n, "cmd")
  let assert Some(position) = position
  node.get_commit_status(n, position) |> should.equal(types.InProgress)

  // Committed: 返信で commit されたら Committed。commit 以下の位置
  // （項マーカーと初期構成）も Committed。
  let reply =
    message.AppendEntriesReply(term: 1, from: 1, last_log_position: position)
  let assert Ok(n) = node.handle_message(n, reply)
  node.get_commit_status(n, position) |> should.equal(types.Committed)
  node.get_commit_status(n, types.position(1, 2))
  |> should.equal(types.Committed)
  node.get_commit_status(n, types.position(0, 1))
  |> should.equal(types.Committed)

  // Rejected: 同じ index が高項 leader の複製で別項に上書きされる。
  let overwrite =
    message.AppendEntriesCall(
      term: 2,
      from: 1,
      leader_commit: 2,
      entries: log.entries_from_list(types.position(1, 2), [log.TermEntry(2)]),
    )
  let assert Ok(n) = node.handle_message(n, overwrite)
  node.get_commit_status(n, position) |> should.equal(types.Rejected)

  // Unknown: anchor より前の位置はスナップショット剪定で作る
  //   （Unknown は「ログから落ちて復元不能」だけを指す）。
  let cfg = voters_config([0, 1])
  let #(n, accepted) =
    node.handle_snapshot_installed(n, types.position(2, 5), cfg)
  accepted |> should.be_true
  node.get_commit_status(n, types.position(1, 2))
  |> should.equal(types.Unknown)
}

pub fn snapshot_install_syncs_commit_index_on_follower_test() -> Nil {
  // handle_snapshot_installed は current_term より高い term のスナップ
  // ショットを拒否するので、follower はスナップショットの term から
  // 始める。呼び出し側の前提（高 term のメッセージを先に処理する）を
  // そのまま再現している。
  let n0 = follower(0, [0, 1], 2, None) |> drain
  node.commit_index(n0) |> should.equal(0)

  // スナップショットが保持ログより先なので、境界起点の空断片に張り替わる。
  let snapshot = types.position(2, 5)
  let cfg = voters_config([0, 1])
  let #(n, accepted) = node.handle_snapshot_installed(n0, snapshot, cfg)
  accepted |> should.be_true
  node.commit_index(n) |> should.equal(5)

  let restarted = node.restart(0, node.current_term(n), None, node.log(n))
  node.commit_index(restarted) |> should.equal(node.commit_index(n))
  node.get_commit_status(n, snapshot) |> should.equal(types.Committed)

  // 同じスナップショットの再導入で commit_index を後退させてはならない。
  let #(n2, accepted2) = node.handle_snapshot_installed(n, snapshot, cfg)
  accepted2 |> should.be_true
  node.commit_index(n2) |> should.equal(5)
}

pub fn snapshot_install_syncs_commit_index_on_candidate_test() -> Nil {
  let n0 = candidate_with(0, [1])
  node.role(n0) |> should.equal(types.Candidate)
  node.commit_index(n0) |> should.equal(0)

  let snapshot = types.position(node.current_term(n0), 5)
  let #(n, accepted) =
    node.handle_snapshot_installed(n0, snapshot, voters_config([0, 1]))
  accepted |> should.be_true
  node.commit_index(n) |> should.equal(5)
  node.get_commit_status(n, snapshot) |> should.equal(types.Committed)
}

pub fn snapshot_install_keeps_log_suffix_and_advances_commit_index_test() -> Nil {
  let cfg = voters_config([0, 1])
  let entries =
    log.entries_from_list(types.zero_position, [
      log.ConfigEntry(cfg),
      log.TermEntry(1),
      log.CommandEntry("a"),
      log.CommandEntry("b"),
      log.CommandEntry("c"),
    ])
  let n0 =
    node.restart(0, 1, None, log.log_new(cfg, entries))
    |> drain
  node.commit_index(n0) |> should.equal(0)

  // 境界がログ内なので suffix（index 4, 5 のコマンド 2 件）が残る。
  let snapshot = types.position(1, 3)
  let #(n, accepted) = node.handle_snapshot_installed(n0, snapshot, cfg)
  accepted |> should.be_true
  node.commit_index(n) |> should.equal(3)
  node.log(n) |> log.log_snapshot_position |> should.equal(snapshot)
  node.log(n) |> log.log_last_position |> should.equal(types.position(1, 5))

  let restarted = node.restart(0, node.current_term(n), None, node.log(n))
  node.commit_index(restarted) |> should.equal(node.commit_index(n))
}

pub fn snapshot_install_does_not_regress_commit_index_on_leader_test() -> Nil {
  let l0 = leader_with(0, [])
  let #(l1, first_opt) = node.propose_command(l0, "c1")
  let #(l2, second_opt) = node.propose_command(l1, "c2")
  let assert Some(first) = first_opt
  let assert Some(second) = second_opt
  let l = drain(l2)
  let before = node.commit_index(l)
  before |> should.equal(second.index)
  should.be_true(first.index < before)

  // commit_index より厳格に低い位置で compact する。leader の
  // is_valid_snapshot は commit >= 境界を要求するので、max は commit を
  // second.index に据え置く。単なる代入なら落ちるテスト。
  let #(l2b, accepted) =
    node.handle_snapshot_installed(l, first, node.config(l))
  accepted |> should.be_true
  node.commit_index(l2b) |> should.equal(before)
}

pub fn snapshot_install_rejects_snapshot_with_higher_term_test() -> Nil {
  let n0 = follower(0, [0, 1], 1, None) |> drain
  let before_log = node.log(n0)

  // term 2 > current_term 1 のスナップショットは拒否。
  let #(n, accepted) =
    node.handle_snapshot_installed(
      n0,
      types.position(2, 5),
      voters_config([0, 1]),
    )
  accepted |> should.be_false

  // 拒否時は観測可能な状態変化が 1 つもない。
  node.current_term(n) |> should.equal(1)
  node.voted_for(n) |> should.equal(None)
  node.role(n) |> should.equal(types.Follower)
  node.commit_index(n) |> should.equal(0)
  node.log(n) |> should.equal(before_log)
  node.actions(n) |> action.actions_is_empty |> should.be_true
}

pub fn snapshot_install_does_not_raise_queued_call_term_above_current_term_test() -> Nil {
  let n0 = follower(0, [0, 1], 0, None) |> drain
  let n1 = node.handle_election_timeout(n0)
  node.role(n1) |> should.equal(types.Candidate)
  let candidate_term = node.current_term(n1)

  let assert Some(queued) = node.actions(n1).broadcast_message
  let assert message.RequestVoteCall(term: queued_term, ..) = queued
  queued_term |> should.equal(candidate_term)

  // 同項のスナップショットで張替えても、保留 Call の term をスナップ
  // ショットの位置項へ吊り上げてはならない（メッセージの term は不変）。
  let snapshot = types.position(candidate_term, 5)
  let #(n2, accepted) =
    node.handle_snapshot_installed(n1, snapshot, voters_config([0, 1]))
  accepted |> should.be_true

  let assert Some(after) = node.actions(n2).broadcast_message
  let assert message.RequestVoteCall(term: queued_after, ..) = after
  should.be_true(queued_after <= node.current_term(n2))
  queued_after |> should.equal(candidate_term)
}

pub fn snapshot_install_after_transition_does_not_produce_self_inconsistent_call_test() -> Nil {
  // 候補が同項より高い AppendEntriesCall で降格された後、スナップショット
  // 導入が自己非整合の Call（自分の term より新しい位置を持つ
  // RequestVoteCall）を復活させないことを確かめる。降格時の purge が先に
  // 保留 Call を消している（固定点 6 と固定点 10 の協調）。
  let n0 = follower(0, [0, 1], 0, None) |> drain
  let n1 = node.handle_election_timeout(n0)
  node.role(n1) |> should.equal(types.Candidate)
  let assert Some(_) = node.actions(n1).broadcast_message

  let bump =
    message.AppendEntriesCall(
      term: 3,
      from: 1,
      leader_commit: 0,
      entries: log.entries_from_list(types.zero_position, []),
    )
  let assert Ok(n2) = node.handle_message(n1, bump)
  node.role(n2) |> should.equal(types.Follower)
  node.current_term(n2) |> should.equal(3)
  node.actions(n2).broadcast_message |> should.equal(None)

  let #(n3, accepted) =
    node.handle_snapshot_installed(
      n2,
      types.position(2, 5),
      voters_config([0, 1]),
    )
  accepted |> should.be_true
  node.actions(n3).broadcast_message |> should.equal(None)
}

// 自作テスト: M3-3 で残した InstallSnapshot 分岐の到達性。leader のログを
// スナップショットで compact すると、取り残された遅れ follower の
// AppendEntriesReply は「遅延でも発散でも leader 超過でもない」ので、
// InstallSnapshot として処理するしかない。
pub fn leader_snapshot_then_lagging_reply_queues_install_snapshot_test() -> Nil {
  let l0 = leader_with(0, [1, 2])
  let #(l1, c1_opt) = node.propose_command(l0, "c1")
  let assert Some(c1_pos) = c1_opt
  c1_pos |> should.equal(types.position(1, 3))

  // follower 2 が (1,3) まで追いつき、leader 自身とで多数派が成立して
  // commit が進む。
  let reply2 =
    message.AppendEntriesReply(term: 1, from: 2, last_log_position: c1_pos)
  let assert Ok(l2) = node.handle_message(l1, reply2)
  let l = drain(l2)
  node.commit_index(l) |> should.equal(3)

  // (1,3) で compact。anchor が (1,3) になり、follower 1 の match_index は
  // anchor より後ろのまま（install は追跡状態を触らない）。
  let #(l3, accepted) =
    node.handle_snapshot_installed(l, c1_pos, node.config(l))
  accepted |> should.be_true
  node.commit_index(l3) |> should.equal(3)

  // follower 1 の (1,2) 返信は match_index 0 と比較して遅延ではなく、
  // leader の保持範囲外なので発散とも違う。InstallSnapshot が積まる。
  let stale =
    message.AppendEntriesReply(
      term: 1,
      from: 1,
      last_log_position: types.position(1, 2),
    )
  let assert Ok(l4) = node.handle_message(l3, stale)
  node.actions(l4).install_snapshots
  |> set.to_list
  |> should.equal([1])
  node.metrics(l4).log_divergences_detected_by_leader |> should.equal(0)
}
