// モデル一致性質テスト 11 本（TESTPLAN §3.2 の L 行）。gleam-raft の公開 API
// に対する性質テストで、共通の方針は次のとおり。
//
//   - 乱数: support/rng.gleam と support/gens.gleam が供給する。各ケースは
//     seed_for_case(base, i) の専用シードから起こした rng を使う（決定論）。
//   - シード/ケース数: support/pbt_env.gleam（GLEAM_RAFT_PBT_SEED /
//     GLEAM_RAFT_PBT_CASES）から読む。既定ケース数は CI の実行時間に合わせて
//     64 にしている。
//   - Rep 行の checked_* 系 4 本は D6（u64 境界の不在・Int は 2^53−1 上限）
//     のため対象外とし、値域は support/gens.gleam の sim_max で頭打ちにする。
//   - D5: index を進める専用メソッドは無く、+1 は int 素の演算になる。
//     log_index_next_advances_by_one は +1 規律の検証として position_next
//     と素の +1 の一致で局所化する。
//   - 失敗メッセージのシード・位置は 10 進で表記する。

import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/set
import gleam/string
import gleam_raft/cluster_config.{
  type ClusterConfig, ClusterConfig, config_new, contains, is_joint_consensus,
  to_joint_consensus, unique_nodes,
}
import gleam_raft/internal/node.{type Node}
import gleam_raft/log.{
  type LogEntries, type LogEntry, ConfigEntry, TermEntry, entries_from_list,
  entries_get_entry, entries_get_term, entries_iter_with_positions, entries_len,
  entries_since, entries_truncate, log_append_suffix,
  log_get_position_and_config, log_last_position, log_latest_config, log_new,
  log_snapshot_position,
}
import gleam_raft/message.{
  AppendEntriesCall, RequestVoteCall, strip_append_entries_prefix,
}
import gleam_raft/support/gens
import gleam_raft/support/pbt_env
import gleam_raft/support/rng.{type Rng}
import gleam_raft/types.{
  type LogPosition, Committed, Follower, Leader, position, position_next,
  zero_position,
}

// ---- 共通ヘルパ ----

/// 偽なら msg を Error に込める。
fn require_ok(cond: Bool, msg: String) -> Result(Nil, String) {
  case cond {
    True -> Ok(Nil)
    False -> Error(msg)
  }
}

/// 0..n-1 の昇順リスト（stdlib 1.0.5 に list.range がないため自作）。
fn upto(n: Int) -> List(Int) {
  upto_loop(n, [])
}

fn upto_loop(k: Int, acc: List(Int)) -> List(Int) {
  case k {
    x if x <= 0 -> acc
    x -> upto_loop(x - 1, [x - 1, ..acc])
  }
}

/// ケース Driver。各ケースは seed_for_case(base, i) の専用 rng を受け取り、
/// 失敗時は "case i seed=s: " を接頭辞して打ち切る
/// （cluster_property_test の run_cases の rng 版）。
fn run_model_cases(
  base: Int,
  count: Int,
  each: fn(Rng) -> Result(Nil, String),
) -> Result(Nil, String) {
  list.try_fold(upto(count), Nil, fn(_acc: Nil, i: Int) {
    let seed = rng.seed_for_case(base, i)
    case each(rng.from_seed(seed)) {
      Ok(Nil) -> Ok(Nil)
      Error(msg) ->
        Error(
          "case "
          <> int.to_string(i)
          <> " seed="
          <> int.to_string(seed)
          <> ": "
          <> msg,
        )
    }
  })
}

fn show_ints(items: List(Int)) -> String {
  items |> list.map(int.to_string) |> string.join(",")
}

fn show_position(p: LogPosition) -> String {
  "term=" <> int.to_string(p.term) <> " index=" <> int.to_string(p.index)
}

fn show_config(config: ClusterConfig) -> String {
  "voters=["
  <> show_ints(set.to_list(config.voters))
  <> "] new_voters=["
  <> show_ints(set.to_list(config.new_voters))
  <> "] non_voters=["
  <> show_ints(set.to_list(config.non_voters))
  <> "]"
}

fn role_name(role: types.Role) -> String {
  case role {
    Follower -> "Follower"
    types.Candidate -> "Candidate"
    Leader -> "Leader"
  }
}

/// 各要素を 1/2 の確率で残すフィルタ。
fn filter_by_bool(r: Rng, items: List(a)) -> #(Rng, List(a)) {
  case items {
    [] -> #(r, [])
    [first, ..rest] -> {
      let #(r2, keep) = rng.rand_bool(r)
      let #(r3, tail) = filter_by_bool(r2, rest)
      case keep {
        True -> #(r3, [first, ..tail])
        False -> #(r3, tail)
      }
    }
  }
}

/// 接続点 prev_position から最大 max_len 件のログ断片を生成する。
fn sample_entries_from(
  r0: Rng,
  prev_position: LogPosition,
  max_len: Int,
) -> #(Rng, LogEntries(Int)) {
  let #(r1, count) = gens.run(gens.sample_len(max_len), r0)
  sample_entries_loop(r1, count, [], prev_position)
}

fn sample_entries_loop(
  r: Rng,
  k: Int,
  acc: List(LogEntry(Int)),
  prev_position: LogPosition,
) -> #(Rng, LogEntries(Int)) {
  case k {
    0 -> #(r, entries_from_list(prev_position, acc))
    _ -> {
      let #(r2, entry) = gens.run(gens.sample_log_entry(), r)
      sample_entries_loop(r2, k - 1, [entry, ..acc], prev_position)
    }
  }
}

/// 断片に含まれる位置を 1 つ引く。
fn sample_contained_position(
  r: Rng,
  entries: LogEntries(Int),
) -> #(Rng, LogPosition) {
  let #(r2, index) =
    gens.run(
      gens.int(entries.prev_position.index, entries.last_position.index),
      r,
    )
  let term = case entries_get_term(entries, index) {
    Some(term) -> term
    None -> panic as "a sampled contained index must have a term"
  }
  #(r2, position(term, index))
}

/// 項番号を必ず変える。D6: u64 のような上限は無く、sim_max を上限として
/// 折り返す。
fn different_term(term: Int) -> Int {
  case term == gens.sim_max {
    True -> 0
    False -> term + 1
  }
}

// ---- テスト 1, 2: クラスタ構成 ----

/// unique_nodes は voters・new_voters・non_voters の和集合そのもの。
/// joint 中の重なりを含む全空間を生成する。
fn unique_nodes_case(r0: Rng) -> Result(Nil, String) {
  use _ <- result.try(require_ok(
    unique_nodes(config_new()) == [],
    "an empty config yielded nodes: " <> show_ints(unique_nodes(config_new())),
  ))
  let #(_r, sampled) = gens.run(gens.sample_config(), r0)
  // 2 ノードの重なりを注入する: 100 は voters/new_voters 両方、
  // 101 は 3 集合すべてに属する。
  let config =
    ClusterConfig(
      voters: sampled.voters |> set.insert(100) |> set.insert(101),
      new_voters: sampled.new_voters |> set.insert(100) |> set.insert(101),
      non_voters: sampled.non_voters |> set.insert(101),
    )
  // 仕様（unique_nodes）は「union を昇順ソート」なので、モデル側も比較前に
  // ソートする（JS 側の set.to_list は挿入順になり得るため、Erlang/JS 双方で
  // 同じ検証になる）。
  let expected =
    set.union(set.union(config.voters, config.new_voters), config.non_voters)
    |> set.to_list
    |> list.sort(int.compare)
  use _ <- result.try(require_ok(
    unique_nodes(config) == expected,
    "unique_nodes returned "
      <> show_ints(unique_nodes(config))
      <> ", expected "
      <> show_ints(expected),
  ))
  use _ <- result.try(require_ok(
    list.all(expected, fn(id) { contains(config, id) }),
    "contains rejected a member of the union: " <> show_config(config),
  ))
  require_ok(
    is_joint_consensus(config) != set.is_empty(config.new_voters),
    "is_joint_consensus disagrees with new_voters: " <> show_config(config),
  )
}

pub fn cluster_config_unique_nodes_matches_set_union_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), unique_nodes_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// to_joint_consensus は旧 voters を保ち、new_voters = (旧 + 追加) - 除去、
/// non_voters は昇格分を除いて集合モデルと一致する。D1: 前条件判定は
/// 第 2 要素の Bool で返る。sampler は前条件を満たす生成のみ行う。
fn to_joint_case(r0: Rng) -> Result(Nil, String) {
  let #(r_after_config, base) = gens.run(gens.sample_normal_config(), r0)
  // 新 id と既存 non_voters を混ぜたプールから 1/2 で追加を選ぶ。同じ
  // プールに既存の non_voters を入れることで、昇格分岐も踏む。
  let pool = list.append([100, 101, 102, 103], set.to_list(base.non_voters))
  let #(r1, additions) = filter_by_bool(r_after_config, pool)
  let #(_r2, removals) = filter_by_bool(r1, set.to_list(base.voters))
  let adds = set.from_list(additions)
  let rems = set.from_list(removals)
  // sample_normal_config の不変条件（非 joint・voters と non_voters 相互素・
  // id 0..5）から前条件は必ず成立する。成立確認を明示して、構成の値と
  // しての整合をそのまま検査する。
  use _ <- result.try(require_ok(
    !is_joint_consensus(base)
      && set.is_empty(set.intersection(adds, rems))
      && set.is_empty(set.intersection(adds, base.voters))
      && set.is_subset(rems, of: base.voters),
    "sampler produced invalid preconditions: " <> show_config(base),
  ))
  let expected_new_voters = set.difference(set.union(base.voters, adds), rems)
  let expected_non_voters = set.difference(base.non_voters, expected_new_voters)
  let #(joint, applied) = to_joint_consensus(base, additions, removals)
  use _ <- result.try(require_ok(
    applied,
    "to_joint_consensus rejected a valid precondition set: "
      <> show_config(base),
  ))
  require_ok(
    joint.voters == base.voters
      && joint.new_voters == expected_new_voters
      && joint.non_voters == expected_non_voters,
    "joint config mismatch: got "
      <> show_config(joint)
      <> ", expected new_voters="
      <> show_ints(set.to_list(expected_new_voters))
      <> " non_voters="
      <> show_ints(set.to_list(expected_non_voters)),
  )
}

pub fn cluster_config_to_joint_consensus_matches_set_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), to_joint_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 3〜9: ログ ----

/// 反復・index 参照・最終位置が同じ列を指すこと。
fn iteration_case(r0: Rng) -> Result(Nil, String) {
  let #(_r1, entries) = sample_entries_from(r0, zero_position, 64)
  let iterated = entries_iter_with_positions(entries)
  use _ <- result.try(require_ok(
    list.length(iterated) == entries_len(entries),
    "iteration returned "
      <> int.to_string(list.length(iterated))
      <> " entries, expected "
      <> int.to_string(entries_len(entries)),
  ))
  check_iteration_loop(iterated, entries.prev_position, entries)
}

fn check_iteration_loop(
  iterated: List(#(LogPosition, LogEntry(Int))),
  previous: LogPosition,
  entries: LogEntries(Int),
) -> Result(Nil, String) {
  case iterated {
    [] ->
      require_ok(
        entries.last_position == previous,
        "last_position "
          <> show_position(entries.last_position)
          <> " does not match the iterated tail "
          <> show_position(previous),
      )
    [#(position_, entry), ..rest] -> {
      let expected_term = case entry {
        TermEntry(term) -> term
        _ -> previous.term
      }
      use _ <- result.try(require_ok(
        position_.index == previous.index + 1,
        "indices must advance by one: "
          <> show_position(position_)
          <> " after "
          <> show_position(previous),
      ))
      use _ <- result.try(require_ok(
        position_.term == expected_term,
        "position term mismatch: "
          <> show_position(position_)
          <> ", expected term="
          <> int.to_string(expected_term),
      ))
      use _ <- result.try(require_ok(
        entries_get_entry(entries, position_.index) == Some(entry),
        "get_entry("
          <> int.to_string(position_.index)
          <> ") did not return the iterated entry",
      ))
      check_iteration_loop(rest, position_, entries)
    }
  }
}

pub fn log_entries_iteration_matches_indexed_lookup_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), iteration_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// 切り詰めは要求した prefix を最終位置まで正確に残す。len は 0・全件・
/// 全件 + 余裕の len+4 を境界値として 1/5 の重みで引く。
fn truncate_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, entries) = sample_entries_from(r0, zero_position, 32)
  let before = entries_iter_with_positions(entries)
  let held = list.length(before)
  let max_requested = held + 4
  let #(_r2, len) =
    gens.run(
      gens.with_boundaries(
        [0, held, max_requested],
        5,
        gens.int(0, max_requested),
      ),
      r1,
    )
  let expected = list.take(before, int.min(len, held))
  let expected_last = case list.last(expected) {
    Ok(#(kept_position, _)) -> kept_position
    Error(_) -> entries.prev_position
  }
  let #(after, returned_last) = entries_truncate(entries, len)
  use _ <- result.try(require_ok(
    entries_iter_with_positions(after) == expected,
    "truncate("
      <> int.to_string(len)
      <> ") kept "
      <> int.to_string(list.length(entries_iter_with_positions(after)))
      <> " entries, expected "
      <> int.to_string(list.length(expected)),
  ))
  require_ok(
    after.last_position == expected_last && returned_last == expected_last,
    "truncate("
      <> int.to_string(len)
      <> ") ended at "
      <> show_position(after.last_position)
      <> " (returned "
      <> show_position(returned_last)
      <> "), expected "
      <> show_position(expected_last),
  )
}

pub fn log_entries_truncate_matches_prefix_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), truncate_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// since は含まれる位置の直後からの正確な後継断片を返し、同じ index に
/// 違う項を合わせると必ず断る。
fn since_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, entries) = sample_entries_from(r0, zero_position, 32)
  let #(_r2, position_) = sample_contained_position(r1, entries)
  case entries_since(entries, position_) {
    None ->
      Error(
        "since("
        <> show_position(position_)
        <> ") rejected a contained position",
      )
    Some(suffix) -> {
      let expected =
        list.filter(
          entries_iter_with_positions(entries),
          fn(item: #(LogPosition, LogEntry(Int))) {
            item.0.index > position_.index
          },
        )
      use _ <- result.try(require_ok(
        suffix.prev_position == position_,
        "since("
          <> show_position(position_)
          <> ") produced prev="
          <> show_position(suffix.prev_position),
      ))
      use _ <- result.try(require_ok(
        entries_iter_with_positions(suffix) == expected,
        "since("
          <> show_position(position_)
          <> ") produced a different entry list than the suffix model",
      ))
      use _ <- result.try(require_ok(
        suffix.last_position == entries.last_position,
        "suffix ended at "
          <> show_position(suffix.last_position)
          <> ", expected "
          <> show_position(entries.last_position),
      ))
      let mismatched = position(different_term(position_.term), position_.index)
      require_ok(
        entries_since(entries, mismatched) == None,
        "since(" <> show_position(mismatched) <> ") accepted a mismatched term",
      )
    }
  }
}

pub fn log_entries_since_matches_suffix_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), since_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// 有効な anchor の suffix は局所側をその位置で切り捨てて置換する。
/// 無効な anchor はログを一切変えない。
fn append_suffix_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, snapshot_config) = gens.run(gens.sample_config(), r0)
  let #(r2, entries) = sample_entries_from(r1, zero_position, 32)
  let original = log_new(snapshot_config, entries)
  let #(r3, anchor) = sample_contained_position(r2, original.entries)
  let #(r4, suffix) = sample_entries_from(r3, anchor, 16)
  let expected_entries =
    entries_from_list(
      log_snapshot_position(original),
      list.append(
        original.entries
          |> entries_iter_with_positions
          |> list.take_while(fn(item) { item.0.index <= anchor.index })
          |> list.map(fn(item) { item.1 }),
        suffix.entries,
      ),
    )
  let expected = log_new(snapshot_config, expected_entries)
  let #(actual, applied) = log_append_suffix(original, suffix)
  use _ <- result.try(require_ok(
    applied,
    "append_suffix rejected valid anchor " <> show_position(anchor),
  ))
  use _ <- result.try(require_ok(
    actual == expected,
    "append_suffix result mismatch: actual "
      <> show_position(log_last_position(actual))
      <> "-kept, expected "
      <> show_position(log_last_position(expected)),
  ))
  // 無効 anchor（保持範囲の直外）はログを動かさない。
  let invalid_anchor = position(0, log_last_position(original).index + 1)
  let #(_r5, invalid_suffix) = sample_entries_from(r4, invalid_anchor, 16)
  let #(unchanged, rejected) = log_append_suffix(original, invalid_suffix)
  require_ok(
    !rejected && unchanged == original,
    "append_suffix changed a log for an invalid anchor",
  )
}

pub fn log_append_suffix_matches_replacement_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), append_suffix_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// latest_config と get_position_and_config は素朴な線形スキャンと一致し、
/// 保持範囲外は None を返す。get_config も同じ線形スキャンで照合する。
fn config_queries_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, snapshot_config) = gens.run(gens.sample_config(), r0)
  let #(r2, entries) = sample_entries_from(r1, zero_position, 32)
  let log_ = log_new(snapshot_config, entries)
  let iterated = entries_iter_with_positions(log_.entries)
  let latest_model =
    list.fold(
      iterated,
      snapshot_config,
      fn(latest: ClusterConfig, item: #(LogPosition, LogEntry(Int))) {
        case item.1 {
          ConfigEntry(config:) -> config
          _ -> latest
        }
      },
    )
  use _ <- result.try(require_ok(
    log_latest_config(log_) == latest_model,
    "latest_config returned "
      <> show_config(log_latest_config(log_))
      <> ", expected "
      <> show_config(latest_model),
  ))
  let #(_r3, target) = sample_contained_position(r2, log_.entries)
  let expected_config =
    list.fold(
      list.take_while(iterated, fn(item) { item.0.index <= target.index }),
      snapshot_config,
      fn(acc: ClusterConfig, item: #(LogPosition, LogEntry(Int))) {
        case item.1 {
          ConfigEntry(config:) -> config
          _ -> acc
        }
      },
    )
  case log_get_position_and_config(log_, target.index) {
    None -> Error("a contained index must resolve: " <> show_position(target))
    Some(#(actual_position, actual_config)) -> {
      use _ <- result.try(require_ok(
        actual_position == target && actual_config == expected_config,
        "config query mismatch at index="
          <> int.to_string(target.index)
          <> ": got ("
          <> show_position(actual_position)
          <> ", "
          <> show_config(actual_config)
          <> "), expected ("
          <> show_position(target)
          <> ", "
          <> show_config(expected_config)
          <> ")",
      ))
      let beyond = position_next(log_last_position(log_)).index
      require_ok(
        log_get_position_and_config(log_, beyond) == None,
        "out-of-range index="
          <> int.to_string(beyond)
          <> " resolved unexpectedly",
      )
    }
  }
}

pub fn log_config_queries_match_scan_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), config_queries_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// position の next は index だけを 1 進め、項を保つ。D6: +1 の精度が
/// 保たれるよう index は 2^53−2 以下から引く。
fn position_next_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, term) = gens.run(gens.sample_term_or_index(), r0)
  let #(_r2, index) =
    gens.run(
      gens.with_boundaries(
        [0, 1, gens.sim_max - 1],
        5,
        gens.int(0, gens.sim_max - 1),
      ),
      r1,
    )
  let current = position(term, index)
  let next = position_next(current)
  require_ok(
    next.index == index + 1 && next.term == term,
    "next(" <> show_position(current) <> ") returned " <> show_position(next),
  )
}

pub fn log_position_next_advances_index_only_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), position_next_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

/// index の +1 規律。D5: gleam-raft に LogIndex::next() 相当のメソッドは
/// 無く、+1 は int 素の演算。+1 規律の実際の載りである position_next が
/// 素の +1 と一致することを検証する（D6: 値域は 2^53−2 以下）。
fn index_next_case(r0: Rng) -> Result(Nil, String) {
  let #(_r1, index) =
    gens.run(
      gens.with_boundaries(
        [0, 1, gens.sim_max - 1],
        5,
        gens.int(0, gens.sim_max - 1),
      ),
      r0,
    )
  let via_position = position_next(position(0, index))
  require_ok(
    via_position == position(0, index + 1),
    "next(index="
      <> int.to_string(index)
      <> ") returned "
      <> show_position(via_position),
  )
}

pub fn log_index_next_advances_by_one_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), index_next_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 10: AppendEntries prefix 除去 ----

/// AE Call の prefix 除去は送信済み後続の正確な suffix を生み、メタデータ
/// を保つ。AE Call 以外は絶対に変わらない。送信済み件数は 0・全件・
/// 全件 + 余裕の count+4 を境界値として引く。
fn strip_prefix_case(r0: Rng) -> Result(Nil, String) {
  let #(r1, count) = gens.run(gens.sample_len(64), r0)
  let #(r2, prev_term) = gens.run(gens.sample_term_or_index(), r1)
  let #(r3, prev_index) = gens.run(gens.int(0, 1024), r2)
  let #(r4, sampled) =
    sample_entries_loop(r3, count, [], position(prev_term, prev_index))
  let prev_position = sampled.prev_position
  let before = entries_iter_with_positions(sampled)
  let #(r5, sent_count) =
    gens.run(
      gens.with_boundaries([0, count, count + 4], 5, gens.int(0, count + 4)),
      r4,
    )
  let dropped = int.min(sent_count, count)
  let expected_prev = case list.last(list.take(before, dropped)) {
    Ok(#(kept_position, _)) -> kept_position
    Error(_) -> prev_position
  }
  let expected_entries =
    entries_from_list(
      expected_prev,
      list.map(list.drop(before, dropped), fn(item) { item.1 }),
    )
  let #(r6, from) = gens.run(gens.int(0, gens.sim_max), r5)
  let #(r7, term) = gens.run(gens.sample_term_or_index(), r6)
  let #(_r8, leader_commit) = gens.run(gens.sample_term_or_index(), r7)
  let message =
    AppendEntriesCall(term:, from:, leader_commit:, entries: sampled)
  let #(stripped, applied) = strip_append_entries_prefix(message, sent_count)
  use _ <- result.try(require_ok(
    applied,
    "AppendEntriesCall was not recognized",
  ))
  use _ <- result.try(require_ok(
    stripped
      == AppendEntriesCall(
      term:,
      from:,
      leader_commit:,
      entries: expected_entries,
    ),
    "strip_append_entries_prefix("
      <> int.to_string(sent_count)
      <> ") mismatch: ended at "
      <> show_position(log_entries_last(stripped))
      <> ", expected "
      <> show_position(expected_prev),
  ))
  let request_vote =
    RequestVoteCall(term:, from:, last_log_position: expected_prev)
  let #(unchanged, touched) =
    strip_append_entries_prefix(request_vote, sent_count)
  require_ok(
    !touched && unchanged == request_vote,
    "prefix stripping changed a non-AppendEntries message",
  )
}

/// 除去後の AE entries の末尾位置（失敗メッセージ用）。
fn log_entries_last(msg: message.Message(Int)) -> LogPosition {
  case msg {
    AppendEntriesCall(entries:, ..) -> entries.last_position
    _ -> zero_position
  }
}

pub fn append_entries_prefix_stripping_matches_model_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), strip_prefix_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

// ---- テスト 11: 単独ノードの有効遷移 ----

/// 単独 voter ノードへ有効な操作だけを加えたとき、台帳状態の不変条件が
/// 保たれること。操作は 4 種（コマンド提案・同一構成提案・項送り・
/// ハートビート）を重み [4,1,1,1] で引く。ステップ数は最大 200 で、setup
/// では必ず 1 件の提案と 1 回の項送りを行う。
type SoloState {
  SoloState(
    r: Rng,
    n: Node(Int),
    step: Int,
    steps: Int,
    term: Int,
    commit: Int,
    last_index: Int,
  )
}

/// 生成された Action を全部飲み切る。ハーネス側は Action の中身を検証せず、
/// 状態の単調性だけ見るので捨ててよい。
fn drain(n: Node(Int)) -> Node(Int) {
  case node.take_action(n) {
    #(after, None) -> after
    #(after, Some(_)) -> drain(after)
  }
}

/// 項送り: NodeId 1（検証対象は 0 なので自分宛にならない）から「現在の項
/// +1・フルログ」の投票要求を通し、選挙タイムアウトで自勝リーダーに
/// 戻す。
fn advance_term(n: Node(Int)) -> #(Node(Int), Result(Nil, String)) {
  let requested = node.current_term(n) + 1
  let request =
    RequestVoteCall(
      term: requested,
      from: 1,
      last_log_position: log_last_position(node.log(n)),
    )
  case node.handle_message(n, request) {
    Error(_) -> #(n, Error("a higher-term vote request was not handled"))
    Ok(after_message) ->
      case win_next_term_election(after_message, requested) {
        Ok(after_win) -> #(after_win, Ok(Nil))
        Error(msg) -> #(after_message, Error(msg))
      }
  }
}

fn win_next_term_election(
  after_message: Node(Int),
  requested: Int,
) -> Result(Node(Int), String) {
  use _ <- result.try(require_ok(
    node.current_term(after_message) == requested
      && node.role(after_message) == Follower,
    "a higher-term vote request did not make a follower in term "
      <> int.to_string(requested)
      <> ": term="
      <> int.to_string(node.current_term(after_message))
      <> " role="
      <> role_name(node.role(after_message)),
  ))
  let after_timeout = node.handle_election_timeout(drain(after_message))
  use _ <- result.try(require_ok(
    node.current_term(after_timeout) > requested
      && node.role(after_timeout) == Leader,
    "the solo follower lost the next election: requested term="
      <> int.to_string(requested)
      <> ": term="
      <> int.to_string(node.current_term(after_timeout))
      <> " role="
      <> role_name(node.role(after_timeout)),
  ))
  Ok(drain(after_timeout))
}

fn solo_case(r0: Rng) -> Result(Nil, String) {
  let fresh = node.start(0)
  let #(after_create, created) = node.create_cluster(fresh, [0])
  use _created_position <- result.try(case created {
    Ok(placed) -> Ok(placed)
    Error(_) -> Error("creating a solo cluster did not place the initial entry")
  })
  let after_setup = drain(after_create)
  use _ <- result.try(require_ok(
    node.role(after_setup) == Leader,
    "creating a solo cluster did not produce a leader, got role "
      <> role_name(node.role(after_setup)),
  ))
  let baseline = node.commit_index(after_setup)
  let #(after_first, proposed) = node.propose_command(after_setup, 0)
  use first_position <- result.try(case proposed {
    Some(placed) -> Ok(placed)
    None -> Error("the mandatory post-setup proposal was rejected")
  })
  let after_first_settled = drain(after_first)
  use _ <- result.try(require_ok(
    node.get_commit_status(after_first_settled, first_position) == Committed,
    "the mandatory post-setup proposal did not commit at "
      <> show_position(first_position),
  ))
  use _ <- result.try(require_ok(
    node.commit_index(after_first_settled) > baseline,
    "the mandatory post-setup proposal did not advance the commit index",
  ))
  use after_term <- result.try(case advance_term(after_first_settled) {
    #(after, Ok(Nil)) -> Ok(after)
    #(_, Error(msg)) -> Error(msg)
  })
  let #(r1, steps) = gens.run(gens.sample_len(200), r0)
  solo_loop(SoloState(
    r: r1,
    n: after_term,
    step: 0,
    steps:,
    term: node.current_term(after_term),
    commit: node.commit_index(after_term),
    last_index: log_last_position(node.log(after_term)).index,
  ))
}

fn solo_loop(state: SoloState) -> Result(Nil, String) {
  case state.step >= state.steps {
    True -> Ok(Nil)
    False -> {
      let #(r2, kind) = gens.run(gens.weighted_index([4, 1, 1, 1]), state.r)
      let #(op_name, after_op, outcome) = case kind {
        0 -> {
          let #(after, placed) = node.propose_command(state.n, state.step)
          case placed {
            Some(placed_position) -> #(
              "propose_command",
              after,
              require_ok(
                node.get_commit_status(after, placed_position) == Committed,
                "a command proposal did not commit at step "
                  <> int.to_string(state.step),
              ),
            )
            None -> #(
              "propose_command",
              state.n,
              Error(
                "a command proposal was rejected at step "
                <> int.to_string(state.step),
              ),
            )
          }
        }
        1 -> {
          let #(after, proposed) =
            node.propose_config(state.n, node.config(state.n))
          case proposed {
            Ok(placed_position) -> #(
              "propose_config",
              after,
              require_ok(
                node.get_commit_status(after, placed_position) == Committed,
                "a same-config proposal did not commit at step "
                  <> int.to_string(state.step),
              ),
            )
            Error(_) -> #(
              "propose_config",
              state.n,
              Error(
                "a same-config proposal was rejected at step "
                <> int.to_string(state.step),
              ),
            )
          }
        }
        2 -> {
          let #(after, advanced) = advance_term(state.n)
          #("advance_term", after, advanced)
        }
        _ -> {
          let #(after, beat) = node.heartbeat(state.n)
          #(
            "heartbeat",
            after,
            require_ok(
              beat,
              "a leader heartbeat failed at step " <> int.to_string(state.step),
            ),
          )
        }
      }
      use _ <- result.try(outcome)
      let next_term = node.current_term(after_op)
      let next_commit = node.commit_index(after_op)
      let next_last_index = log_last_position(node.log(after_op)).index
      use _ <- result.try(require_ok(
        next_term >= state.term,
        op_name
          <> " at step "
          <> int.to_string(state.step)
          <> " decreased the term: "
          <> int.to_string(state.term)
          <> " -> "
          <> int.to_string(next_term),
      ))
      use _ <- result.try(require_ok(
        next_commit >= state.commit,
        op_name
          <> " at step "
          <> int.to_string(state.step)
          <> " decreased the commit index: "
          <> int.to_string(state.commit)
          <> " -> "
          <> int.to_string(next_commit),
      ))
      use _ <- result.try(require_ok(
        next_last_index >= state.last_index,
        op_name
          <> " at step "
          <> int.to_string(state.step)
          <> " truncated the solo leader's log: "
          <> int.to_string(state.last_index)
          <> " -> "
          <> int.to_string(next_last_index),
      ))
      use _ <- result.try(require_ok(
        node.role(after_op) == Leader,
        op_name
          <> " at step "
          <> int.to_string(state.step)
          <> " stopped the solo voter being a leader, got role "
          <> role_name(node.role(after_op)),
      ))
      solo_loop(
        SoloState(
          ..state,
          r: r2,
          n: drain(after_op),
          step: state.step + 1,
          term: next_term,
          commit: next_commit,
          last_index: next_last_index,
        ),
      )
    }
  }
}

pub fn solo_node_valid_transitions_preserve_invariants_test() -> Nil {
  case run_model_cases(pbt_env.seed(), pbt_env.cases(64), solo_case) {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}
