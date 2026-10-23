import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import gleam_raft/cluster_config.{type ClusterConfig, ClusterConfig}
import gleam_raft/quorum.{smallest_majority_index}
import gleeunit/should

fn cfg(
  voters: List(Int),
  new_voters: List(Int),
  non_voters: List(Int),
) -> ClusterConfig {
  ClusterConfig(
    voters: set.from_list(voters),
    new_voters: set.from_list(new_voters),
    non_voters: set.from_list(non_voters),
  )
}

fn followers(entries: List(#(Int, Int))) -> Dict(Int, Int) {
  dict.from_list(entries)
}

// 空 voters の Quorum 構築を拒否しない。
// panic 相当を置かない方針（D1）なので、None を返すことで受け取る。
pub fn empty_voters_config_is_rejected_test() -> Nil {
  let empty = cfg([], [], [1, 2])
  should.equal(
    smallest_majority_index(empty, 1, 5, followers([#(2, 99)])),
    None,
  )

  // joint 中に旧側が空なら、新側が有効でも None（片方でも None なら None）。
  let joint_empty_old = cfg([], [1, 2], [])
  should.equal(
    smallest_majority_index(joint_empty_old, 1, 5, followers([#(2, 7)])),
    None,
  )
}

// top-k 集合のサイズが n/2+1 であることは直接見えない。純関数では
// k 番目のマッチが結果に現れることで同じ割当を検証する。
pub fn majority_size_matches_voter_count_test() -> Nil {
  let ascending = followers([#(2, 20), #(3, 30), #(4, 40), #(5, 50)])
  should.equal(
    smallest_majority_index(cfg([1, 2, 3, 4, 5], [], []), 1, 10, ascending),
    Some(30),
  )
  // n=5, k=3
  should.equal(
    smallest_majority_index(cfg([1, 2, 3], [], []), 1, 10, ascending),
    Some(20),
  )
  // n=3, k=2
  should.equal(
    smallest_majority_index(cfg([1], [], []), 1, 10, dict.new()),
    Some(10),
  )
  // n=1, k=1
  should.equal(
    smallest_majority_index(cfg([1, 2], [], []), 1, 10, ascending),
    Some(10),
  )
  // n=2, k=2
  should.equal(
    smallest_majority_index(cfg([1, 2, 3, 4], [], []), 1, 10, ascending),
    Some(20),
  )
  // n=4, k=3

  // joint 中は新旧それぞれに独立的な割当がいる。旧側が小さい方を決める。
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3], [1, 2, 3, 4, 5], []),
      1,
      10,
      ascending,
    ),
    Some(20),
  )
  // min(旧 k=2 → 20, 新 k=3 → 30)
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3, 4], [1, 2, 3, 4, 5, 6], []),
      1,
      10,
      followers([#(2, 20), #(3, 30), #(4, 40), #(5, 50), #(6, 60)]),
    ),
    Some(20),
  )
  // min(旧 k=3 → 20, 新 k=4 → 30)
}

pub fn even_voter_majority_requires_half_plus_one_test() -> Nil {
  // 2 投票者: 両方が進まないと多数派が動かない。
  should.equal(
    smallest_majority_index(cfg([1, 2], [], []), 1, 0, followers([#(2, 20)])),
    Some(0),
  )
  should.equal(
    smallest_majority_index(cfg([1, 2], [], []), 1, 10, followers([#(2, 20)])),
    Some(10),
  )

  // 4 投票者: 3 台進んで初めて多数派が動く。2 台では足りない。
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3, 4], [], []),
      1,
      10,
      followers([#(2, 20)]),
    ),
    Some(0),
  )
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3, 4], [], []),
      1,
      10,
      followers([#(2, 20), #(3, 30)]),
    ),
    Some(10),
  )
}

pub fn joint_smallest_majority_picks_min_of_old_and_new_test() -> Nil {
  let joint = cfg([1, 2, 3], [1, 2, 4, 5, 6], [])
  should.equal(smallest_majority_index(joint, 1, 0, dict.new()), Some(0))

  // 旧構成 (1, 2, 3) が 10 に進む。新構成専用ノード (4, 5, 6) が 0 のうちは
  // 新側の多数派が全体を下から押さえる。
  should.equal(
    smallest_majority_index(joint, 1, 10, followers([#(2, 10), #(3, 10)])),
    Some(0),
  )

  // 4, 5, 6 が 20 に進むと新側の k 番目（3 番目）も 20 になり、min が 10 へ。
  should.equal(
    smallest_majority_index(
      joint,
      1,
      10,
      followers([#(2, 10), #(3, 10), #(4, 20), #(5, 20), #(6, 20)]),
    ),
    Some(10),
  )
}

// D3: 増分状態を持たないので「同一 index の再適用」は入力として現れない。
// 意味論である「多数派境界値と同じ index の config 外ノードのエントリは
// 結果を変えない」として記述する。
pub fn in_set_voter_same_index_update_is_a_no_op_test() -> Nil {
  let voters5 = cfg([1, 2, 3, 4, 5], [9, 10], [])
  let base = followers([#(2, 0), #(3, 0), #(4, 10), #(5, 0)])
  should.equal(smallest_majority_index(voters5, 1, 0, base), Some(0))
  should.equal(
    smallest_majority_index(
      voters5,
      1,
      0,
      dict.merge(base, followers([#(9, 10)])),
    ),
    Some(0),
  )
}

// D3: top-k 集合の要素一意性（1 ノードのマッチが 1 度だけ計上される）への
// 書き換え。呼び出し側が規約に反して followers に自ノードの鍵を混ぜても、
// 自己のマッチは `self_match` として一度だけ数えられる。
pub fn advancing_in_set_non_min_voter_removes_its_old_entry_test() -> Nil {
  // voters [1, 2]（k=2）、自己のマッチは 9。フォロワ側の自己エントリ 5 と
  // 併せて二重計上なら、マッチ集合は [9, 5, 0] となり 2 番目が 5 になる。
  should.equal(
    smallest_majority_index(
      cfg([1, 2], [], []),
      1,
      9,
      followers([#(1, 5), #(2, 0)]),
    ),
    Some(0),
  )
  // 自己エントリが `self_match` より大きくても同じく無視される。
  should.equal(
    smallest_majority_index(
      cfg([1, 2], [], []),
      1,
      9,
      followers([#(1, 50), #(2, 0)]),
    ),
    Some(0),
  )
}

// D3: top-k 集合からの追い出しは増分実装内部の状態遷移。純関数の結果に
// おける対応は「集合外ノードが集合外のまま進んでも多数派インデックスは
// 変わらない」である。
pub fn advancing_a_voter_outside_top_k_evicts_the_min_test() -> Nil {
  let voters5 = cfg([1, 2, 3, 4, 5], [], [])
  // 5 投票者（k=3）、マッチは [10, 9, 8, 0, 0]。3 番目は 8。
  should.equal(
    smallest_majority_index(
      voters5,
      1,
      10,
      followers([#(2, 9), #(3, 8), #(4, 0)]),
    ),
    Some(8),
  )
  // 4 台目を 0 → 1 と進めてもなお集合外。多数派インデックスは不変。
  should.equal(
    smallest_majority_index(
      voters5,
      1,
      10,
      followers([#(2, 9), #(3, 8), #(4, 1)]),
    ),
    Some(8),
  )
}

// D3: 増分 top-k 集合なら同一 index に ID 順の追い出し順序を持ち得るが、
// 純関数の結果はそこに依存しない。「同一 index のノードは別枠として
// 個別に数えられ、結果は ID の並び順に依存しない」として検証する。
pub fn same_index_tie_break_evicts_smaller_node_id_first_test() -> Nil {
  // 2 台が同じ 5 を持つ。k=2 の 2 番目は 5（同一値を 2 エントリとして数える）。
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3], [], []),
      1,
      5,
      followers([#(2, 5), #(3, 4)]),
    ),
    Some(5),
  )
  // 同一値のエントリは ID の割り当て順に依存しない。
  should.equal(
    smallest_majority_index(
      cfg([1, 2, 3], [], []),
      1,
      5,
      followers([#(2, 4), #(3, 5)]),
    ),
    Some(5),
  )
}

// 同じ入力・同じ期待値（前半 20、後半 10）をアサートする。
pub fn constructing_a_wider_quorum_starts_from_zero_test() -> Nil {
  let matches = followers([#(2, 20), #(3, 30)])
  should.equal(
    smallest_majority_index(cfg([1, 2, 3], [], []), 1, 10, matches),
    Some(20),
  )
  // 4, 5 が加わると followers に無い 2 台は 0 から始める。k=2 が k=3 になり、
  // 多数派インデックスは 10 へ戻る。
  should.equal(
    smallest_majority_index(cfg([1, 2, 3, 4, 5], [], []), 1, 10, matches),
    Some(10),
  )
}

pub fn non_voter_updates_do_not_affect_majority_test() -> Nil {
  let with_learners = cfg([1, 2, 3], [], [9, 10])
  should.equal(
    smallest_majority_index(with_learners, 1, 0, dict.new()),
    Some(0),
  )
  // 非投票者のマッチがどんなに大きくても多数派には入らない。
  should.equal(
    smallest_majority_index(
      with_learners,
      1,
      0,
      followers([#(9, 100), #(10, 200)]),
    ),
    Some(0),
  )
}

pub fn joint_old_and_new_voters_evolve_independently_test() -> Nil {
  let joint = cfg([1, 2, 3], [3, 4, 5], [9])
  // 新側が先に 50 へ進んでも、旧側の多数派が 0 を押さえている。
  should.equal(
    smallest_majority_index(
      joint,
      1,
      100,
      followers([#(2, 0), #(3, 0), #(4, 50), #(5, 60)]),
    ),
    Some(0),
  )
  // 旧側が 30 に進むと全体は 30 へ。新側（k 番目が 50）はそのまま独立に
  // 残っており、min が結果を決める。
  should.equal(
    smallest_majority_index(
      joint,
      1,
      100,
      followers([#(2, 0), #(3, 30), #(4, 50), #(5, 60)]),
    ),
    Some(30),
  )
}

// 総当り一致は M1-5 の単体テストで行う。決定論的な
// 疑似乱数（LCG）で同じ総当りを行う。オラクルは「サイズ k の全
// 組合せの最小値の最大値」を組合せ列挙で計算し、実装の降順ソートと
// 独立な計算経路を持つ。
pub fn quorum_matches_naive_model_test() -> Nil {
  let counters = run_cases(1_234_567, 128, counters_new())
  // カバレッジ検査: どの形態も 1 件は通っていること。
  should.be_true(counters.joint > 0)
  should.be_true(counters.non_joint > 0)
  should.be_true(counters.advanced > 0)
  should.be_true(counters.non_voter > 0)
}

type CaseCounters {
  CaseCounters(joint: Int, non_joint: Int, advanced: Int, non_voter: Int)
}

fn counters_new() -> CaseCounters {
  CaseCounters(joint: 0, non_joint: 0, advanced: 0, non_voter: 0)
}

fn run_cases(
  state: Int,
  remaining: Int,
  counters: CaseCounters,
) -> CaseCounters {
  case remaining {
    0 -> counters
    _ -> {
      let #(next_state, next_counters) = check_one_case(state, counters)
      run_cases(next_state, remaining - 1, next_counters)
    }
  }
}

fn check_one_case(state: Int, counters0: CaseCounters) -> #(Int, CaseCounters) {
  // 投票者数。偶数構成を厚めに置いた候補から選ぶ
  let voter_choices = [1, 2, 2, 3, 4, 4, 5, 6, 6, 8]
  let #(vi, s1) = state_below(state, list.length(voter_choices))
  let voter_count = case nth_at(voter_choices, vi) {
    Some(n) -> n
    None -> 1
  }
  let #(voters, s2) = sample_distinct(s1, voter_count, int_range(1, 16))

  // 約半数で joint consensus 構成にする。新側は旧側と-ID が重なり得る。
  let #(jr, s3) = state_below(s2, 2)
  let #(new_voters, s4) = case jr {
    0 -> {
      let #(ni, s3a) = state_below(s3, list.length(voter_choices))
      let new_count = case nth_at(voter_choices, ni) {
        Some(n) -> n
        None -> 1
      }
      sample_distinct(s3a, new_count, int_range(1, 16))
    }
    _ -> #([], s3)
  }

  // 約半数で 1〜2 台の非投票者（ID 17..=20。投票者と非交）を添える。
  let #(nr, s5) = state_below(s4, 2)
  let #(non_voters, s6) = case nr {
    0 -> {
      let #(nc, s5a) = state_below(s5, 2)
      sample_distinct(s5a, nc + 1, int_range(17, 20))
    }
    _ -> #([], s5)
  }

  // 自己ノードは voters から 1 台選ぶ。自マッチは別経路で渡す。
  let #(si, s7) = state_below(s6, list.length(voters))
  let self_id = case nth_at(voters, si) {
    Some(id) -> id
    None -> 1
  }
  let #(self_match, s8) = state_below(s7, 31)

  // 全投票者のマッチを 0..=30 から取り出す。各鍵は 1/6 の確率で
  // followers に入れず、0 シード経路も通す。
  let #(fs, s9) =
    sample_matches(s8, list.append(voters, new_voters), dict.new())
  let config = cfg(voters, new_voters, non_voters)

  // 期待値: マッチ集合のサイズ k 全組合せの最小値の最大値。
  let old_expected =
    voters
    |> list.map(voter_match(_, self_id, self_match, fs))
    |> brute_force_smallest(voter_count / 2 + 1)
  let expected = case new_voters {
    [] -> old_expected
    _ ->
      case
        new_voters
        |> list.map(voter_match(_, self_id, self_match, fs))
        |> brute_force_smallest(list.length(new_voters) / 2 + 1)
      {
        Some(n) -> min_option(old_expected, Some(n))
        None -> None
      }
  }
  should.equal(
    smallest_majority_index(config, self_id, self_match, fs),
    expected,
  )

  // カバレッジ用の計数。joint/non-joint、非投票者付き、前進ありの各形態を
  // 少なくとも 1 件通したことを最後に検査する。
  let counters1 = case new_voters {
    [] -> CaseCounters(..counters0, non_joint: counters0.non_joint + 1)
    _ -> CaseCounters(..counters0, joint: counters0.joint + 1)
  }
  let counters2 = case non_voters {
    [] -> counters1
    _ -> CaseCounters(..counters1, non_voter: counters1.non_voter + 1)
  }
  let counters3 = case expected {
    Some(n) ->
      case n > 0 {
        True -> CaseCounters(..counters2, advanced: counters2.advanced + 1)
        False -> counters2
      }
    None -> counters2
  }
  #(s9, counters3)
}

fn voter_match(
  id: Int,
  self_id: Int,
  self_match: Int,
  followers: Dict(Int, Int),
) -> Int {
  case id == self_id {
    True -> self_match
    False ->
      case dict.get(followers, id) {
        Ok(matched) -> matched
        Error(_) -> 0
      }
  }
}

// --- 以下、テスト用の補助（決定論的疑似乱数と総当りオラクル） ---

// stdlib 1.0.5 に list.range と list.nth が無いのでここで用意する。
fn int_range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..int_range(from + 1, to)]
  }
}

fn nth_at(items: List(a), i: Int) -> Option(a) {
  case items, i {
    [first, ..], 0 -> Some(first)
    [_, ..rest], _ -> nth_at(rest, i - 1)
    [], _ -> None
  }
}

fn state_below(state: Int, n: Int) -> #(Int, Int) {
  // 線形合同法。乗算後の中間値は 2^53-1 に収まる範囲に抑える。
  let next = int.bitwise_and(state * 1_664_525 + 1_013_904_223, 4_294_967_295)
  // n は常に 1 以上なので Error にはならない。
  let value = int.modulo(next, n) |> result.unwrap(0)
  #(value, next)
}

fn sample_distinct(
  state: Int,
  count: Int,
  pool: List(Int),
) -> #(List(Int), Int) {
  case count, pool {
    0, _ -> #([], state)
    _, [] -> #([], state)
    _, _ -> {
      let #(i, s1) = state_below(state, list.length(pool))
      let #(picked, rest) = take_at(pool, i)
      let #(tail, s2) = sample_distinct(s1, count - 1, rest)
      #([picked, ..tail], s2)
    }
  }
}

fn take_at(items: List(a), i: Int) -> #(a, List(a)) {
  case items {
    [first, ..rest] ->
      case i {
        0 -> #(first, rest)
        _ ->
          case take_at(rest, i - 1) {
            #(picked, tail) -> #(picked, [first, ..tail])
          }
      }
    [] -> panic as "take_at: index out of bounds"
  }
}

fn sample_matches(
  state: Int,
  ids: List(Int),
  acc: Dict(Int, Int),
) -> #(Dict(Int, Int), Int) {
  case ids {
    [] -> #(acc, state)
    [id, ..rest] -> {
      let #(skip, s1) = state_below(state, 6)
      let #(value, s2) = state_below(s1, 31)
      let acc1 = case skip {
        0 -> acc
        _ -> dict.insert(acc, id, value)
      }
      sample_matches(s2, rest, acc1)
    }
  }
}

fn combinations(items: List(a), k: Int) -> List(List(a)) {
  case k {
    0 -> [[]]
    _ ->
      case items {
        [] -> []
        [first, ..rest] ->
          list.map(combinations(rest, k - 1), fn(c) { [first, ..c] })
          |> list.append(combinations(rest, k))
      }
  }
}

fn brute_force_smallest(indices: List(Int), k: Int) -> Option(Int) {
  // k 番目に大きいマッチの同値関係: 「サイズ k の全組合せの最小値の最大値」
  // （多数派 k 台を選ぶとき、最小マッチを最大化できる値）。k 番目に大きい
  // 値がその最大値であり、多数派到達の定義（k 台以上が以上を持つ最大値）と
  // も一致する。
  case combinations(indices, k) {
    [] -> None
    combos -> Some(list.map(combos, minimum) |> maximum)
  }
}

fn maximum(indices: List(Int)) -> Int {
  case indices {
    [] -> 0
    [first, ..rest] -> list.fold(rest, first, int.max)
  }
}

fn minimum(indices: List(Int)) -> Int {
  case indices {
    [] -> 0
    [first, ..rest] -> list.fold(rest, first, int.min)
  }
}

fn min_option(a: Option(Int), b: Option(Int)) -> Option(Int) {
  case a, b {
    Some(x), Some(y) -> Some(int.min(x, y))
    _, _ -> None
  }
}
