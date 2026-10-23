// サンプルヘルパと生成器プリミティブ。境界値優先や重み付き選択などは rng.gleam の
// xorshift64 上での自作実装で代替する（REQUIREMENTS §5: noprop は hexpm に
// 不在のため、プリミティブも自作を原則とする）。
//
// 値域の値 u64 は D5/D6 の Int 値域規律に合わせて [0, 2^53−1]（SIM_MAX）へ
// 縮小する。境界注入 [0, 1, MAX−1, MAX] の MAX 相当がこれに対応する。
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set

import gleam_raft/cluster_config.{type ClusterConfig, ClusterConfig, config_new}
import gleam_raft/log.{type LogEntry, CommandEntry, ConfigEntry, TermEntry}
import gleam_raft/support/rng as xs
import gleam_raft/types

/// 乱数ストリームを引数にとって値を作る生成器。
pub type Gen(a) =
  fn(xs.Rng) -> #(xs.Rng, a)

/// 生成器を実行する。
pub fn run(gen: Gen(a), rng: xs.Rng) -> #(xs.Rng, a) {
  gen(rng)
}

/// 検証に使う Int 値域の上限（2^53 − 1）。
pub const sim_max = 9_007_199_254_740_991

/// [min, max]（両端含む）の一様整数。
pub fn int(min: Int, max: Int) -> Gen(Int) {
  fn(r) { xs.rand_in(r, min, max) }
}

/// Bool の 1/2 抽選。
pub fn bool() -> Gen(Bool) {
  fn(r) { xs.rand_bool(r) }
}

/// 確率 n/d の Bernoulli 抽選。
pub fn ratio(n: Int, d: Int) -> Gen(Bool) {
  fn(r) { xs.rand_ratio(r, n, d) }
}

/// リストから 1 個の一様選択。
/// 空リストは呼び出し側ミスとして panic。
pub fn choice(items: List(a)) -> Gen(a) {
  fn(r) {
    let #(r2, i) = xs.rand_in(r, 0, list.length(items) - 1)
    let item = case nth_at(items, i) {
      Some(value) -> value
      None -> panic as "gens.choice index out of bounds"
    }
    #(r2, item)
  }
}

/// stdlib 1.0.5 に list.at が無いのでここで用意する（quorum_test の
/// nth_at と同系）。
fn nth_at(items: List(a), i: Int) -> Option(a) {
  case items, i {
    [first, ..], 0 -> Some(first)
    [_, ..rest], _ -> nth_at(rest, i - 1)
    [], _ -> None
  }
}

/// 重み付き index 選択。
/// 重み 0 の項目は選ばれる確率 0。
pub fn weighted_index(weights: List(Int)) -> Gen(Int) {
  fn(r) {
    let total = list.fold(weights, 0, fn(acc, w) { acc + w })
    let #(r2, ticket) = xs.rand_in(r, 0, total - 1)
    let picked =
      list.fold(weights, #(0, ticket, -1), fn(acc, w) {
        let #(i, t, picked) = acc
        case picked >= 0 {
          True -> #(i + 1, t, picked)
          False ->
            case t < w {
              True -> #(i + 1, t, i)
              False -> #(i + 1, t - w, picked)
            }
        }
      })
      |> third_of
    #(r2, picked)
  }
}

fn third_of(t: #(Int, Int, Int)) -> Int {
  let #(_, _, c) = t
  c
}

/// 境界値を 1/one_in の確率で優先し、外れたら fallback する。
pub fn with_boundaries(
  boundaries: List(a),
  one_in: Int,
  fallback: Gen(a),
) -> Gen(a) {
  fn(r) {
    let #(r2, hit) = xs.rand_ratio(r, 1, one_in)
    case hit {
      True -> choice(boundaries)(r2)
      False -> fallback(r2)
    }
  }
}

/// 試行を最大 attempts 回繰返し、成功値を得る。全滅は生成器設定ミスとして panic。
pub fn with_rejection(attempts: Int, gen: Gen(Result(a, Nil))) -> Gen(a) {
  fn(r) {
    case attempts {
      0 -> panic as "gens.with_rejection exhausted"
      _ -> {
        let #(r2, outcome) = gen(r)
        case outcome {
          Ok(value) -> #(r2, value)
          Error(Nil) -> with_rejection(attempts - 1, gen)(r2)
        }
      }
    }
  }
}

/// [0, max] の長さ。0, 1, max を 1/5 で優先する。
pub fn sample_len(max: Int) -> Gen(Int) {
  fn(r) {
    case max < 3 {
      True -> panic as "gens.sample_len requires max >= 3"
      False -> with_boundaries([0, 1, max], 5, int(2, max - 1))(r)
    }
  }
}

/// 項番号・ログ index の生成。[0, SIM_MAX] で境界注入
/// （D5/D6 の値域縮小版）。
pub fn sample_term_or_index() -> Gen(Int) {
  with_boundaries([0, 1, sim_max - 1, sim_max], 5, int(0, sim_max))
}

/// 3 集合が重なりうる（不正構成も含む）全空間のクラスタ構成
/// （id 0..5、mask 0..7）。
pub fn sample_config() -> Gen(ClusterConfig) {
  fn(r) { sample_config_go(r, 0, config_new()) }
}

fn sample_config_go(
  r: xs.Rng,
  id: types.NodeId,
  config: ClusterConfig,
) -> #(xs.Rng, ClusterConfig) {
  let #(r2, membership) = xs.rand_in(r, 0, 7)
  let config =
    ClusterConfig(
      voters: {
        case int_bit(membership, 1) {
          True -> set.insert(config.voters, id)
          False -> config.voters
        }
      },
      new_voters: {
        case int_bit(membership, 2) {
          True -> set.insert(config.new_voters, id)
          False -> config.new_voters
        }
      },
      non_voters: {
        case int_bit(membership, 4) {
          True -> set.insert(config.non_voters, id)
          False -> config.non_voters
        }
      },
    )
  case id < 5 {
    True -> sample_config_go(r2, id + 1, config)
    False -> #(r2, config)
  }
}

fn int_bit(value: Int, bit: Int) -> Bool {
  int.bitwise_and(value, bit) == bit
}

/// 非 joint で voters と non_voters が相互素の正常構成
/// （id 0..5）。
pub fn sample_normal_config() -> Gen(ClusterConfig) {
  fn(r) { sample_normal_go(r, 0, config_new()) }
}

fn sample_normal_go(
  r: xs.Rng,
  id: types.NodeId,
  config: ClusterConfig,
) -> #(xs.Rng, ClusterConfig) {
  let #(r2, kind) = xs.rand_in(r, 0, 2)
  let config = case kind {
    0 -> ClusterConfig(..config, voters: set.insert(config.voters, id))
    1 -> ClusterConfig(..config, non_voters: set.insert(config.non_voters, id))
    _ -> config
  }
  case id < 5 {
    True -> sample_normal_go(r2, id + 1, config)
    False -> #(r2, config)
  }
}

/// ログエントリの生成。
/// 重み [3, 1, 1] で Command / Term / Config を選ぶ。コマンド値は
/// テストの可読性のため [0, 1_000_000] の一様整数。command は値を持たないため
/// の自作の値域設定。
pub fn sample_log_entry() -> Gen(LogEntry(Int)) {
  fn(r) {
    let #(r2, kind) = weighted_index([3, 1, 1])(r)
    case kind {
      0 -> {
        let #(r3, cmd) = xs.rand_in(r2, 0, 1_000_000)
        #(r3, CommandEntry(cmd))
      }
      1 -> {
        let #(r3, term) = sample_term_or_index()(r2)
        #(r3, TermEntry(term))
      }
      _ -> {
        let #(r3, cfg) = sample_config()(r2)
        #(r3, ConfigEntry(cfg))
      }
    }
  }
}
