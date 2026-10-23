// 自作テスト: gens.gleam（サンプルヘルパ）の自己検証。生成器の単体テスト自体は
// noprop 内部にあり対応物が無いので、ここで網羅する。
// 決定論（同一シード→同一列）に依存するため、アサーションはすべて
// 固定シード・固定回数で両ターゲット同一結果になる。
import gleam/list
import gleam/option.{None, Some}
import gleam/set
import gleam_raft/log.{CommandEntry, ConfigEntry, TermEntry}
import gleam_raft/support/gens
import gleam_raft/support/rng
import gleeunit/should

pub fn int_range_test() {
  let values = draw_many(gens.int(5, 9), 500, rng.from_seed(1))
  list.each(values, fn(v) { should.be_true(v >= 5 && v <= 9) })
  // min == max の退化区間。
  let one = draw_many(gens.int(3, 3), 10, rng.from_seed(2))
  should.equal([3, 3, 3, 3, 3, 3, 3, 3, 3, 3], one)
}

pub fn int_determinism_test() {
  let a = draw_many(gens.int(0, 1000), 50, rng.from_seed(77))
  let b = draw_many(gens.int(0, 1000), 50, rng.from_seed(77))
  should.equal(a, b)
}

pub fn boundaries_injection_test() {
  // 1/5 で境界値を優先する。5000 回描画なら 4 境界すべて高確率で出る
  // （固定シードなので決定的）。
  let values = draw_many(gens.sample_term_or_index(), 5000, rng.from_seed(3))
  let has = fn(target) { list.any(values, fn(v) { v == target }) }
  should.be_true(has(0))
  should.be_true(has(1))
  should.be_true(has(gens.sim_max - 1))
  should.be_true(has(gens.sim_max))
  // 全域範囲も守られる。
  list.each(values, fn(v) { should.be_true(v >= 0 && v <= gens.sim_max) })
}

pub fn sample_len_test() {
  let values = draw_many(gens.sample_len(10), 3000, rng.from_seed(4))
  list.each(values, fn(v) { should.be_true(v >= 0 && v <= 10) })
  should.be_true(list.any(values, fn(v) { v == 0 }))
  should.be_true(list.any(values, fn(v) { v == 10 }))
}

pub fn weighted_index_test() {
  // 重み 0 は選ばれない。
  let picks = draw_many(gens.weighted_index([0, 1, 0]), 200, rng.from_seed(5))
  list.each(picks, fn(p) { should.equal(1, p) })
  // 重み [3, 1] なら 0 が高頻度、1 も出る（固定シード・2000 回）。
  let mixed = draw_many(gens.weighted_index([3, 1]), 2000, rng.from_seed(6))
  let zeros =
    list.fold(mixed, 0, fn(acc, p) {
      case p {
        0 -> acc + 1
        _ -> acc
      }
    })
  should.be_true(zeros > 1200 && zeros < 1800)
}

pub fn choice_test() {
  let picked = draw_many(gens.choice(["a", "b", "c"]), 300, rng.from_seed(8))
  list.each(picked, fn(item) {
    should.be_true(item == "a" || item == "b" || item == "c")
  })
}

pub fn with_rejection_test() {
  // 1/3 受入の生成器でも 60 回試行ならほぼ必ず通る。
  let values =
    draw_many(
      gens.with_rejection(60, fn(r) {
        let #(r2, hit) = rng.rand_ratio(r, 1, 3)
        case hit {
          True -> #(r2, Ok(1))
          False -> #(r2, Error(Nil))
        }
      }),
      100,
      rng.from_seed(9),
    )
  should.equal(list.repeat(1, 100), values)
}

pub fn sample_config_shape_test() {
  // 全ノード id が 0..5 に収まり、決定論的に再現する。
  let configs = draw_many(gens.sample_config(), 300, rng.from_seed(10))
  list.each(configs, fn(c) {
    let all =
      set.to_list(c.voters)
      |> list.append(set.to_list(c.new_voters))
      |> list.append(set.to_list(c.non_voters))
    list.each(all, fn(id) { should.be_true(id >= 0 && id <= 5) })
  })
  // 同一シード 2 回で一致、別シードと（高確率で）不一致。
  let again = draw_many(gens.sample_config(), 1, rng.from_seed(10))
  should.equal(at(configs, 0), at(again, 0))
  let other = draw_many(gens.sample_config(), 1, rng.from_seed(11))
  should.be_true(at(configs, 0) != at(other, 0))
}

pub fn sample_normal_config_test() {
  // voters ∩ non_voters = ∅、new_voters は常に空。300 構成で検証。
  let configs = draw_many(gens.sample_normal_config(), 300, rng.from_seed(12))
  list.each(configs, fn(c) {
    should.equal(0, set.size(c.new_voters))
    should.equal(0, set.size(set.intersection(c.voters, c.non_voters)))
  })
}

pub fn sample_log_entry_variants_test() {
  // 重み [3, 1, 1] の 3 変種が全部出る。コマンド値域も確認。
  let entries = draw_many(gens.sample_log_entry(), 1000, rng.from_seed(13))
  let #(cmds, terms, cfgs) =
    list.fold(entries, #(0, 0, 0), fn(acc, e) {
      let #(c, t, g) = acc
      case e {
        CommandEntry(value) ->
          case value >= 0 && value <= 1_000_000 {
            True -> #(c + 1, t, g)
            False -> panic as "command value out of range"
          }
        TermEntry(_) -> #(c, t + 1, g)
        ConfigEntry(_) -> #(c, t, g + 1)
      }
    })
  should.be_true(cmds > 0 && terms > 0 && cfgs > 0)
  should.equal(1000, cmds + terms + cfgs)
}

// --- テスト用ヘルパ（自作） ---

fn draw_many(gen: gens.Gen(a), count: Int, seed_state: rng.Rng) -> List(a) {
  let #(_s, reversed) =
    list.fold(repeats(count), #(seed_state, []), fn(acc, _) {
      let #(s, vs) = acc
      let #(s2, v) = gens.run(gen, s)
      #(s2, [v, ..vs])
    })
  list.reverse(reversed)
}

fn repeats(count: Int) -> List(Int) {
  case count {
    0 -> []
    _ -> [0, ..repeats(count - 1)]
  }
}

/// stdlib 1.0.5 に list.at が無いので Option で返す自作版。
fn at(items: List(a), i: Int) -> option.Option(a) {
  case items, i {
    [first, ..], 0 -> Some(first)
    [_, ..rest], _ -> at(rest, i - 1)
    [], _ -> None
  }
}
