// 自作テスト: rng.gleam（xorshift64 ＋乗算ハッシュ出力の自作実装）の照合。
// 自作部品（noprop 代替）なので照合元は無い。黄金ベクトルは
// Erlang ターゲットで生成し、独立実装（Python の論理シフト版 Marsaglia
// xorshift64, 三つ組 13/7/17 ＋ 黄金定数 0x9E37 79B9 の積 bits[24..55]
// スクランブル）と一致を確認した値をハードコードし、両ターゲットで同一断言を
// 実行する（M5 合意、出力スクランブル追加で 2026-10-22 に再生成）。
import gleam/int
import gleam/list
import gleam_raft/support/rng
import gleeunit/should

pub fn golden_vectors_test() {
  should.equal(
    [
      273_675_108,
      846_409_448,
      2_015_411_522,
      708_427_441,
      3_956_521_375,
      2_752_154_786,
    ],
    rng.golden32(0, 6),
  )
  should.equal(
    [
      4_233_327_211,
      2_379_433_159,
      3_456_747_802,
      1_680_574_138,
      1_585_836_518,
      3_722_381_427,
    ],
    rng.golden32(1, 6),
  )
  should.equal(
    [
      127_549_375,
      4_108_916_916,
      4_220_901_222,
      1_336_014_232,
      1_872_764_177,
      2_733_482_727,
    ],
    rng.golden32(42, 6),
  )
  should.equal(
    [
      2_409_292_584,
      3_455_026_289,
      1_882_333_543,
      2_094_120_237,
      558_951_818,
      800_947_244,
    ],
    rng.golden32(999, 6),
  )
  should.equal(
    [
      3_280_377_065,
      401_342_811,
      21_493_645,
      439_144_331,
      2_813_648_919,
      1_613_600_466,
    ],
    rng.golden32(123_456_789, 6),
  )
}

pub fn golden_rand53_test() {
  let #(_, v) = rng.rand53(rng.from_seed(42))
  should.equal(7_388_443_074_697_396, v)
  let #(_, v2) = rng.rand53(rng.from_seed(1_000_000))
  should.equal(8_233_063_941_671_561, v2)
}

pub fn rand53_range_test() {
  // rand53 の値域は [0, 2^53 − 1]。固定シードで 200 個。
  let values = draw_ints(rng.from_seed(3), 200, fn(s) { rng.rand53(s) })
  list.each(values, fn(v) {
    should.be_true(v >= 0 && v <= 9_007_199_254_740_991)
  })
}

pub fn determinism_test() {
  // 同一シードなら描画列が一致し、別シードなら（高確率で）最初から違う。
  let a = rng.golden32(42, 10)
  let b = rng.golden32(42, 10)
  should.equal(a, b)
  let c = rng.golden32(43, 10)
  should.be_true(a != c)
}

pub fn rand_in_range_test() {
  // span ≤ 2^32 路と > 2^32 路（rand53 経由）の両方で範囲内。
  let small = draw_ints(rng.from_seed(11), 300, fn(s) { rng.rand_in(s, 3, 17) })
  list.each(small, fn(v) { should.be_true(v >= 3 && v <= 17) })
  let big =
    draw_ints(rng.from_seed(12), 300, fn(s) {
      rng.rand_in(s, 0, 9_007_199_254_740_991)
    })
  list.each(big, fn(v) { should.be_true(v >= 0 && v <= 9_007_199_254_740_991) })
}

pub fn rand_in_degenerate_test() {
  // min == max なら必ず min。span = 2^32 ちょうど（32bit 路の境界）。
  let #(s1, v1) = rng.rand_in(rng.from_seed(5), 7, 7)
  should.equal(7, v1)
  let #(_, v2) = rng.rand_in(s1, 0, 4_294_967_295)
  should.be_true(v2 >= 0 && v2 <= 4_294_967_295)
  // span が 2^32 を 1 超えると rand53 路に切り替わる。
  let #(_, v3) = rng.rand_in(s1, 0, 4_294_967_296)
  should.be_true(v3 >= 0 && v3 <= 4_294_967_296)
}

pub fn rand_ratio_test() {
  // 確率 1/1 と 0/1 は決定論的。
  let #(_, hit) = rng.rand_ratio(rng.from_seed(9), 1, 1)
  should.be_true(hit)
  let #(_, miss) = rng.rand_ratio(rng.from_seed(9), 0, 1)
  should.be_false(miss)
  // 千分率 300 の出現頻度がだいたい 30%（固定シード・2000 回・区間検証）。
  let hits =
    draw_bools(rng.from_seed(21), 2000, fn(s) { rng.rand_ratio(s, 300, 1000) })
    |> list.fold(0, fn(acc, b) {
      case b {
        True -> acc + 1
        False -> acc
      }
    })
  should.be_true(hits > 300 && hits < 900)
}

pub fn seed_for_case_test() {
  should.equal(1000, rng.seed_for_case(1000, 0))
  should.equal(1007, rng.seed_for_case(1000, 7))
  // 隣接ケースのストリームは重ならない（先頭 4 個一致なし）。
  should.be_true(rng.golden32(1000, 4) != rng.golden32(1001, 4))
}

pub fn zero_seed_not_stuck_test() {
  // seed 0 でも全ゼロ不動点に入らず、描画が異なることを確認する。
  let values = rng.golden32(0, 8)
  should.equal(8, list.length(values))
  should.be_true(list.any(values, fn(v) { v != 0 }))
  let #(_s, first) = rng.rand32(rng.from_seed(0))
  let #(_s2, second) = rng.rand32(rng.step(rng.from_seed(0)))
  should.be_true(first != second || first != 0)
}

pub fn rand_in_decorrelation_test() {
  // 自作テスト: seed 900015 の選挙飢餓（観測済みライブロック）の回帰検査。
  // 出力スクランブル前は同一ティック連続ドローがほぼ等値で、U(1, 1000)
  // の遅延が 320 未満に落ちなかった。スクランブル後は平均 ≈ 500、
  // 極値到達、隣接差 ≤ 32 の頻度 ≈ 6.3%（理論値）に収まること。
  let values =
    draw_ints(rng.from_seed(900_015), 500, fn(s) { rng.rand_in(s, 1, 1000) })
  let mins = list.fold(values, 1001, int.min)
  let maxs = list.fold(values, 0, int.max)
  should.be_true(mins <= 50)
  should.be_true(maxs >= 950)
  let mean = list.fold(values, 0, int.add)
  should.be_true(mean > 200_000 && mean < 300_000)
  // 隣接差 ≤ 32 の出現回数（先頭要素は prev 不在なので数えない。-1 は
  // 番兵。値域が [1, 1000] なので -1 との実データ衝突はない）。
  let #(_, close) =
    list.fold(values, #(-1, 0), fn(acc, v) {
      let inc = case acc.0 {
        -1 -> 0
        _ ->
          case int.max(acc.0, v) - int.min(acc.0, v) <= 32 {
            True -> 1
            False -> 0
          }
      }
      #(v, acc.1 + inc)
    })
  should.be_true(close < 60)
}

// --- テスト用ヘルパ（自作） ---

fn draw_ints(
  seed_state: rng.Rng,
  count: Int,
  draw: fn(rng.Rng) -> #(rng.Rng, Int),
) -> List(Int) {
  let #(_s, reversed) =
    list.fold(repeats(count), #(seed_state, []), fn(acc, _) {
      let #(s, vs) = acc
      let #(s2, v) = draw(s)
      #(s2, [v, ..vs])
    })
  list.reverse(reversed)
}

fn draw_bools(
  seed_state: rng.Rng,
  count: Int,
  draw: fn(rng.Rng) -> #(rng.Rng, Bool),
) -> List(Bool) {
  let #(_s, reversed) =
    list.fold(repeats(count), #(seed_state, []), fn(acc, _) {
      let #(s, vs) = acc
      let #(s2, v) = draw(s)
      #(s2, [v, ..vs])
    })
  list.reverse(reversed)
}

/// stdlib 1.0.5 に list.range が無いので回数リストを自作する。
fn repeats(count: Int) -> List(Int) {
  case count {
    0 -> []
    _ -> [0, ..repeats(count - 1)]
  }
}
