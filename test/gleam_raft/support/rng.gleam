// 自作の seed 駆動乱数生成器。noprop は hexpm に存在しないため（REQUIREMENTS §5）、
// 乱数生成は DESIGN §11.3 が定める xorshift64 の自作実装で代替する。生成器の選択
// 自体は DESIGN §11.3 が認める許容乖離（D6 系統）である。
//
// 出力は状態 hi 32bit のままでは使わない。raw な Marsaglia xorshift64 の
// 下位ビットは相関が強く（seed 900015 で選挙タイマーと同時ティック送出の
// 往復遅延がほぼ等値になり、投票返信が構造的に間に合わずクラスタ作成が
// 恒久的に飢餓するのを観察済み）、乗算ハッシュ（黄金定数 0x9E37 79B9 の
// 積の bits[24..55]）でスクランブルしてから返す。
//
// 64bit 状態は hi/lo の 32bit 半対で保持する。Gleam 1.17 の JavaScript
// バックエンドはビット演算を BigInt 経由で実装しており、全中間値が
// 2^53 以下なら Erlang とビット単位で一致する（M5 で実測確認）。一方、
// 2^53 を超える Int リテラルは JS 側だけでコンパイル時に丸められるため、
// 64bit 値を単一の Int で持つことはできない。xorshift64 のシフト 13/7/17
// は全演算とも中間値 2^49 以下に収まり、両ターゲットで完全一致する。
// 乗算ハッシュも 16bit limb 分解で全中間値 2^53 以下に保つ（mulhash 参照）。
import gleam/int
import gleam/list
import gleam/result

/// 2^32 − 1。半対の各要素はこの範囲の非負整数。
const mask32 = 4_294_967_295

/// xorshift64 の状態（hi: 上位 32bit、lo: 下位 32bit）。
pub type Rng {
  Rng(hi: Int, lo: Int)
}

/// シードから状態を作る。
///
/// シードは [0, 2^52) に畳み込んでから固定定数と混合し、8 回 step で
/// 暖機する。これにより seed=0 でも全ゼロの不動点に入らない。
/// 2^52 を超えるシードは高位が畳み込まれる（テスト再現用途で十分。
/// D6 の値域規律と同根の制限）。
pub fn from_seed(seed: Int) -> Rng {
  let folded =
    seed
    |> int.modulo(4_503_599_627_370_496)
    |> result.unwrap(0)
  let mixed = int.bitwise_exclusive_or(folded + 0x632B_E59B, 0x9E37_79B9)
  let lo = int.bitwise_and(mixed, mask32)
  let hi =
    int.bitwise_and(int.bitwise_shift_right(mixed, 20) + 0x85EB_CA6B, mask32)
  warmup(Rng(hi: hi, lo: lo), 8)
}

fn warmup(rng: Rng, count: Int) -> Rng {
  case count {
    0 -> rng
    _ -> rng |> step |> warmup(count - 1)
  }
}

/// 対形式の論理左シフト。k は 13/17 のみ渡す。
/// 中間値は高々 (2^32−1)<<17 < 2^49 で両ターゲット厳密。
fn shl(s: Rng, k: Int) -> Rng {
  let Rng(hi, lo) = s
  let new_lo = int.bitwise_and(int.bitwise_shift_left(lo, k), mask32)
  let new_hi =
    int.bitwise_and(
      int.bitwise_or(
        int.bitwise_shift_left(hi, k),
        int.bitwise_shift_right(lo, 32 - k),
      ),
      mask32,
    )
  Rng(hi: new_hi, lo: new_lo)
}

/// 対形式の右シフト。k は 7 のみ渡す。
/// (hi & 0x7F) << 25 ≤ 2^32 − 2^25 で厳密。
fn shr(s: Rng, k: Int) -> Rng {
  let Rng(hi, lo) = s
  let new_lo =
    int.bitwise_and(
      int.bitwise_or(
        int.bitwise_shift_right(lo, k),
        int.bitwise_shift_left(
          int.bitwise_and(hi, int.bitwise_shift_left(1, k) - 1),
          32 - k,
        ),
      ),
      mask32,
    )
  let new_hi = int.bitwise_shift_right(hi, k)
  Rng(hi: new_hi, lo: new_lo)
}

fn xor(s: Rng, t: Rng) -> Rng {
  Rng(
    hi: int.bitwise_exclusive_or(s.hi, t.hi),
    lo: int.bitwise_exclusive_or(s.lo, t.lo),
  )
}

/// Marsaglia の xorshift64（三つ組 13, 7, 17）を 1 回進める。
pub fn step(s: Rng) -> Rng {
  let s1 = xor(s, shl(s, 13))
  let s2 = xor(s1, shr(s1, 7))
  xor(s2, shl(s2, 17))
}

/// 32bit × 32bit の積から bits[24..55] を取り出す乗算ハッシュ
/// （Knuth の乗算ハッシュ系。黄金定数を掛け、中〜上位ビットを返すため、
/// 入力の全ビットが出力へ影響する）。16bit limb 分解で全中間値を
/// 2^53 以下に保ち、両ターゲットでビット単位一致する。
fn mulhash(r: Int, c: Int) -> Int {
  let a0 = int.bitwise_and(r, 0xFFFF)
  let a1 = int.bitwise_shift_right(r, 16)
  let b0 = int.bitwise_and(c, 0xFFFF)
  let b1 = int.bitwise_shift_right(c, 16)
  let p0 = a0 * b0
  let p1 = a0 * b1 + a1 * b0
  let p2 = a1 * b1
  // 積 >> 24 の下位 32bit を、2^24 未満の剰余の桁あふれまで厳密に扱う。
  // p2 項は 2^32 倍なので 2^24 未満の剰余を持たない（carries は 1 以下）。
  let shifted =
    p2 * 256 + int.bitwise_shift_right(p1, 8) + int.bitwise_shift_right(p0, 24)
  let remainder =
    int.bitwise_and(p0, 0xFF_FFFF)
    + int.bitwise_shift_left(int.bitwise_and(p1, 0xFF), 16)
  int.bitwise_and(shifted + int.bitwise_shift_right(remainder, 24), mask32)
}

/// 32bit 乱数を 1 個取り出す（xorshift64 状態の上位半対を乗算ハッシュで
/// スクランブルして返す）。
pub fn rand32(s: Rng) -> #(Rng, Int) {
  let s2 = step(s)
  #(s2, mulhash(s2.hi, 0x9E37_79B9))
}

/// 21bit + 32bit を結合した 53bit 乱数（[0, 2^53 − 1]）を取り出す。
pub fn rand53(s: Rng) -> #(Rng, Int) {
  let #(s2, a) = rand32(s)
  let #(s3, b) = rand32(s2)
  let hi21 = int.bitwise_and(a, 0x1F_FFFF)
  #(s3, hi21 * 4_294_967_296 + b)
}

/// [min, max]（両端含む）の一様整数。span が 2^32 以下なら 32bit 1 回、
/// さもなくば 53bit 描画の剰余（テスト用途の微小バイアスは許容。
/// noprop とは分布が異なるが、再現性は seed だけで決まる）。
pub fn rand_in(s: Rng, min: Int, max: Int) -> #(Rng, Int) {
  let span = max - min + 1
  case span <= 4_294_967_296 {
    True -> {
      let #(s2, r) = rand32(s)
      let v = int.modulo(r, span) |> result.unwrap(0)
      #(s2, min + v)
    }
    False -> {
      let #(s2, r) = rand53(s)
      let v = int.modulo(r, span) |> result.unwrap(0)
      #(s2, min + v)
    }
  }
}

/// 確率 n/d の Bernoulli 抽選。
pub fn rand_ratio(s: Rng, n: Int, d: Int) -> #(Rng, Bool) {
  let #(s2, r) = rand32(s)
  let hit = int.modulo(r, d) |> result.unwrap(0)
  #(s2, hit < n)
}

/// Bool の 1/2 抽選。
pub fn rand_bool(s: Rng) -> #(Rng, Bool) {
  rand_ratio(s, 1, 2)
}

/// ケース番号からシードを導く（noprop::Runner::run のケース展開相当）。
/// 各ケースは独立したストリームで走るため、ケース削除・追加が
/// 他ケースの再現性を壊さない。
pub fn seed_for_case(base_seed: Int, case_index: Int) -> Int {
  base_seed + case_index
}

/// stdlib 1.0.5 に list.range が無いのでここで用意する（M4 の慣習に統一）。
fn int_range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..int_range(from + 1, to)]
  }
}

/// 黄金ベクトル: シードから 32bit 値を n 個取り出す（両ターゲット照合用）。
pub fn golden32(seed: Int, n: Int) -> List(Int) {
  let #(_, values) =
    list.fold(int_range(1, n), #(from_seed(seed), []), fn(acc, _) {
      let #(s, vs) = acc
      let #(s2, v) = rand32(s)
      #(s2, [v, ..vs])
    })
  list.reverse(values)
}
