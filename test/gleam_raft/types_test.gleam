// 期待値は TESTPLAN §1.1 の方針どおりに固定している。
import gleam/order
import gleam_raft/types
import gleeunit/should

pub fn log_position_new_and_next_test() -> Nil {
  let p = types.position(3, 7)
  should.equal(p.term, 3)
  should.equal(p.index, 7)
  should.equal(types.position_next(p), types.position(3, 8))
}

pub fn log_position_cmp_test() -> Nil {
  should.equal(types.compare_position(pos(5, 5), pos(5, 5)), order.Eq)
  should.equal(types.compare_position(pos(7, 3), pos(5, 5)), order.Gt)
  should.equal(types.compare_position(pos(3, 7), pos(5, 5)), order.Lt)
  should.equal(types.compare_position(pos(5, 7), pos(5, 5)), order.Gt)
  should.equal(types.compare_position(pos(5, 3), pos(5, 5)), order.Lt)
}

// 自前の追加: zero_position は全位置以下、position_next は元の位置より真に大きい。
// 境界値は JS ターゲットの Number 安全整数上限（2^53-1）に合わせる。
// Raft の index が 2^53 に到達する運用は考えない（DESIGN §2 D6）。
pub fn position_order_bounds_test() -> Nil {
  let big = types.position(2, 9_007_199_254_740_991)
  should.equal(types.compare_position(types.zero_position, big), order.Lt)
  should.equal(types.compare_position(types.position_next(big), big), order.Gt)
}

fn pos(term: Int, index: Int) -> types.LogPosition {
  types.position(term, index)
}
