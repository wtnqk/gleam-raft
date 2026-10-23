#!/usr/bin/env bash
# M5 手順6: 性質テストランナ（DESIGN §11.3 のケース数スケール:
# CI 既定 1,000・ローカル 10,000）。
#
# Erlang ターゲットのみで走らせる（DESIGN §11.4 のターゲット格付け:
# 第一ターゲットは BEAM で、seed 検証・障害デバッグは Erlang 基準。
# JS は既定の test.yml の門（compat gate）に委ねる）。
#
# シードのシャーディング（重要）:
#   gleeunit は EUnit を scale_timeouts(10) で走らせるため、テスト関数
#   1 本あたりの上限は既定 5 秒 ×10 = 50 秒（OTP eunit_internal.hrl の
#   DEFAULT_TEST_TIMEOUT=5000）。safety テストは 1,000 ケースを単発では
#   50 秒に収まらない（基点 seed 900001 で実測: 単発 1,000 は 50 秒
#   超過で EUnit Timeout → モジュールごと Blame 連鎖。性質違反ではない）。
#   そこで総ケース数を 100 ケースのチャンクに分割し、
#   GLEAM_RAFT_PBT_SEED を base + k*100 で進めて順実行する。ケース seed は
#   seed_for_case = base + i なのでチャンク間で重複・こぼれは生じない。
#   1 チャンク（100 ケース）で full suite 約 20 秒（実測: 200 ケース
#   40.2 秒の線形換算）。ci=1,000 で約 4 分、local=10,000 で約 35〜45 分。
#
# 失敗時、テストは "case i seed=s" と絶対シードを出力する。その seed を
# debug モードに渡して単一ケースへ縮退再現する。
#
# 使い方:
#   ./scripts/pbt.sh ci                  # 総ケース数 1,000（100 × 10 シャード）
#   ./scripts/pbt.sh local               # 総ケース数 10,000（100 × 100 シャード）
#   ./scripts/pbt.sh debug SEED [CASES]  # SEED 固定・CASES 既定 1（再現用）
#   ./scripts/pbt.sh plain               # env 無しで既定ケース数
#
# ci/local の基点シードは 900_001。GLEAM_RAFT_PBT_SEED を外部で設定すると
# 基点を置き換えられる（別 seed 帯の回帰に使用）。
set -euo pipefail

cd "$(dirname "$0")/.."
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$PWD/.cache}"
mkdir -p "$XDG_CACHE_HOME"

CHUNK=100

usage() {
  sed -n '2,31p' "$0"
  exit 1
}

mode="${1:-}"
[ -n "$mode" ] || usage

case "$mode" in
  ci)
    total=1000
    ;;
  local)
    total=10000
    ;;
  debug)
    seed="${2:-}"
    [ -n "$seed" ] || usage
    cases="${3:-1}"
    echo "pbt: debug (SEED=$seed CASES=$cases)"
    GLEAM_RAFT_PBT_SEED="$seed" GLEAM_RAFT_PBT_CASES="$cases" \
      exec gleam test --target erlang
    ;;
  plain)
    echo "pbt: plain (既定ケース数)"
    exec gleam test --target erlang
    ;;
  *)
    usage
    ;;
esac

[ $((total % CHUNK)) -eq 0 ] || { echo "pbt: total は CHUNK の整数倍であること" >&2; exit 1; }
base="${GLEAM_RAFT_PBT_SEED:-900001}"
shards=$((total / CHUNK))
echo "pbt: $mode（総ケース数 $total = ${CHUNK} × $shards、基点 seed $base）"

for k in $(seq 0 $((shards - 1))); do
  s0=$((base + k * CHUNK))
  echo "pbt: shard $((k + 1))/$shards（seed $s0..$((s0 + CHUNK - 1))）"
  GLEAM_RAFT_PBT_SEED="$s0" GLEAM_RAFT_PBT_CASES="$CHUNK" gleam test --target erlang
done
echo "pbt: $mode 完了（$total ケース × 各テスト、全 shards green）"
