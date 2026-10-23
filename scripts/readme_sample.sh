#!/usr/bin/env bash
# M6 手順4: README サンプルの抽出実行ランナ。
#
# README の <!-- readme-sample:start/end --> マーカー間の gleam ブロックを
# そのまま examples/readme_sample/src/readme_sample.gleam に抽出し、その
# プロジェクト（リポジトリ本体を path 依存する別パッケージ）で実行する。
# 完了条件「README サンプルが example ではなく README 内のコードそのままで
# 動く」を構造で担保するため、CI（.github/workflows/test.yml）も同じ
# スクリプトを呼ぶ。
#
# 使い方: ./scripts/readme_sample.sh [erlang|javascript]   既定: erlang
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
target="${1:-erlang}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$root/.cache}"

proj="$root/examples/readme_sample"
out="$proj/src/readme_sample.gleam"
mkdir -p "$proj/src"

# フェンス行（```gleam と ```）だけを除いて中身を写す。マーカーが無い、
# もしくは空なら抽出失敗として落ちる。
awk '
  /^<!-- readme-sample:start -->$/ { inside = 1; next }
  /^<!-- readme-sample:end -->$/   { inside = 0 }
  inside && !/^```/
' "$root/README.md" > "$out"

if [ ! -s "$out" ]; then
  echo "README に readme-sample のマーカーブロックが見つからない" >&2
  exit 1
fi

output="$(cd "$proj" && gleam run --target "$target" -m readme_sample 2>&1)"
printf '%s\n' "$output"

# 出力の内容まで照合する（走ればいい、ではサンプルの約束にならない）。
grep -q 'commit 状態: Committed' <<<"$output" || {
  echo "README サンプルの期待出力（commit 状態: Committed）が見つからない" >&2
  exit 1
}
