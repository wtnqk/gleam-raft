// 環境変数を読むための JavaScript 側 getenv FFI。Erlang 側の os:getenv/2 と
// 呼び出し形を揃えるため (name, fallback) を受け取るが、fallback は Erlang
// 側の Sentinel 用であり JS では使わない（未設定なら os:getenv と同じく
// false を返す）。
export function dynamic_identity(value) {
  return value;
}

export function getenv2(name, _fallback) {
  if (
    typeof process !== "undefined" &&
    process.env !== undefined &&
    process.env !== null &&
    process.env[name] !== undefined
  ) {
    return process.env[name];
  }
  return false;
}
