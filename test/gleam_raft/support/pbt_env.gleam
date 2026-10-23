// 環境変数 GLEAM_RAFT_PBT_SEED / GLEAM_RAFT_PBT_CASES によるシードと
// ケース数の指定。読み取りは両ターゲット対応の FFI
// （pbt_env_ffi.mjs / os:getenv）で実装する。
// cluster_property_test と model_property_test の共通 harness として
// support に置いた（M5 手順5）。

import gleam/dynamic/decode
import gleam/int
import gleam/option.{type Option, None, Some}

// ---- 環境変数 ----
//
// Erlang の os:getenv/2 は名前にリスト（os_tab）を取り、命中時もリストを
// 返すため、erlang 側では binary_to_list / unicode:characters_to_binary で
// 往復させる（JS では FFI の恒等関数を通す）。未設定時はデフォルト項が
// そのまま返るので、あり得ない Sentinel 文字列を渡して等値検出する。JS
// FFI（getenv2）は未設定時に false を返し、文字列復号の失敗になる。

const env_absent = "gleam-raft-pbt-absent"

@external(erlang, "erlang", "binary_to_list")
@external(javascript, "./pbt_env_ffi.mjs", "dynamic_identity")
fn env_tab(value: String) -> decode.Dynamic

@external(erlang, "os", "getenv")
@external(javascript, "./pbt_env_ffi.mjs", "getenv2")
fn raw_getenv(name: decode.Dynamic, fallback: decode.Dynamic) -> decode.Dynamic

@external(erlang, "unicode", "characters_to_binary")
@external(javascript, "./pbt_env_ffi.mjs", "dynamic_identity")
fn env_text(value: decode.Dynamic) -> decode.Dynamic

fn env_str(name: String) -> Option(String) {
  case
    env_text(raw_getenv(env_tab(name), env_tab(env_absent)))
    |> decode.run(decode.string)
  {
    // 小文字名の bare pattern は新規バインドになってしまうため、
    // sentinel との等値は guard ではなく case で明示する。
    Ok(v) ->
      case v == env_absent {
        True -> None
        False -> Some(v)
      }
    Error(_) -> None
  }
}

fn env_int(name: String, default: Int) -> Int {
  case env_str(name) {
    None -> default
    Some(v) ->
      case int.parse(v) {
        Ok(n) -> n
        Error(_) -> default
      }
  }
}

/// ベースシード。既定は 900_001 で、環境変数 GLEAM_RAFT_PBT_SEED の数値で
/// 上書きできる。
pub fn seed() -> Int {
  env_int("GLEAM_RAFT_PBT_SEED", 900_001)
}

/// ケース数。既定値を使い、環境変数 GLEAM_RAFT_PBT_CASES が正整数のときだけ
/// 上書きする。
pub fn cases(default: Int) -> Int {
  case env_int("GLEAM_RAFT_PBT_CASES", default) {
    n if n > 0 -> n
    _ -> default
  }
}

/// いずれかの環境変数が立っているか（失敗再現用のデバッグ実行判定）。
pub fn debug_override() -> Bool {
  case env_str("GLEAM_RAFT_PBT_SEED"), env_str("GLEAM_RAFT_PBT_CASES") {
    Some(_), _ -> True
    _, Some(_) -> True
    None, None -> False
  }
}
