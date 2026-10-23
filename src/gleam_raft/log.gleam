//// 保持ログとその操作。公開 API は docs/DESIGN.md §5 に置く。
////
//// `entries` に内容を明示的に保持し、`term_markers` と
//// `config_markers` は検索用の降順索引として併存させる。term_markers の
//// 末尾には常に anchor の `(prev_position.index, prev_position.term)` を
//// 番兵として置く。おかげで範囲内の項の検索は 1 本で済み、マーカーに
//// 当たらない場合は anchor の term になる。
//// config_markers に番兵は置かず、不在時のスナップショット構成への
//// フォールバックは `Log` 級の関数が担う。
////
//// 操作関数はすべて純粋で、マーカーは毎回 `entries` と整合的に更新される。

import gleam/list
import gleam/option.{type Option, None, Some}
import gleam_raft/cluster_config.{type ClusterConfig}
import gleam_raft/types.{type LogIndex, type LogPosition, type Term}

/// ログ 1 件。`CommandEntry` のペイロードは D2 に従い型パラメータで
/// ジェネリックに受け、ペイロードが要らない用途では Unit で済ませる。
pub type LogEntry(command) {
  /// 新しい項の開始位置を示すマーカー。
  TermEntry(term: Term)

  /// この位置で有効になったクラスタ構成。
  ConfigEntry(config: ClusterConfig)

  /// 複製対象のコマンド。
  CommandEntry(command: command)
}

/// anchor（prev_position）以降の連続したログ断片。
///
/// `entries` は 0 始まりで、i 番目の項目の index は
/// `prev_position.index + 1 + i`。`term_markers` と `config_markers` は
/// index 降順のリストで、前者の末尾は anchor 番兵、後者に番兵はない。
pub type LogEntries(command) {
  LogEntries(
    prev_position: LogPosition,
    last_position: LogPosition,
    entries: List(LogEntry(command)),
    term_markers: List(#(LogIndex, Term)),
    config_markers: List(#(LogIndex, ClusterConfig)),
  )
}

/// 保持部分にスナップショット時点の構成を添えたログ。
pub type Log(command) {
  Log(snapshot_config: ClusterConfig, entries: LogEntries(command))
}

/// `position` に空の断片を作る。anchor と末尾が一致し、長さは 0。
pub fn entries_new(prev_position: LogPosition) -> LogEntries(command) {
  LogEntries(
    prev_position: prev_position,
    last_position: prev_position,
    entries: [],
    term_markers: [#(prev_position.index, prev_position.term)],
    config_markers: [],
  )
}

/// `entries_new` から順に `entries_push` した結果と一致する断片を作る。
pub fn entries_from_list(
  prev_position: LogPosition,
  entries: List(LogEntry(command)),
) -> LogEntries(command) {
  list.fold(entries, entries_new(prev_position), entries_push)
}

/// 保持している項目数。
pub fn entries_len(e: LogEntries(command)) -> Int {
  e.last_position.index - e.prev_position.index
}

/// 断片が空か。anchor と末尾が一致する状態。
pub fn entries_is_empty(e: LogEntries(command)) -> Bool {
  entries_len(e) == 0
}

/// 項目を末尾に 1 件追加する。TermEntry は末尾位置の term とマーカーを、
/// ConfigEntry は構成マーカーを更新する。
pub fn entries_push(
  e: LogEntries(command),
  entry: LogEntry(command),
) -> LogEntries(command) {
  let next = types.position_next(e.last_position)
  let entries = list.append(e.entries, [entry])
  case entry {
    TermEntry(term) ->
      LogEntries(
        ..e,
        last_position: types.position(term, next.index),
        entries: entries,
        term_markers: [#(next.index, term), ..e.term_markers],
      )
    ConfigEntry(config) ->
      LogEntries(..e, last_position: next, entries: entries, config_markers: [
        #(next.index, config),
        ..e.config_markers
      ])
    CommandEntry(_) -> LogEntries(..e, last_position: next, entries: entries)
  }
}

/// `len` 件より長ければ `len` 件目に切り詰める。保持件数が `len` 以下の
/// 場合は無操作で、現在の末尾位置を返す。マーカーは残った範囲だけを残す。
pub fn entries_truncate(
  e: LogEntries(command),
  len: Int,
) -> #(LogEntries(command), LogPosition) {
  case len >= entries_len(e) {
    True -> #(e, e.last_position)
    False -> {
      let last_index = e.prev_position.index + len
      let last_term = case entries_get_term(e, last_index) {
        Some(term) -> term
        None -> e.prev_position.term
      }
      let last = types.position(last_term, last_index)
      #(
        LogEntries(
          ..e,
          last_position: last,
          entries: list.take(e.entries, len),
          term_markers: list.filter(e.term_markers, fn(m) { m.0 <= last_index }),
          config_markers: list.filter(e.config_markers, fn(m) {
            m.0 <= last_index
          }),
        ),
        last,
      )
    }
  }
}

/// index が保持範囲 `prev_position.index <= index <= last_position.index`
/// にあるか。anchor 自身も含む。
pub fn entries_contains_index(e: LogEntries(command), index: LogIndex) -> Bool {
  e.prev_position.index <= index && index <= e.last_position.index
}

/// その index が属する項を返す。範囲外なら None。番兵があるため、範囲内
/// で TermEntry マーカーに当たらない場合は anchor の term になる。
pub fn entries_get_term(
  e: LogEntries(command),
  index: LogIndex,
) -> Option(Term) {
  case entries_contains_index(e, index) {
    False -> None
    True -> find_term_marker(e.term_markers, index)
  }
}

/// 位置が断片の内容と一致するか。その index の項が position.term と
/// 等しいことと同値。
pub fn entries_contains(e: LogEntries(command), position: LogPosition) -> Bool {
  case entries_get_term(e, position.index) {
    Some(term) -> term == position.term
    None -> False
  }
}

/// index の項目を返す。anchor 自身（index == prev_position.index）は
/// 断片の外なので None。範囲外も None。
pub fn entries_get_entry(
  e: LogEntries(command),
  index: LogIndex,
) -> Option(LogEntry(command)) {
  case entries_contains_index(e, index) && index > e.prev_position.index {
    False -> None
    True -> nth(e.entries, index - e.prev_position.index - 1)
  }
}

/// 位置つきの項目リスト。先頭項目の index は `prev_position.index + 1`、
/// 先頭位置の term は TermEntry が現れるまで anchor の term から更新されない。
pub fn entries_iter_with_positions(
  e: LogEntries(command),
) -> List(#(LogPosition, LogEntry(command))) {
  scan_with_positions(
    e.entries,
    e.prev_position.index + 1,
    e.prev_position.term,
    [],
  )
}

fn scan_with_positions(
  entries: List(LogEntry(command)),
  index: LogIndex,
  term: Term,
  acc: List(#(LogPosition, LogEntry(command))),
) -> List(#(LogPosition, LogEntry(command))) {
  case entries {
    [] -> list.reverse(acc)
    [first, ..rest] -> {
      let current_term = case first {
        TermEntry(t) -> t
        _ -> term
      }
      scan_with_positions(rest, index + 1, current_term, [
        #(types.position(current_term, index), first),
        ..acc
      ])
    }
  }
}

/// anchor を `new_prev_position` に張り替えた後継断片。断片が位置を
/// 含まないなら None。
pub fn entries_since(
  e: LogEntries(command),
  new_prev_position: LogPosition,
) -> Option(LogEntries(command)) {
  case entries_contains(e, new_prev_position) {
    False -> None
    True -> {
      let next_index = new_prev_position.index + 1
      let anchor_marker = #(new_prev_position.index, new_prev_position.term)
      Some(LogEntries(
        prev_position: new_prev_position,
        last_position: e.last_position,
        entries: list.drop(
          e.entries,
          new_prev_position.index - e.prev_position.index,
        ),
        term_markers: list.filter(e.term_markers, fn(m) { m.0 >= next_index })
          |> list.append([anchor_marker]),
        config_markers: list.filter(e.config_markers, fn(m) {
          m.0 >= next_index
        }),
      ))
    }
  }
}

/// `suffix` の anchor が断片に含まれるなら、その位置で局所側を切り捨てて
/// suffix を連結する。含まれないなら `(e, False)` で断片は不変
/// （D1 に従い、失敗は Option でなく Bool と不変値の組で表す）。
pub fn entries_append_suffix(
  e: LogEntries(command),
  suffix: LogEntries(command),
) -> #(LogEntries(command), Bool) {
  case entries_contains(e, suffix.prev_position) {
    False -> #(e, False)
    True -> {
      let anchor_index = suffix.prev_position.index
      let kept_count = anchor_index - e.prev_position.index
      #(
        LogEntries(
          ..e,
          last_position: suffix.last_position,
          entries: list.take(e.entries, kept_count)
            |> list.append(suffix.entries),
          term_markers: suffix_term_markers_without_anchor(suffix)
            |> list.append(
              list.filter(e.term_markers, fn(m) { m.0 <= anchor_index }),
            ),
          config_markers: suffix_config_markers_without_anchor(suffix)
            |> list.append(
              list.filter(e.config_markers, fn(m) { m.0 <= anchor_index }),
            ),
        ),
        True,
      )
    }
  }
}

/// suffix 実在のマーカー（末尾の anchor 番兵を除く）を返す。連結後は
/// 局所側の残存マーカー（index <= anchor）より常に大きい index を持つため、
/// 降順リストは suffix 側を先頭に置いて連結する。
fn suffix_term_markers_without_anchor(
  suffix: LogEntries(command),
) -> List(#(LogIndex, Term)) {
  drop_last(suffix.term_markers)
}

fn suffix_config_markers_without_anchor(
  suffix: LogEntries(command),
) -> List(#(LogIndex, ClusterConfig)) {
  suffix.config_markers
}

/// `local` との共通プレフィックスを探し、anchor を合意点まで寄せる。
///
/// local の末尾位置を remote が含めばそこへ張り替え、含まなければ remote 側の term
/// マーカーを昇順に調べ、local が受け入れた最後の境界まで下りる。
/// マーカーが全て受け入れられている場合（term マーカーが番兵だけの状態を
/// 含む）は remote のまま返す。
pub fn entries_strip_common_prefix(
  remote: LogEntries(command),
  local: LogEntries(command),
) -> LogEntries(command) {
  case remote.prev_position == local.last_position {
    True -> remote
    False ->
      case entries_since(remote, local.last_position) {
        Some(since) -> since
        None ->
          scan_divergence(
            remote,
            local,
            ascending_term_markers(remote),
            remote.prev_position,
          )
      }
  }
}

fn ascending_term_markers(
  remote: LogEntries(command),
) -> List(#(LogIndex, Term)) {
  remote.term_markers
  |> drop_last
  |> list.reverse
}

fn scan_divergence(
  remote: LogEntries(command),
  local: LogEntries(command),
  markers: List(#(LogIndex, Term)),
  last_common: LogPosition,
) -> LogEntries(command) {
  case markers {
    [] -> remote
    [marker, ..rest] -> {
      let position = types.position(marker.1, marker.0)
      case entries_contains(local, position) {
        True -> scan_divergence(remote, local, rest, position)
        False -> {
          let candidate = types.position(last_common.term, marker.0 - 1)
          let anchor = case entries_contains(local, candidate) {
            True -> candidate
            False -> last_common
          }
          case entries_since(remote, anchor) {
            Some(since) -> since
            None -> remote
          }
        }
      }
    }
  }
}

/// スナップショット適用後の断片の再構成。未処理の追記アクションを
/// 再接続する経路（M2）が使う。
///
/// position が保持範囲より前なら不変（true）。それ以外では since が成功して
/// suffix を残せば true、term が合わず suffix を捨てて position 起点の空断片に
/// 張り替えた場合は false。
pub fn entries_handle_snapshot_installed(
  e: LogEntries(command),
  position: LogPosition,
) -> #(LogEntries(command), Bool) {
  case position.index < e.prev_position.index {
    True -> #(e, True)
    False ->
      case entries_since(e, position) {
        Some(since) -> #(since, True)
        None -> #(entries_new(position), False)
      }
  }
}

fn find_term_marker(
  markers: List(#(LogIndex, Term)),
  index: LogIndex,
) -> Option(Term) {
  case markers {
    [] -> None
    [marker, ..rest] ->
      case marker.0 <= index {
        True -> Some(marker.1)
        False -> find_term_marker(rest, index)
      }
  }
}

/// 指定位置の要素。gleam_stdlib 1.0.5 には `list.at` に相当する
/// インデックス参照関数がないため、`list.split` で切って先頭を読む。
fn nth(xs: List(a), i: Int) -> Option(a) {
  let #(_, rest) = list.split(xs, at: i)
  case rest {
    [first, ..] -> Some(first)
    [] -> None
  }
}

/// 末尾の要素だけを落として返す。空リストは空のまま。
fn drop_last(xs: List(a)) -> List(a) {
  case xs {
    [] -> []
    [_] -> []
    [first, ..rest] -> [first, ..drop_last(rest)]
  }
}

/// スナップショット構成と保持断片からログを作る。
pub fn log_new(
  snapshot_config: ClusterConfig,
  entries: LogEntries(command),
) -> Log(command) {
  Log(snapshot_config: snapshot_config, entries: entries)
}

/// ログ全体の末尾位置。
pub fn log_last_position(log: Log(command)) -> LogPosition {
  log.entries.last_position
}

/// ログの接続点。スナップショットの末尾位置でもある。
pub fn log_snapshot_position(log: Log(command)) -> LogPosition {
  log.entries.prev_position
}

/// 最新の有効構成。構成マーカーの最大 index のもの、無ければ
/// スナップショット構成。
pub fn log_latest_config(log: Log(command)) -> ClusterConfig {
  case log.entries.config_markers {
    [#(_, config), ..] -> config
    [] -> log.snapshot_config
  }
}

/// 最新の構成が有効になった index。構成マーカーが無ければ接続点の index。
pub fn log_latest_config_index(log: Log(command)) -> LogIndex {
  case log.entries.config_markers {
    [#(index, _), ..] -> index
    [] -> log.entries.prev_position.index
  }
}

/// index の位置で有効な構成。保持範囲外は None、範囲内で構成マーカーが
/// 無い領域はスナップショット構成を返す。
pub fn log_get_config(
  log: Log(command),
  index: LogIndex,
) -> Option(ClusterConfig) {
  case entries_contains_index(log.entries, index) {
    False -> None
    True ->
      Some(find_config_marker(
        log.entries.config_markers,
        index,
        log.snapshot_config,
      ))
  }
}

fn find_config_marker(
  markers: List(#(LogIndex, ClusterConfig)),
  index: LogIndex,
  snapshot_config: ClusterConfig,
) -> ClusterConfig {
  case markers {
    [] -> snapshot_config
    [marker, ..rest] ->
      case marker.0 <= index {
        True -> marker.1
        False -> find_config_marker(rest, index, snapshot_config)
      }
  }
}

/// index の位置と、その時点で有効な構成。どちらかが取得できなければ
/// None。
pub fn log_get_position_and_config(
  log: Log(command),
  index: LogIndex,
) -> Option(#(LogPosition, ClusterConfig)) {
  case entries_get_term(log.entries, index), log_get_config(log, index) {
    Some(term), Some(config) -> Some(#(types.position(term, index), config))
    _, _ -> None
  }
}

/// suffix を連結した新ログ。anchor が含まれなければ
/// `(log, False)` でログは不変。スナップショット構成は保持する。
pub fn log_append_suffix(
  log: Log(command),
  suffix: LogEntries(command),
) -> #(Log(command), Bool) {
  let #(entries, ok) = entries_append_suffix(log.entries, suffix)
  #(Log(..log, entries: entries), ok)
}

/// スナップショット適用後のログ再構成。保持断片が position を含むなら
/// suffix を残して構成を差し替え（true）、term が合わず suffix を捨てる
/// 場合は position 起点の空断片へ張り替える（false）。
pub fn log_handle_snapshot_installed(
  log: Log(command),
  position: LogPosition,
  config: ClusterConfig,
) -> #(Log(command), Bool) {
  case entries_since(log.entries, position) {
    Some(entries) -> #(log_new(config, entries), True)
    None -> #(log_new(config, entries_new(position)), False)
  }
}
