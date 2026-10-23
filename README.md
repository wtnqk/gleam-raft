# gleam-raft

## 概要

gleam-raft は、Gleam 向けの最小で機能完備な sans-IO Raft core です。

I/O を内包せず、入力を純粋な状態変換として処理し、副作用を `Action` として呼び出し側へ返します。BEAM を主対象とし、Node.js 上の JavaScript にも対応します。

## 最小サンプル

単独ノードでクラスタを作り、コマンドを提案し、Action を消化して commit を確認する。このコードは実行されるものそのままで、CI が README から抽出して走らせる（下の「このサンプルの実行方法」を参照）。

<!-- readme-sample:start -->
```gleam
// 最小サンプル: 単独ノードでクラスタを作り、コマンドを提案し、Action を
// 消化して commit を確認する。このコードは README に掲載したものそのままで、
// scripts/readme_sample.sh が README から抽出して実行する。
import gleam/int
import gleam/io
import gleam/option.{None, Some}
import gleam_raft
import gleam_raft/types.{
  type CommitStatus, type Role, Candidate, Committed, Follower, InProgress,
  Leader, Rejected, Unknown,
}

pub fn main() -> Nil {
  let started = gleam_raft.start(1)
  let #(after_create, created) = gleam_raft.create_cluster(started, [1])
  case created {
    Error(_) -> io.println("クラスター形成に失敗しました")
    Ok(_) -> {
      let leader = drain(after_create)
      io.println("形成後の役割: " <> role_name(gleam_raft.role(leader)))
      let #(after_propose, proposed) =
        gleam_raft.propose_command(leader, "write:hello")
      case proposed {
        None -> io.println("leader でないため提案は却下されました")
        Some(position) -> {
          let settled = drain(after_propose)
          io.println(
            "commit 状態: "
            <> status_name(gleam_raft.get_commit_status(settled, position)),
          )
          io.println(
            "commit index: " <> int.to_string(gleam_raft.commit_index(settled)),
          )
        }
      }
    }
  }
}

// 優先順に Action を 1 件ずつ取り出し、取り尽くすまで回す。sans-IO なので
// 実行は呼び出し側の責務で、単独ノードは実行を伴わなくても状態が進む。
// 実運用ではこの位置にストア書き込み・RPC 送信・タイマー設定が来る。
fn drain(node: gleam_raft.Node(String)) -> gleam_raft.Node(String) {
  case gleam_raft.take_action(node) {
    #(after, None) -> after
    #(after, Some(_action)) -> drain(after)
  }
}

fn role_name(role: Role) -> String {
  case role {
    Leader -> "Leader"
    Candidate -> "Candidate"
    Follower -> "Follower"
  }
}

fn status_name(status: CommitStatus) -> String {
  case status {
    InProgress -> "InProgress"
    Committed -> "Committed"
    Rejected -> "Rejected"
    Unknown -> "Unknown"
  }
}
```
<!-- readme-sample:end -->

得られる出力:

```
形成後の役割: Leader
commit 状態: Committed
commit index: 3
```

`commit index: 3` はサンプル固有の値である。提案の前にクラスタ形成の過程で commit index が 2 まで進んでおり、提案は index 3 に載る。

実行方法: `examples/readme_sample/` はこのリポジトリを path 依存する別パッケージで、その `src/readme_sample.gleam` は上記ブロックから生成される写しにすぎない。生成と実行はまとめて次のスクリプトが行う。

```sh
./scripts/readme_sample.sh               # 既定は Erlang
./scripts/readme_sample.sh javascript    # Node.js でも同じ出力になる
```

CI（.github/workflows/test.yml）も同じスクリプトを Erlang で走らせるため、掲載サンプルは放置によって腐らない構造になっている。
