#!/usr/bin/env bash
# ============================================================================
# 统一的「提交 + 推送」封装，供 GitHub Actions 使用
#
# 为什么需要它（2026-11-16 那次 daily 失败的直接原因）：
#   1) push 被拒：main 被别的写入者推进（周更 CFTC 工作流、或人工/本地提交）；
#   2) 回退用的 `git pull --rebase` 又被脏工作区挡住：
#      `error: cannot pull with rebase: You have unstaged changes.`
#      因为工作流除了要提交的文件，还会留下 data/index_point_history.json 之类的产物，
#      原来的 `git add data/macro_history.json data/holdings` 覆盖不到它们；
#   3) 单次 push 失败就让整步失败（exit 128），后面的部署跟着一起挂掉。
#
# 这里的做法：按路径 git add（含删除）→ 有变化才 commit → push；
# 被拒则 `pull --rebase --autostash` 后重试，最多 3 次，仍失败才真正报错。
#
# 用法: bash .github/scripts/commit_and_push.sh "<提交信息>" <路径> [<路径>...]
# ============================================================================
set -uo pipefail

msg="${1:-}"
shift || true
if [ -z "$msg" ] || [ "$#" -eq 0 ]; then
  echo "::error::用法: commit_and_push.sh \"<提交信息>\" <路径> [<路径>...]"
  exit 2
fi

git config --local user.email "github-actions[bot]@users.noreply.github.com"
git config --local user.name  "github-actions[bot]"

# -A：目录里的删除（例如季报缓存被清空）也要一起进暂存区，
#     否则工作区一直是脏的，后面的 rebase 仍会被拒。
for p in "$@"; do
  if ! git add -A -- "$p" 2>/dev/null; then
    echo "::notice::路径不存在或无法加入暂存区，已跳过: $p"
  fi
done

if git diff --staged --quiet; then
  echo "No changes to commit: $msg"
  exit 0
fi

git commit -m "$msg"
echo "已提交: $(git log --oneline -1)"

branch="${GITHUB_REF_NAME:-main}"
for attempt in 1 2 3; do
  if git push; then
    echo "✅ 推送成功（第 ${attempt} 次尝试）"
    exit 0
  fi

  if [ "$attempt" -ge 3 ]; then
    echo "::error::push 连续 3 次被拒，放弃（本次缓存未能持久化）"
    exit 1
  fi

  echo "::warning::push 被拒（第 ${attempt} 次），拉取远端 ${branch} 后重试"
  # --autostash：本脚本之外的产物（如 data/index_point_history.json）还是脏的，
  #              不带 autostash 就会直接报 cannot pull with rebase。
  if ! git pull --rebase --autostash origin "$branch"; then
    git rebase --abort 2>/dev/null || true
    echo "::error::rebase 远端失败（可能有冲突），需人工处理"
    exit 1
  fi
  sleep 5
done
