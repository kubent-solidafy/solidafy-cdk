# Public Repo Sync Process

This document describes how to sync changes from the private repo to the public open-source repo.

## Repositories

| Repo | URL | Purpose |
|------|-----|---------|
| Private (origin) | `https://github.com/kubent/solidafy-cdk.git` | Development, full history |
| Public | `https://github.com/kubent-solidafy/solidafy-cdk.git` | Open source release |

## Files Excluded from Public Repo

The following files are **private** and excluded from the public repo:

- `CLAUDE.md` - AI assistant context
- `MEDIUM_ARTICLE.md` - Draft marketing content
- `IMPLEMENTATION_PLAN.md` - Internal planning docs
- `/state` - Local state files (root level only)
- `/.cargo` - Local cargo config (external drive target)

## Sync Command

Run from the project root (`/Users/kubent/solidafy-platform/solidafy-cdk`):

```bash
# Create temp directory, rsync with exclusions, init git, push
cd /tmp && \
rm -rf solidafy-cdk-public && \
mkdir solidafy-cdk-public && \
cd solidafy-cdk-public && \
rsync -av \
  --exclude='.git' \
  --exclude='/CLAUDE.md' \
  --exclude='/MEDIUM_ARTICLE.md' \
  --exclude='/IMPLEMENTATION_PLAN.md' \
  --exclude='/state' \
  --exclude='/.cargo' \
  /Users/kubent/solidafy-platform/solidafy-cdk/ . && \
git init && \
git remote add origin https://github.com/kubent-solidafy/solidafy-cdk.git && \
git fetch origin main && \
git checkout -b main && \
git add -A && \
git commit -m "YOUR COMMIT MESSAGE HERE" && \
git push -f origin main
```

## One-liner (copy-paste ready)

Replace `YOUR_MESSAGE` with your commit message:

```bash
cd /tmp && rm -rf solidafy-cdk-public && mkdir solidafy-cdk-public && cd solidafy-cdk-public && rsync -av --exclude='.git' --exclude='/CLAUDE.md' --exclude='/MEDIUM_ARTICLE.md' --exclude='/IMPLEMENTATION_PLAN.md' --exclude='/state' --exclude='/.cargo' /Users/kubent/solidafy-platform/solidafy-cdk/ . && git init && git remote add origin https://github.com/kubent-solidafy/solidafy-cdk.git && git fetch origin main && git checkout -b main && git add -A && git commit -m "YOUR_MESSAGE" && git push -f origin main
```

## When to Sync

Sync to public repo when:
- ✅ All tests pass locally
- ✅ CI builds succeed (linux-x86_64)
- ✅ Feature is complete and tested
- ✅ No sensitive data in commit

## Verification

After syncing, verify at: https://github.com/kubent-solidafy/solidafy-cdk

Check that:
1. CI workflow triggers
2. Build succeeds
3. Excluded files are not present
