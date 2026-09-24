# Security

This repository is **public**. Please treat it with that in mind.

## Reporting a vulnerability

If you believe you have found a security vulnerability in this project,
please **do not** open a public GitHub issue or PR describing it. Instead:

1. **Email**: ryan.restivo(at)gmail.com — replace `(at)` with `@` when composing.
2. **Subject line**: `[puller] <short description>`
3. **Include** (only what is needed to reproduce safely):
   - the file / line / commit SHA
   - a *description* of the flaw (do NOT paste actual credential values)
   - a safe reproduction if one exists

I will aim to acknowledge within **72 hours** and to ship a patch within
**7 days** of a confirmed-impact vulnerability, whichever is reasonable.

## What to do instead if you find a *live* credential

This repo does not (as of this SECURITY.md being added) contain any known
live credentials in git history. If you believe you have found one:

- **Do not** use it.
- **Do not** paste it into a PR or issue.
- **Do** email the address above so the credential can be rotated before
  it is used.

## What I will NOT act on

- Reports of `.gitignore` being bypassed by filename variants (this is a
  net, not a scanner — see `.github/workflows/security-scanner.yaml` for
  the enforcement mechanism).
- Reports of "unpinned dependencies" in general — see `.github/dependabot.yml`
  and the Dependabot PRs this repo generates.

## Security posture (as of this file being added)

- No secrets committed in tracked files (verified by masked scan of every
  tracked file and every branch in history).
- All credential injection via GitHub Actions `${{ secrets.* }}` → `os.getenv(...)`.
- Commit-time secret scanning: gitleaks via `security-scanner.yaml`.
- Supply chain: Dependabot (`dependabot.yml`) for `actions/*`, `requirements.txt`,
  `nlp_requirements.txt`.
- Action pins: 41 of 44 workflows on `actions/checkout@v6` / `setup-python@v6`
  / `cache@v5`; 3 historical workflows (`source_data_two.yaml`, `stories.yaml`,
  `newsroom_embeddings_one.yaml`) carried older pins and are being bumped by
  the same hardening PR.
