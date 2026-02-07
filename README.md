# databricks_exam_skills

A focused repo for Databricks exam prep skills, set up for Codex to load skill guides from `.agents/skills`.

## What’s inside
- `.agents/skills/databricks-unity-catalog/`
  - `SKILL.md`: the main skill guide
  - `references/`: supporting reference material

## How skills work
Skills are guidance documents. They are not executed like scripts. Codex loads a skill when your prompt matches the skill’s `description` in `SKILL.md`.

Example prompt to trigger the skill:
"Explain Unity Catalog permissions and hierarchy for exam prep."

## How to use in Codex
1. Open a Codex session in this repo.
2. Ask a question that matches the skill’s description.
3. Codex loads the skill and follows the instructions.

## Adding more skills
1. Create a folder under `.agents/skills/<skill-name>/`
2. Add a `SKILL.md` with `name` and `description` frontmatter
3. Add optional `references/`, `scripts/`, or `assets/`
4. Update this README

## GitHub Actions
This repo includes a minimal CI workflow at `.github/workflows/ci.yml`.
