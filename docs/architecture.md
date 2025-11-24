# ARCHITECTURE

This project implements automated synchronization from **Snowflake to GitHub** during the migration phase, allowing database developers to continue making changes directly in Snowflake. A Python script compares live DDLs in Snowflake with the corresponding files in GitHub, if differences are detected, it updates GitHub to reflect Snowflake.

> **During migration: Snowflake is treated as the operational source of truth.**  
> GitHub serves as the version-controlled backup repository.

---

## 1. Migration vs Final State

| Phase | Source of Truth | How updates are applied |
|------|----------------|---------------------------|
| **Migration (current)** | **Snowflake** | Developers modify directly in Snowflake → script syncs to GitHub |
| **Post-Migration (future final state)** | **GitHub** | Developers modify SQL in Git → PR + CI/CD → deployed to Snowflake |

Once all existing objects are correctly versioned and validated in GitHub, the direction will reverse, and GitHub will become the primary change management platform.

---

## 2. Current Workflow (Migration Phase)

Developer → Snowflake → DDL Extraction Script → Compare with GitHub → Commit updates to GitHub

---

## 3. Planned Final Workflow (Post-Migration)

Developer → Git Pull Request → Review & Approval → CI/CD → Deploy SQL to Snowflake
(Implementation of final workflow will be defined later.)

---

## 4. Repository Structure (Simplified)

/db
└── <DATABASE>
    └── <SCHEMA>
        ├── tables/
        ├── views/
        └── procedures/

/scripts
sync_snowflake_to_git.py # Compares Snowflake vs GitHub
test_sf.py # Test Snowflake connection
test_github.py # Test GitHub connection

/config
env_template.sh # Environment variable template (no secrets committed)

/.github/workflows
deploy_snowflake.yml # Future CI/CD deployment script

/docs
ARCHITECTURE.md

/requirements.txt

---

## 5. Synchronization Logic

- Extract DDLs from Snowflake using stored procedures.
- Write outputs into SP: `/PRD_HOSPENG_REPORTING/GITHUB/tables/EXPORT_DDLS_SP`, tables: `/PRD_HOSPENG_REPORTING/GITHUB/tables/EXPORT_DDLS_TABLES` and views: `/PRD_HOSPENG_REPORTING/GITHUB/tables/EXPORT_DDLS_VIEWS`.
- Python script compare against existing Git files.
- If differences exist, commit changes to GitHub.
- File naming must fully match Snowflake object references.

---

## 6. Security & Authentication

- Private key authentication for Snowflake.
- GitHub access via Personal Access Token or GitHub App.
- Environment variables managed in `env_local.sh` for local development (excluded from Git).

---

## 7. Summary

> **Current state:** Snowflake holds the primary model → tool syncs to GitHub.  
> **Future state:** GitHub will define objects → automated deployment to Snowflake.

This migration project ensures consistent capture of existing Snowflake objects into Git for full visibility and version control, preparing the foundation for a Git-led CI/CD deployment model.