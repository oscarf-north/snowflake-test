# Snowflake Git Integration Project

This repository manages Snowflake database objects using version control (Git) and automated deployment (GitHub Actions).

## Goals
- Export all database objects (tables, views, procedures) from Snowflake. 
- Store definitions in Git as '.sql' files.
- Use CI/CD to apply changes on files. 
- Enable full change tracking. 

## Structure Overview
- /db -> Source of truth for database objects. 
- /scripts -> Utility scripts for extraction and deployment. 
- /config -> Environment and deployment parameters. 
- /docs -> Documentation