# Challenges and Lessons Learned

Building this project surfaced several practical lessons that are common in real data engineering work. The biggest challenge was not the business logic itself, but the surrounding environment and platform setup needed to run Spark and PostgreSQL reliably.

## Native Windows Spark Can Be Fragile

Running PySpark directly on Windows introduced Hadoop filesystem permission issues during Parquet writes. Even after configuring `winutils.exe`, native Windows Spark remained unreliable for this workflow.

## WSL Is a Better Local Runtime for Spark

Switching Spark execution to Ubuntu in WSL made the pipeline much more stable. The project code stayed on the Windows filesystem, while the Python virtual environment and Spark runtime ran inside Linux.

## Local Networking Between WSL and PostgreSQL Matters

Once Spark was running inside WSL, PostgreSQL connectivity became the next challenge. `localhost` did not work because the warehouse was running on Windows while the Spark jobs ran inside Ubuntu. The solution was to connect through `host.docker.internal` and adjust PostgreSQL access rules so the WSL environment could authenticate successfully.

## PostgreSQL Configuration Is Part of the Platform

The warehouse setup required more than just creating tables. PostgreSQL had to be configured to accept network connections from WSL through `postgresql.conf` and `pg_hba.conf`.

## Environment Management Is a Real Engineering Skill

This project required fixing dependency conflicts, managing Python environments across Windows and Linux, setting Java correctly for Spark, and troubleshooting system-level issues.

## Incremental Design Paid Off

Because the project was built around `batch_date`, it was possible to rerun a single day repeatedly while debugging setup issues.

## Key Takeaway

One of the most important lessons from this project is that data engineering is not just about writing transformations. It also involves operating systems, package management, runtime configuration, connectivity, validation, and observability.
