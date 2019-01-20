---
name: "\U0001F41B Bug Report"
about: Something isn't working as expected

---

## Bug Report

Please answer these questions before submitting your issue. Thanks!

1. What did you do? If possible, provide a recipe for reproducing the error.

2. What did you expect to see?

3. What did you see instead?

4. Versions of the cluster

    - DM version (run `dmctl -V` or `dm-worker -V` or `dm-master -V`):

        ```
        (paste DM version here, and your must ensure versions of dmctl, DM-worker and DM-master are the same)
        ```

    - Upstream MySQL/MariaDB server version:

        ```
        (paste upstream MySQL/MariaDB server version here)
        ```

    - Downstream TiDB cluster version (execute `SELECT tidb_version();` in a MySQL client):

        ```
        (paste TiDB cluster version here)
        ```

    - How did you deploy DM: DM-Ansible or via manually?

        ```
        (leave DM-Ansible or manually here)
        ```

    - Other interesting information (system version, hardware config, etc):

        >
        >

5. current status of DM cluster (execute `query-status` in dmctl)

6. Operation logs
   - Please upload `dm-worker.log` for every DM-worker instance if possible
   - Please upload `dm-master.log` if possible
   - Other interesting logs
   - Output of dmctl's commands with problems
   
7. Configuration of the cluster and the task
   - `dm-worker.toml` for every DM-worker instance if possible
   - `dm-master.toml` for DM-master if possible
   - task config, like `task.yaml` if possible
   - `inventory.ini` if deployed by DM-Ansible

8. Screenshot/exported-PDF of Grafana dashboard or metrics' graph in Prometheus for DM if possible
