# How to setup databricks CLI
```
conda install -c conda-forge databricks-cli
pip install databricks-cli
```

```
databricks configure --token
```

- Enter your workspace URL, with the format https://<instance-name>.cloud.databricks.com.
  - Sample: https://adb-2224662069466927.7.azuredatabricks.net
- When prompted, specify the token
  - Sample Token: dapi258dc5dfd20b20d4475b05dc2b628f5b-2

- Check the configuration on windows:
```
type %USERPROFILE%\.databrickscfg
```

- Run various databricks cli commands:
```
databricks jobs list --output JSON
databricks clusters list --output JSON
databricks jobs run-now --job-id 9 --jar-params "[\"20180505\", \"alantest\"]"
databricks workspace ls
```

## Create databricks cluster using CLI
- Create cluster:
```
databricks clusters create --json-file create-cluster.json
```

- Get details of cluster created:
```
databricks clusters get --cluster-name standard-cluster
```
- Delete Cluster
```
databricks clusters delete --cluster-id "0508-080937-43ys6d98"
```

- For more details on Databricks CLI Commands, refer:
  - https://docs.databricks.com/dev-tools/cli/clusters-cli.html
