<h1 align="center">Data warehouse tech stack for city traffic data 👋</h1>
<p>
</p>

> The pNeuma dataset was used for this project. The data was generated by drones that recorded trajectory information for different vehicles. This information was then saved in CSV files that are available for download from <a href=https://open-traffic.epfl.ch/index.php/downloads/#1599047632450-ebe509c8-1330>Downloads - pNeuma</a>. More information about the pNeuma experiment can be found at <a href=https://open-traffic.epfl.ch/index.php/downloads/#1599047632450-ebe509c8-1330>About - pNeuma</a>.

## Project Structure

```
traffic-dwh-tech-stack
├─ .gitignore
├─ LICENSE
├─ README.md
├─ airflow-traffic
│  ├─ airflow.cfg
│  ├─ airflow.db
│  ├─ dags
│  │  └─ dag_tasks
│  │     ├─ create_tables.py
│  │     ├─ db_connect.py
│  │     └─ upload_to_db.py
│  ├─ docker-compose.yaml
│  ├─ plugins
│  ├─ postgresql
│  │  └─ db_schema.sql
│  └─ webserver_config.py
└─screenshots
   ├─ airflow-screenshot1.png
   ├─ airflow-screenshot2.png
   ├─ airflow-screenshot3.png
   └─ airflow-screenshot4.png
```


## Author

👤 **Janerose Nyambura Njogu**