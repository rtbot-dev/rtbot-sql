# RtBot SQL

Real-time SQL computation engine for streaming numerical data. Write SQL to define streaming pipelines — RtBot compiles them into high-performance C++ programs that process data incrementally, one message at a time.

## Quick start

```python
from rtbot_sql import RtBotSql

sql = RtBotSql()
sql.execute("CREATE STREAM sensors (temperature DOUBLE, pressure DOUBLE)")
sql.execute("""
  CREATE MATERIALIZED VIEW alerts AS
    SELECT temperature, pressure,
           MOVING_AVERAGE(temperature, 50) AS avg_temp,
           MOVING_STD(temperature, 50) AS std_temp
    FROM sensors
    WHERE ABS(temperature - MOVING_AVERAGE(temperature, 50)) > 2 * MOVING_STD(temperature, 50)
""")

import pandas as pd
df = pd.read_csv("sensor_history.csv")
sql.insert_dataframe("sensors", df)
results = sql.execute("SELECT * FROM alerts")
```

## Features

- **One language, all stages** — the SQL you write in a notebook is the same SQL that runs in production
- **Incremental execution** — each new data point updates pipeline state in constant time
- **Deterministic** — same input always produces the same output, regardless of timing
- **High performance** — native C++ engine with Python bindings via pybind11

## Documentation

- [Getting Started](https://rtbot.dev/docs/user-guide/getting-started)
- [Python Quickstart](https://rtbot.dev/docs/user-guide/quickstart-python)
- [SQL Reference](https://rtbot.dev/docs/reference/rtbot-sql/overview)

## License

Proprietary. See [rtbot.dev](https://rtbot.dev) for licensing options.
