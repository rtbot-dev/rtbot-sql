# RtBot SQL

Real-time SQL computation engine for streaming numerical data. Write SQL to define streaming pipelines — RtBot compiles them into high-performance C++ programs that process data incrementally, one message at a time.

## Quick start

```python
import math
from rtbot_sql import RtBotSql

sql = RtBotSql()
sql.configure_time_format(formatter=lambda ts: ts)

sql.execute("CREATE STREAM sensors (temperature DOUBLE)")
sql.execute("""
  CREATE MATERIALIZED VIEW stats AS
    SELECT temperature,
           MOVING_AVERAGE(temperature, 50) AS avg_temp,
           MOVING_STD(temperature, 50) AS std_temp
    FROM sensors
""")

# Generate 200 readings: smooth sine wave with 3 injected spikes
readings = []
for i in range(200):
    temp = 20.0 + 2.0 * math.sin(i * 2 * math.pi / 60) + 0.3 * math.sin(i * 7.1)
    if i == 80:
        temp = 35.0   # spike high
    elif i == 130:
        temp = 5.0    # spike low
    elif i == 170:
        temp = 38.0   # spike high
    readings.append({"time": i, "temperature": temp})

sql.insert_dataframe("sensors", readings)

# Query only the anomalies
result = sql.execute("""
  SELECT * FROM stats
  WHERE ABS(temperature - avg_temp) > 2 * std_temp
""")

for col in result["columns"]:
    print(f"{col:>15}", end="")
print()
for row in result["rows"]:
    for val in row:
        print(f"{val:>15.2f}", end="")
    print()
```

Output:

```
    temperature       avg_temp       std_temp
          35.00          20.10           2.61
           5.00          19.27           2.39
          38.00          20.23           3.65
```

See the [full documentation](https://rtbot.dev/docs) for more examples and the SQL reference.

### With pandas

```python
from rtbot_sql import RtBotSql
import pandas as pd

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
