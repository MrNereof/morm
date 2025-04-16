# 🌿MORM

Lightweight asynchronous MongoDB ORM.

## Installation

```bash
pip install morm-python
```

## Usage

```python
from morm import Database, Model

db = Database(name="db")


@db
class User(Model):
    name: str
    password: str


await User(name="John Doe", password="zaq123").create()

await User.get(name="John Doe")
```