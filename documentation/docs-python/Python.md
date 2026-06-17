# Python Programming Guide

## Links
- Official tutorial: https://docs.python.org/3/tutorial/
- Library reference (standard library index): https://docs.python.org/3/library/
- Language reference: https://docs.python.org/3/reference/
- PEP 8 style guide: https://peps.python.org/pep-0008/

## Table of Contents
- [Python Programming Guide](#python-programming-guide)
  - [Links](#links)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [How Python Runs](#how-python-runs)
  - [Idioms and conventions](#idioms-and-conventions)
  - [Values and Types](#values-and-types)
  - [Collections](#collections)
    - [Lists](#lists)
    - [Tuples](#tuples)
    - [Dictionaries](#dictionaries)
    - [Sets](#sets)
  - [Functions](#functions)
    - [Positional and Keyword Arguments](#positional-and-keyword-arguments)
    - [\*args and \*\*kwargs](#args-and-kwargs)
  - [Comprehensions and Generators](#comprehensions-and-generators)
    - [List Comprehension](#list-comprehension)
    - [Dict Comprehension](#dict-comprehension)
    - [Generator Expression](#generator-expression)
  - [Errors and Exceptions](#errors-and-exceptions)
  - [Modules and Packages](#modules-and-packages)
    - [Import Basics](#import-basics)
    - [Package Layout](#package-layout)
    - [Standard Library Map](#standard-library-map)
  - [Files and IO](#files-and-io)
  - [Data Formats](#data-formats)
    - [JSON](#json)
    - [Dictionaries](#dictionaries-1)
      - [Methods](#methods)
      - [`.get()` vs Square Brackets](#get-vs-square-brackets)
  - [Classes and Objects](#classes-and-objects)
    - [Dataclasses (clean data containers)](#dataclasses-clean-data-containers)
  - [Type Hints](#type-hints)
  - [Common Pitfalls](#common-pitfalls)
  - [Pip and Package Management](#pip-and-package-management)
  - [Virtual Environments](#virtual-environments)

## Overview
Python is a readable, general-purpose language used for automation, APIs, data work, and scripting. It emphasizes clarity and a small amount of syntax.

## How Python Runs
Python is an interpreted language. You run source files (modules), and the interpreter executes them line by line. Files are modules, and folders with an `__init__.py` file are packages.

When a file is executed directly, `__name__` is set to `"__main__"`. That is why this pattern is common:
```python
def main() -> None:
    print("Hello from main")

if __name__ == "__main__":
    main()
```

## Idioms and conventions
```python
# **snake_case** for variables and functions
my_var = 101
# **PascalCase** for classes
class MyClass:
    pass
# **type hints** for function signatures
def greet(name: str) -> str:
    return f"Hello, {name}"
# **docstrings** to document functions and classes
def add(a: int, b: int) -> int:
    """Return the sum of a and b."""
    return a + b
# **list comprehensions** and **generator expressions** for concise code
squares = [n**2 for n in range(10)]
# **f-strings** for string interpolation
name = "Alice"
greeting = f"Hello, {name}!"
# **context managers** (**with** statements) for resource management
with open("file.txt", "r") as f:
    content = f.read()
# **logging** instead of print statements for debug output
import logging
logging.basicConfig(level=logging.INFO)
logging.info("This is an info message")
# **virtual environments** (venv) to manage dependencies
python -m venv .venv
source .venv/bin/activate
# **requirements.txt** or **pyproject.toml** to specify dependencies
pip install -r requirements.txt
# **pytest** for testing and test discovery
import pytest
def test_add():
    assert add(2, 3) == 5
# **black** or **flake8** or **ruff** for code formatting and linting
pip install ruff
ruff check .
# **double underscores** for “private” variables and methods in classes (e.g., __my_variable)
__my_variable = 42
def __my_method(self):
    pass
# **single underscores** for “protected” variables and methods in classes (e.g., _my_variable)
_my_variable = 42
def _my_method(self):
    pass
# **all caps** for constants (e.g., MAX_RETRIES)
MAX_RETRIES = 5
# using `if __name__ == "__main__":` to allow a Python file to be both imported as a module and executed as a script
if __name__ == "__main__":
    main()
# using a trailing underscore for variable names that would otherwise conflict with Python keywords (e.g., lambda_ instead of lambda)
lambda_ = lambda x: x * 2

# using AWS services in Python Lambda functions with the boto3 library
import boto3
s3 = boto3.client("s3")
s3.put_object(Bucket="my-bucket", Key="data.txt", Body="Hello, S3!")

# if elif else for conditional logic
score = 85
if score >= 90:
    grade = "A"
elif score >= 80:
    grade = "B"
else:
    grade = "C"

### For Loops
for name in ["Ada", "Linus"]:
    print(name)

### While Loops
count = 0
while count < 3:
    print(count)
    count += 1
# Switches (match/case)
def status(code: int) -> str:
    match code:
        case 200:
            return "OK"
        case 404:
            return "Not Found"
        case _:
            return "Unknown"
```

## Values and Types
Common built-in types:
- Numbers: `int`, `float`
- Text: `str`
- True/False: `bool`
- Empty: `None`

Examples:
```python
count: int = 3
pi: float = 3.14159
name: str = "Ada"
is_ready: bool = True
nothing = None
```

## Collections
### Lists
Ordered, mutable sequences.
```python
names = ["Ada", "Linus", "Grace"]
names.append("Guido")
first = names[0]
```

### Tuples
Ordered, immutable sequences.
```python
point = (10, 20)
x, y = point
```

### Dictionaries
Key-value mappings.
```python
user = {"id": 1, "name": "Ada"}
user["role"] = "admin"
```

### Sets
Unordered collections of unique items.
```python
tags = {"python", "api", "docs"}
tags.add("wiki")
```

## Functions
Define functions with `def`:
```python
def greet(name: str, excited: bool = False) -> str:
    message = f"Hello, {name}"
    return message + "!" if excited else message
```

### Positional and Keyword Arguments
```python
greet("Ada")
greet(name="Ada", excited=True)
```

### *args and **kwargs
```python
def log(message: str, *tags: str, **meta: str) -> None:
    print(message, tags, meta)

log("Started", "api", "auth", user="myUser")
```

## Comprehensions and Generators
### List Comprehension
```python
evens = [n for n in range(10) if n % 2 == 0]
```

### Dict Comprehension
```python
lengths = {name: len(name) for name in ["Ada", "Grace"]}
```

### Generator Expression
```python
total = sum(n for n in range(1_000_000))
```

## Errors and Exceptions
Use `try/except` to handle errors.
```python
try:
    value = int("not-a-number")
except ValueError as exc:
    print(f"Bad input: {exc}")
```

You can raise your own errors:
```python
def require_positive(n: int) -> int:
    if n <= 0:
        raise ValueError("n must be positive")
    return n
```

## Modules and Packages
Imports pull in code from other modules and packages.
```python
from pathlib import Path
import json
```

### Import Basics
- Absolute imports use full package paths: `from my_app.utils import parse`.
- Relative imports use `.` to move within a package: `from .utils import parse`.
- Python searches `sys.path` to find modules (current directory, site-packages, etc.).

### Package Layout
Organize code into packages:
```text
my_app/
  __init__.py
  config.py
  main.py
```

### Standard Library Map
The standard library ships with Python. Below is a grouped map of the built-in modules you will use most. For the complete index, see the Links section.

Language and core utilities:
- `builtins`, `types`, `typing`, `dataclasses`, `abc`, `enum`
- `functools`, `itertools`, `operator`, `collections`, `collections.abc`, `contextlib`

Math and numeric:
- `math`, `cmath`, `decimal`, `fractions`, `statistics`, `random`

Dates and time:
- `datetime`, `time`, `zoneinfo`, `calendar`

Filesystem and OS:
- `os`, `pathlib`, `shutil`, `glob`, `fnmatch`, `tempfile`, `filecmp`, `stat`

Text and parsing:
- `re`, `string`, `textwrap`, `difflib`, `unicodedata`

Data formats:
- `json`, `csv`, `configparser`, `tomllib`, `plistlib`
- `xml.etree.ElementTree`, `xml.dom`, `xml.sax`
- `html`, `html.parser`

Compression and archiving:
- `gzip`, `bz2`, `lzma`, `zipfile`, `tarfile`

Networking and protocols:
- `socket`, `ssl`, `ipaddress`
- `http.client`, `http.server`
- `urllib.request`, `urllib.parse`
- `ftplib`, `smtplib`, `imaplib`, `poplib`, `email`

Concurrency:
- `threading`, `multiprocessing`, `concurrent.futures`, `asyncio`, `queue`, `sched`

Processes and system:
- `subprocess`, `sys`, `platform`, `signal`, `logging`, `argparse`, `traceback`, `warnings`

Security:
- `hashlib`, `hmac`, `secrets`

Binary and memory:
- `pickle`, `struct`, `array`, `mmap`, `base64`, `binascii`

Data storage:
- `sqlite3`

Internationalization:
- `locale`, `gettext`

Packaging and runtime:
- `importlib`, `pkgutil`, `site`, `venv`, `ensurepip`, `zipapp`

GUI:
- `tkinter`

Example:
```python
from collections import Counter

counts = Counter(["api", "api", "wiki"])
```

## Files and IO
Use `with` to safely open files:
```python
from pathlib import Path

path = Path("notes.txt")
path.write_text("Hello, file", encoding="utf-8")
content = path.read_text(encoding="utf-8")
```

## Data Formats
### JSON
```python
import json

payload = {"name": "Ada", "active": True}
text = json.dumps(payload, indent=2)
data = json.loads(text)
```
### Dictionaries
#### Methods
- `get(key[, default])` Returns the value for `key`. If the key doesn't exist, it returns `None` (or a specified default) instead of raising a `KeyError`. 
- `keys()` a view object containing all the keys in the dictionary. 
- `values()` Returns a view object containing all the values. 
- `items()` Returns a view object containing tuples of `(key, value)` pairs.
- `update([other])` Updates the dictionary with elements from another dictionary or an iterable of key-value pairs. Existing keys are overwritten.
- `setdefault(key[, default])` Returns the value of a key. If the key does not exist, it inserts the key with the specified default value.
- `fromkeys(iterable[, value])` A class method that creates a new dictionary with keys from an iterable and values set to a specific value (defaults to `None`).
- `pop(key[, default])` Removes the specified key and returns its value. If the key isn't found, it returns the default (or raises `KeyError`).
- `popitem()` Removes and returns the last inserted `(key, value)` pair as a tuple. (LIFO order).
- `clear()` Removes all items from the dictionary, leaving it empty.
- `copy()` Returns a shallow copy of the dictionary.
#### `.get()` vs Square Brackets
```python
data = {"name": "Gemini", "version": 3.0}
# The "Riskier" way:
print(data["status"])  # This raises a KeyError!
status = data.get("status", "Unknown") 
print(status) # Output: Unknown
```

## Classes and Objects
Classes group data and behavior.
```python
class Server:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def url(self) -> str:
        return f"http://{self.host}:{self.port}"
```

### Dataclasses (clean data containers)
```python
from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str
```

## Type Hints
Type hints make intent clear and help tooling:
```python
from typing import Iterable

def total(values: Iterable[int]) -> int:
    return sum(values)
```

## Common Pitfalls
- Mutable default arguments:
```python
def add_item(item: str, items: list[str] | None = None) -> list[str]:
    if items is None:
        items = []
    items.append(item)
    return items
```

- Shadowing built-ins (avoid names like `list`, `dict`, `type`).

- Confusing `is` with `==` (`is` checks identity, `==` checks equality).

## Pip and Package Management
`pip` installs and manages third-party packages from PyPI. Use `python -m pip` to ensure you're targeting the correct interpreter.

Install, upgrade, and remove:
```shell
python -m pip install requests
python -m pip install --upgrade requests
python -m pip uninstall requests
```

Find and inspect packages:
```shell
python -m pip list
python -m pip show requests
python -m pip check
```

Lock dependencies to a requirements file:
```shell
python -m pip freeze > requirements.txt
python -m pip install -r requirements.txt
```

Notes:
- Keep `requirements.txt` in version control for reproducible installs.
- `pip check` helps spot incompatible or missing dependencies.


## Virtual Environments
Use a virtual environment to isolate dependencies per project. This avoids global package conflicts and keeps `pip` installs local to the project.

Create and activate:
```shell
python -m venv .venv
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# macOS/Linux
source .venv/bin/activate
```

Install packages inside the venv:
```shell
python -m pip install requests
python -m pip freeze > requirements.txt
```

Deactivate when done:
```shell
deactivate
```

Notes:
- Keep `.venv/` out of version control (add to `.gitignore`).
- Use `python -m pip` to ensure you're using the venv's pip.


