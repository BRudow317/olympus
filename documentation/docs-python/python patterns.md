# Python Patterns

---
# Handy Python files

## sitecustomize.py 
- a file that can be placed in the site-packages directory to run code on interpreter startup. 
- This is useful for setting up logging, environment variables, or other global configurations.
## __init__.py 
- a file that can be placed in a package directory to run code when the package is imported. 
- This is useful for initializing package-level variables, setting up logging, or performing other setup tasks.
## __main__.py
- a file that can be placed in a package directory to allow the package to be run as a script. 
- This tells Python to execute this file if the package is run directly.
## usercustomize.py
- same as sitecustomize.py but runs after it and is user-scoped (respects `PYTHONPATH`).
- Useful when you don't have write access to site-packages but still want per-user startup behavior.

## pyproject.toml
- the modern standard (PEP 517/518) for defining a Python project. Replaces setup.py for most use cases.
- tells build tools (pip, poetry, hatch, etc.) everything they need to install, build, or publish your package.
- lives at the project root alongside your source code.
- has three main jobs: declare the build backend, define project metadata, and configure tools (pytest, ruff, mypy, etc.) all in one file.
```toml
[build-system]
# tells pip which tool to use to build the package before installing it
# without this section pip may not know how to install your project from source
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "quickbitlabs"
version = "0.1.0"
description = "quickbitlabs pipeline orchestrator"
requires-python = ">=3.11"

# runtime dependencies - installed automatically when someone installs your package
dependencies = [
    "requests>=2.31",
    "psycopg2-binary>=2.9",
]

[project.optional-dependencies]
# installed only on request: pip install quickbitlabs[dev]
dev = ["pytest", "ruff"]

[project.scripts]
# creates a CLI command in the venv's bin/ after install
# running "quickbitlabs" in the terminal calls master.py:main()
quickbitlabs = "master:main"

[tool.setuptools.packages.find]
# tells setuptools which folders to include as packages
# without this it may miss subfolders like server/connectors/sf
where = ["."]
include = ["server*", "utils*"]

[tool.pytest.ini_options]
# pytest config lives here instead of a separate pytest.ini
testpaths = ["server/connectors/sf/tests"]

[tool.ruff]
line-length = 120
```

## setup.py
- the older way to define a Python package. Predates pyproject.toml and is now mostly legacy.
- still sometimes needed for packages with C extensions or complex build steps that pyproject.toml cannot express.
- if you have both, pyproject.toml takes precedence for metadata. A modern setup.py is often just two lines that delegate to setuptools.
- for new projects: use pyproject.toml instead. For existing projects: migrate when it is convenient.
```python
# minimal modern setup.py - just a shim so legacy tools that call
# "python setup.py install" still work. All real config is in pyproject.toml.
from setuptools import setup
setup()
```
```python
# full standalone setup.py (older style, still valid but verbose)
from setuptools import setup, find_packages

setup(
    name="quickbitlabs",
    version="0.1.0",
    packages=find_packages(),          # auto-discovers all packages with __init__.py
    install_requires=[
        "requests>=2.31",
        "psycopg2-binary>=2.9",
    ],
    entry_points={
        # same as [project.scripts] in pyproject.toml
        "console_scripts": [
            "quickbitlabs=master:main",
        ],
    },
    python_requires=">=3.11",
)
```


## __all__ to control module exports
- defines the public API of a module. Only names listed in `__all__` are exported when someone does `from module import *`. Also signals intent to readers about what is considered stable/public.
```python
from .rest import SfRest
from .bulk2 import SfBulk2

__all__ = ["SfRest", "SfBulk2"]
```


---
# Handy Decorators

## contextlib.contextmanager
- turns a generator function into a context manager without writing a full class with `__enter__`/`__exit__`. The `yield` is where the `with` block runs.
```python
from contextlib import contextmanager

@contextmanager
def sf_session(config: SalesforceConfig):
    token, url = fetch_credentials(config)
    try:
        yield SalesforceConnector(token, url)
    finally:
        pass  # cleanup if needed

with sf_session(config) as sf:
    sf.rest.query("SELECT Id FROM Lead LIMIT 10")
```

## dataclasses
- removes boilerplate `__init__`, `__repr__`, and `__eq__` for data-holding classes. Use `field()` for mutable defaults.
```python
from dataclasses import dataclass, field

@dataclass
class SalesforceConfig:
    base_url: str
    consumer_key: str
    consumer_secret: str
    scopes: list[str] = field(default_factory=list)
```

## lru_cache for memoization
- caches the return value of a function keyed by its arguments. Useful for expensive calls that repeat with the same inputs (e.g. describe calls, schema lookups).
```python
from functools import lru_cache

@lru_cache(maxsize=128)
def get_object_fields(object_name: str) -> dict:
    return sf.rest.describe(object_name)  # only hits the API once per object
```

---
# Dynamic Python

## getattr to dynamically call methods
```python
class MyService:
    mode: str = ""
    def request_soap(self):
        return "SOAP Request Sent"
    def request_rest(self):
        return "REST Request Sent"

service = MyService(mode="soap")
method_name = f"request_{service.mode}"

# 1. Grab the method (it's just an object right now)
func = getattr(service, method_name, None)

# 2. Check if it exists and run it adding back the parentheses
if callable(func):
    print(func()) # Output: SOAP Request Sent
else:
    print("Method not found or not runnable.")
```

# uv the modern python package manager
- uv (short for "universal") is a new package manager that aims to unify the best features of pip, poetry, and pipenv. It uses pyproject.toml for configuration and supports both virtual environments and global installs.
- To add a dependency, simply run `uv add package-name`. This updates pyproject.toml and installs the package. You can also specify dev dependencies with `uv add package-name --dev`.
```bash
uv add requests          # adds to [project.dependencies] + installs
uv add pytest --dev      # adds to [project.optional-dependencies] dev group
uv remove requests       # removes it
```

---
# requests
A library for making http requests that's mostly fleshed out and simple to use. See the Sessions class in the requests library.

```python
class Session(SessionRedirectMixin):
    """A Requests session.

    Provides cookie persistence, connection-pooling, and configuration.

    Basic Usage::

      >>> import requests
      >>> s = requests.Session()
      >>> s.get('https://httpbin.org/get')
      <Response [200]>

    Or as a context manager::

      >>> with requests.Session() as s:
      ...     s.get('https://httpbin.org/get')
      <Response [200]>
    """

    __attrs__ = [
        "headers",
        "cookies",
        "auth",
        "proxies",
        "hooks",
        "params",
        "verify",
        "cert",
        "adapters",
        "stream",
        "trust_env",
        "max_redirects",
    ]

    def __init__(self):
        #: A case-insensitive dictionary of headers to be sent on each
        #: :class:`Request <Request>` sent from this
        #: :class:`Session <Session>`.
        self.headers = default_headers()

        #: Default Authentication tuple or object to attach to
        #: :class:`Request <Request>`.
        self.auth = None

        #: Dictionary mapping protocol or protocol and host to the URL of the proxy
        #: (e.g. {'http': 'foo.bar:3128', 'http://host.name': 'foo.bar:4012'}) to
        #: be used on each :class:`Request <Request>`.
        self.proxies = {}

        #: Event-handling hooks.
        self.hooks = default_hooks()

        #: Dictionary of querystring data to attach to each
        #: :class:`Request <Request>`. The dictionary values may be lists for
        #: representing multivalued query parameters.
        self.params = {}

        #: Stream response content default.
        self.stream = False

        #: SSL Verification default.
        #: Defaults to `True`, requiring requests to verify the TLS certificate at the
        #: remote end.
        #: If verify is set to `False`, requests will accept any TLS certificate
        #: presented by the server, and will ignore hostname mismatches and/or
        #: expired certificates, which will make your application vulnerable to
        #: man-in-the-middle (MitM) attacks.
        #: Only set this to `False` for testing.
        self.verify = True

        #: SSL client certificate default, if String, path to ssl client
        #: cert file (.pem). If Tuple, ('cert', 'key') pair.
        self.cert = None

        #: Maximum number of redirects allowed. If the request exceeds this
        #: limit, a :class:`TooManyRedirects` exception is raised.
        #: This defaults to requests.models.DEFAULT_REDIRECT_LIMIT, which is
        #: 30.
        self.max_redirects = DEFAULT_REDIRECT_LIMIT

        #: Trust environment settings for proxy configuration, default
        #: authentication and similar.
        self.trust_env = True

        #: A CookieJar containing all currently outstanding cookies set on this
        #: session. By default it is a
        #: :class:`RequestsCookieJar <requests.cookies.RequestsCookieJar>`, but
        #: may be any other ``cookielib.CookieJar`` compatible object.
        self.cookies = cookiejar_from_dict({})

        # Default connection adapters.
        self.adapters = OrderedDict()
        self.mount("https://", HTTPAdapter())
        self.mount("http://", HTTPAdapter())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def prepare_request(self, request):
        """Constructs a :class:`PreparedRequest <PreparedRequest>` for
        transmission and returns it. The :class:`PreparedRequest` has settings
        merged from the :class:`Request <Request>` instance and those of the
        :class:`Session`.

        :param request: :class:`Request` instance to prepare with this
            session's settings.
        :rtype: requests.PreparedRequest
        """
        cookies = request.cookies or {}

        # Bootstrap CookieJar.
        if not isinstance(cookies, cookielib.CookieJar):
            cookies = cookiejar_from_dict(cookies)

        # Merge with session cookies
        merged_cookies = merge_cookies(
            merge_cookies(RequestsCookieJar(), self.cookies), cookies
        )

        # Set environment's basic authentication if not explicitly set.
        auth = request.auth
        if self.trust_env and not auth and not self.auth:
            auth = get_netrc_auth(request.url)

        p = PreparedRequest()
        p.prepare(
            method=request.method.upper(),
            url=request.url,
            files=request.files,
            data=request.data,
            json=request.json,
            headers=merge_setting(
                request.headers, self.headers, dict_class=CaseInsensitiveDict
            ),
            params=merge_setting(request.params, self.params),
            auth=merge_setting(auth, self.auth),
            cookies=merged_cookies,
            hooks=merge_hooks(request.hooks, self.hooks),
        )
        return p

    def request(
        self,
        method,
        url,
        params=None,
        data=None,
        headers=None,
        cookies=None,
        files=None,
        auth=None,
        timeout=None,
        allow_redirects=True,
        proxies=None,
        hooks=None,
        stream=None,
        verify=None,
        cert=None,
        json=None,
    ):
        """Constructs a :class:`Request <Request>`, prepares it and sends it.
        Returns :class:`Response <Response>` object.

        :param method: method for the new :class:`Request` object.
        :param url: URL for the new :class:`Request` object.
        :param params: (optional) Dictionary or bytes to be sent in the query
            string for the :class:`Request`.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param json: (optional) json to send in the body of the
            :class:`Request`.
        :param headers: (optional) Dictionary of HTTP Headers to send with the
            :class:`Request`.
        :param cookies: (optional) Dict or CookieJar object to send with the
            :class:`Request`.
        :param files: (optional) Dictionary of ``'filename': file-like-objects``
            for multipart encoding upload.
        :param auth: (optional) Auth tuple or callable to enable
            Basic/Digest/Custom HTTP Auth.
        :param timeout: (optional) How many seconds to wait for the server to send
            data before giving up, as a float, or a :ref:`(connect timeout,
            read timeout) <timeouts>` tuple.
        :type timeout: float or tuple
        :param allow_redirects: (optional) Set to True by default.
        :type allow_redirects: bool
        :param proxies: (optional) Dictionary mapping protocol or protocol and
            hostname to the URL of the proxy.
        :param hooks: (optional) Dictionary mapping hook name to one event or
            list of events, event must be callable.
        :param stream: (optional) whether to immediately download the response
            content. Defaults to ``False``.
        :param verify: (optional) Either a boolean, in which case it controls whether we verify
            the server's TLS certificate, or a string, in which case it must be a path
            to a CA bundle to use. Defaults to ``True``. When set to
            ``False``, requests will accept any TLS certificate presented by
            the server, and will ignore hostname mismatches and/or expired
            certificates, which will make your application vulnerable to
            man-in-the-middle (MitM) attacks. Setting verify to ``False``
            may be useful during local development or testing.
        :param cert: (optional) if String, path to ssl client cert file (.pem).
            If Tuple, ('cert', 'key') pair.
        :rtype: requests.Response
        """
        # Create the Request.
        req = Request(
            method=method.upper(),
            url=url,
            headers=headers,
            files=files,
            data=data or {},
            json=json,
            params=params or {},
            auth=auth,
            cookies=cookies,
            hooks=hooks,
        )
        prep = self.prepare_request(req)

        proxies = proxies or {}

        settings = self.merge_environment_settings(
            prep.url, proxies, stream, verify, cert
        )

        # Send the request.
        send_kwargs = {
            "timeout": timeout,
            "allow_redirects": allow_redirects,
        }
        send_kwargs.update(settings)
        resp = self.send(prep, **send_kwargs)

        return resp

    def get(self, url, **kwargs):
        r"""Sends a GET request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        kwargs.setdefault("allow_redirects", True)
        return self.request("GET", url, **kwargs)

    def options(self, url, **kwargs):
        r"""Sends a OPTIONS request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        kwargs.setdefault("allow_redirects", True)
        return self.request("OPTIONS", url, **kwargs)

    def head(self, url, **kwargs):
        r"""Sends a HEAD request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        kwargs.setdefault("allow_redirects", False)
        return self.request("HEAD", url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        r"""Sends a POST request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param json: (optional) json to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        return self.request("POST", url, data=data, json=json, **kwargs)

    def put(self, url, data=None, **kwargs):
        r"""Sends a PUT request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        return self.request("PUT", url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        r"""Sends a PATCH request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        return self.request("PATCH", url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        r"""Sends a DELETE request. Returns :class:`Response` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: requests.Response
        """

        return self.request("DELETE", url, **kwargs)

    def send(self, request, **kwargs):
        """Send a given PreparedRequest.

        :rtype: requests.Response
        """
        # Set defaults that the hooks can utilize to ensure they always have
        # the correct parameters to reproduce the previous request.
        kwargs.setdefault("stream", self.stream)
        kwargs.setdefault("verify", self.verify)
        kwargs.setdefault("cert", self.cert)
        if "proxies" not in kwargs:
            kwargs["proxies"] = resolve_proxies(request, self.proxies, self.trust_env)

        # It's possible that users might accidentally send a Request object.
        # Guard against that specific failure case.
        if isinstance(request, Request):
            raise ValueError("You can only send PreparedRequests.")

        # Set up variables needed for resolve_redirects and dispatching of hooks
        allow_redirects = kwargs.pop("allow_redirects", True)
        stream = kwargs.get("stream")
        hooks = request.hooks

        # Get the appropriate adapter to use
        adapter = self.get_adapter(url=request.url)

        # Start time (approximately) of the request
        start = preferred_clock()

        # Send the request
        r = adapter.send(request, **kwargs)

        # Total elapsed time of the request (approximately)
        elapsed = preferred_clock() - start
        r.elapsed = timedelta(seconds=elapsed)

        # Response manipulation hooks
        r = dispatch_hook("response", hooks, r, **kwargs)

        # Persist cookies
        if r.history:
            # If the hooks create history then we want those cookies too
            for resp in r.history:
                extract_cookies_to_jar(self.cookies, resp.request, resp.raw)

        extract_cookies_to_jar(self.cookies, request, r.raw)

        # Resolve redirects if allowed.
        if allow_redirects:
            # Redirect resolving generator.
            gen = self.resolve_redirects(r, request, **kwargs)
            history = [resp for resp in gen]
        else:
            history = []

        # Shuffle things around if there's history.
        if history:
            # Insert the first (original) request at the start
            history.insert(0, r)
            # Get the last request made
            r = history.pop()
            r.history = history

        # If redirects aren't being followed, store the response on the Request for Response.next().
        if not allow_redirects:
            try:
                r._next = next(
                    self.resolve_redirects(r, request, yield_requests=True, **kwargs)
                )
            except StopIteration:
                pass

        if not stream:
            r.content

        return r

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        """
        Check the environment and merge it with some settings.

        :rtype: dict
        """
        # Gather clues from the surrounding environment.
        if self.trust_env:
            # Set environment's proxies.
            no_proxy = proxies.get("no_proxy") if proxies is not None else None
            env_proxies = get_environ_proxies(url, no_proxy=no_proxy)
            for k, v in env_proxies.items():
                proxies.setdefault(k, v)

            # Look for requests environment configuration
            # and be compatible with cURL.
            if verify is True or verify is None:
                verify = (
                    os.environ.get("REQUESTS_CA_BUNDLE")
                    or os.environ.get("CURL_CA_BUNDLE")
                    or verify
                )

        # Merge all the kwargs.
        proxies = merge_setting(proxies, self.proxies)
        stream = merge_setting(stream, self.stream)
        verify = merge_setting(verify, self.verify)
        cert = merge_setting(cert, self.cert)

        return {"proxies": proxies, "stream": stream, "verify": verify, "cert": cert}

    def get_adapter(self, url):
        """
        Returns the appropriate connection adapter for the given URL.

        :rtype: requests.adapters.BaseAdapter
        """
        for prefix, adapter in self.adapters.items():
            if url.lower().startswith(prefix.lower()):
                return adapter

        # Nothing matches :-/
        raise InvalidSchema(f"No connection adapters were found for {url!r}")

    def close(self):
        """Closes all adapters and as such the session"""
        for v in self.adapters.values():
            v.close()

    def mount(self, prefix, adapter):
        """Registers a connection adapter to a prefix.

        Adapters are sorted in descending order by prefix length.
        """
        self.adapters[prefix] = adapter
        keys_to_move = [k for k in self.adapters if len(k) < len(prefix)]

        for key in keys_to_move:
            self.adapters[key] = self.adapters.pop(key)

    def __getstate__(self):
        state = {attr: getattr(self, attr, None) for attr in self.__attrs__}
        return state

    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)
```

# Python Standard Class Methods (Dunder Methods / Magic Methods)

Python has dozens of special methods that define how classes behave. Here's a comprehensive guide organized by category.

## Object Initialization & Representation

### `__init__` and `__new__`
```python
class Person:
    def __new__(cls, name):
        # Called before __init__ to create the instance
        print(f"Creating instance for {name}")
        return super().__new__(cls)
    
    def __init__(self, name):
        # Initialize the instance
        self.name = name
        print(f"Initializing {name}")
```

### `__repr__` and `__str__`
```python
class Book:
    def __init__(self, title, author):
        self.title = title
        self.author = author
    
    def __repr__(self):
        # For developers - unambiguous representation
        return f"Book('{self.title}', '{self.author}')"
    
    def __str__(self):
        # For users - readable string
        return f"{self.title} by {self.author}"

book = Book("1984", "Orwell")
print(repr(book))  # Book('1984', 'Orwell')
print(str(book))   # 1984 by Orwell
print(f"{book}")   # 1984 by Orwell (uses __str__)
```

### `__format__`
```python
class Money:
    def __init__(self, amount):
        self.amount = amount
    
    def __format__(self, format_spec):
        if format_spec == 'usd':
            return f"${self.amount:.2f}"
        elif format_spec == 'eur':
            return f"€{self.amount:.2f}"
        return str(self.amount)

m = Money(19.5)
print(f"{m:usd}")  # $19.50
print(f"{m:eur}")  # €19.50
```

### `__hash__` and `__eq__`
```python
class Coordinate:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __eq__(self, other):
        return self.x == other.x and self.y == other.y
    
    def __hash__(self):
        return hash((self.x, self.y))

# Now can use in sets/dicts
coords = {Coordinate(0, 0), Coordinate(1, 1)}
```

## Comparison Operators

```python
class Version:
    def __init__(self, major, minor):
        self.major = major
        self.minor = minor
    
    def __eq__(self, other):
        return self.major == other.major and self.minor == other.minor
    
    def __lt__(self, other):
        return (self.major, self.minor) < (other.major, other.minor)
    
    def __le__(self, other):
        return self < other or self == other
    
    def __gt__(self, other):
        return not self <= other
    
    def __ge__(self, other):
        return not self < other
    
    def __ne__(self, other):
        return not self == other

v1 = Version(1, 0)
v2 = Version(1, 5)
print(v1 < v2)   # True
print(v1 == v2)  # False
```

## Arithmetic Operators

```python
class Vector:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __add__(self, other):
        return Vector(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other):
        return Vector(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar):
        return Vector(self.x * scalar, self.y * scalar)
    
    def __rmul__(self, scalar):
        # Right multiplication (scalar * vector)
        return self.__mul__(scalar)
    
    def __truediv__(self, scalar):
        return Vector(self.x / scalar, self.y / scalar)
    
    def __floordiv__(self, scalar):
        return Vector(self.x // scalar, self.y // scalar)
    
    def __mod__(self, scalar):
        return Vector(self.x % scalar, self.y % scalar)
    
    def __pow__(self, power):
        return Vector(self.x ** power, self.y ** power)
    
    def __repr__(self):
        return f"Vector({self.x}, {self.y})"

v1 = Vector(3, 4)
v2 = Vector(1, 2)
print(v1 + v2)      # Vector(4, 6)
print(v1 - v2)      # Vector(2, 2)
print(v1 * 2)       # Vector(6, 8)
print(2 * v1)       # Vector(6, 8) - uses __rmul__
```

## In-Place Assignment Operators

```python
class Counter:
    def __init__(self, value=0):
        self.value = value
    
    def __iadd__(self, other):
        self.value += other
        return self
    
    def __isub__(self, other):
        self.value -= other
        return self
    
    def __imul__(self, other):
        self.value *= other
        return self
    
    def __repr__(self):
        return f"Counter({self.value})"

c = Counter(5)
c += 3
print(c)  # Counter(8)
```

## Container/Sequence Methods

```python
class Playlist:
    def __init__(self, songs=None):
        self.songs = songs or []
    
    def __len__(self):
        return len(self.songs)
    
    def __getitem__(self, index):
        return self.songs[index]
    
    def __setitem__(self, index, value):
        self.songs[index] = value
    
    def __delitem__(self, index):
        del self.songs[index]
    
    def __contains__(self, item):
        return item in self.songs
    
    def __iter__(self):
        return iter(self.songs)
    
    def __reversed__(self):
        return reversed(self.songs)
    
    def __add__(self, other):
        return Playlist(self.songs + other.songs)
    
    def __mul__(self, times):
        return Playlist(self.songs * times)

playlist = Playlist(["Song A", "Song B", "Song C"])
print(len(playlist))           # 3
print(playlist[0])             # Song A
print("Song B" in playlist)    # True
for song in playlist:
    print(song)
```

## Attribute Access

```python
class DynamicObject:
    def __init__(self):
        self.data = {}
    
    def __getattr__(self, name):
        # Called when attribute not found normally
        print(f"Getting {name}")
        return self.data.get(name, "Not found")
    
    def __setattr__(self, name, value):
        # Called for all attribute assignments
        if name == 'data':
            super().__setattr__(name, value)
        else:
            print(f"Setting {name} = {value}")
            self.data[name] = value
    
    def __delattr__(self, name):
        # Called when deleting attributes
        print(f"Deleting {name}")
        if name in self.data:
            del self.data[name]
    
    def __getattribute__(self, name):
        # Called for ALL attribute access (use with caution!)
        print(f"Accessing {name}")
        return super().__getattribute__(name)

obj = DynamicObject()
obj.foo = "bar"   # Setting foo = bar
print(obj.foo)    # Gets bar
```

## Callable Objects

```python
class Multiplier:
    def __init__(self, factor):
        self.factor = factor
    
    def __call__(self, x):
        return x * self.factor

times3 = Multiplier(3)
print(times3(5))   # 15
print(times3(10))  # 30

# Check if callable
print(callable(times3))  # True
```

## Type Conversion

```python
class Temperature:
    def __init__(self, kelvin):
        self.kelvin = kelvin
    
    def __int__(self):
        return int(self.kelvin)
    
    def __float__(self):
        return float(self.kelvin)
    
    def __bool__(self):
        # True if above absolute zero
        return self.kelvin > 0
    
    def __complex__(self):
        return complex(self.kelvin)
    
    def __index__(self):
        # For use as list index
        return int(self.kelvin)

temp = Temperature(298.15)
print(int(temp))      # 298
print(float(temp))    # 298.15
print(bool(temp))     # True

lst = [10, 20, 30]
print(lst[temp])      # 30 (uses __index__)
```

## Context Managers

```python
class Timer:
    def __init__(self, name):
        self.name = name
    
    def __enter__(self):
        print(f"Starting {self.name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Ending {self.name}")
        return False  # Don't suppress exceptions

with Timer("Operation"):
    print("Doing work")
```

## Descriptor Protocol

```python
class Validator:
    def __init__(self, name):
        self.name = name
    
    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return obj.__dict__.get(self.name, None)
    
    def __set__(self, obj, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"{self.name} must be non-negative")
        obj.__dict__[self.name] = value
    
    def __delete__(self, obj):
        del obj.__dict__[self.name]

class Product:
    price = Validator('price')
    quantity = Validator('quantity')
    
    def __init__(self, price, quantity):
        self.price = price
        self.quantity = quantity

p = Product(10, 5)
p.price = -5  # Raises ValueError
```

## Async Methods

```python
class AsyncResource:
    async def __aenter__(self):
        print("Acquiring async resource")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print("Releasing async resource")
        return False
    
    def __aiter__(self):
        self.count = 0
        return self
    
    async def __anext__(self):
        if self.count < 3:
            self.count += 1
            return self.count
        raise StopAsyncIteration

# Usage:
# async with AsyncResource() as r:
#     async for value in r:
#         print(value)
```

## Other Useful Methods

```python
class CustomClass:
    def __sizeof__(self):
        # Return size in bytes
        return object.__sizeof__(self) + 100
    
    def __reduce__(self):
        # For pickling
        return (CustomClass, ())
    
    def __reduce_ex__(self, protocol):
        # Extended pickling
        return self.__reduce__()
    
    def __subclasscheck__(cls, subclass):
        # Check if subclass
        return False
    
    def __instancecheck__(cls, instance):
        # Check if instance
        return False

obj = CustomClass()
print(obj.__sizeof__())  # Size in bytes
```

## Slots
```python
class Point:
    __slots__ = ('x', 'y')
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __repr__(self):
        return f"Point({self.x}, {self.y})"

# Create millions of points efficiently
points = [Point(i, i*2) for i in range(1000000)]
```