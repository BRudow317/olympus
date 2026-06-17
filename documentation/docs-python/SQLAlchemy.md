````md
# SQLAlchemy Quick Reference
## Example: `users`

This is a minimal quick-start for using **SQLAlchemy 2.x** with a `users` table from your Lambdalith-style backend.

---

## 1. Install

```bash
pip install sqlalchemy psycopg[binary]
````

If you want async later:

```bash
pip install sqlalchemy "psycopg[binary]" asyncpg
```

---

## 2. Basic project shape

```text
app/
  database/
    base.py
    session.py
    models/
      user.py
  repositories/
    user_repository.py
```

---

## 3. Base model

**`app/database/base.py`**

```python
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
```

---

## 4. DB session setup

**`app/database/session.py`**

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql+psycopg://username:password@host:5432/database"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False
)
```

### Usage in Lambda/service code

```python
from app.database.session import SessionLocal

with SessionLocal() as db:
    ...
```

---

## 5. User model

**`app/database/models/user.py`**

```python
from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base


class User(Base):
    __tablename__ = "users"

    uuid: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4
    )
    user_id: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True
    )
    display_name: Mapped[str] = mapped_column(
        String(150),
        nullable=False
    )
    first_name: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )
    last_name: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True
    )
    primary_email: Mapped[str | None] = mapped_column(
        String(254),
        nullable=True,
        index=True
    )
    username: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        unique=True,
        index=True
    )
    is_platform_admin: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    profile_json: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    created_by: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )
    updated_by: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
```

---

## 6. Create tables

Temporary dev-only example:

```python
from app.database.base import Base
from app.database.session import engine
from app.database.models.user import User

Base.metadata.create_all(bind=engine)
```

For real projects, use **Alembic** migrations later.

---

## 7. Insert a user

```python
from app.database.models.user import User
from app.database.session import SessionLocal

with SessionLocal() as db:
    user = User(
        user_id="user_blaine_001",
        display_name="Blaine Rudow",
        first_name="Blaine",
        last_name="Rudow",
        primary_email="blainerudow@gmail.com",
        username="brudow317",
        is_platform_admin=False,
        is_active=True,
        profile_json={"source": "seed"},
        created_by="seed",
        updated_by="seed"
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    print(user.uuid)
    print(user.user_id)
```

---

## 8. Query a user

### By `user_id`

```python
from sqlalchemy import select

from app.database.models.user import User
from app.database.session import SessionLocal

with SessionLocal() as db:
    stmt = select(User).where(User.user_id == "user_blaine_001")
    user = db.scalar(stmt)

    print(user)
```

### By email

```python
stmt = select(User).where(User.primary_email == "blainerudow@gmail.com")
user = db.scalar(stmt)
```

### Get all active users

```python
stmt = select(User).where(User.is_active.is_(True)).order_by(User.created_at.desc())
users = db.scalars(stmt).all()
```

---

## 9. Update a user

```python
from sqlalchemy import select

from app.database.models.user import User
from app.database.session import SessionLocal

with SessionLocal() as db:
    stmt = select(User).where(User.user_id == "user_blaine_001")
    user = db.scalar(stmt)

    if user:
        user.display_name = "Blaine R."
        user.updated_by = "admin_user_001"
        db.commit()
        db.refresh(user)

        print(user.display_name)
```

---

## 10. Delete a user

```python
from sqlalchemy import select

from app.database.models.user import User
from app.database.session import SessionLocal

with SessionLocal() as db:
    stmt = select(User).where(User.user_id == "user_blaine_001")
    user = db.scalar(stmt)

    if user:
        db.delete(user)
        db.commit()
```

---

## 11. Repository example

**`app/repositories/user_repository.py`**

```python
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database.models.user import User


class UserRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_user_id(self, user_id: str) -> User | None:
        stmt = select(User).where(User.user_id == user_id)
        return self.db.scalar(stmt)

    def get_by_email(self, email: str) -> User | None:
        stmt = select(User).where(User.primary_email == email)
        return self.db.scalar(stmt)

    def create(
        self,
        *,
        user_id: str,
        display_name: str,
        created_by: str,
        updated_by: str,
        primary_email: str | None = None,
        username: str | None = None
    ) -> User:
        user = User(
            user_id=user_id,
            display_name=display_name,
            primary_email=primary_email,
            username=username,
            is_platform_admin=False,
            is_active=True,
            created_by=created_by,
            updated_by=updated_by
        )
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return user
```

---

## 12. Using the repository

```python
from app.database.session import SessionLocal
from app.repositories.user_repository import UserRepository

with SessionLocal() as db:
    repo = UserRepository(db)

    user = repo.create(
        user_id="user_demo_001",
        display_name="Demo User",
        primary_email="demo@example.com",
        username="demo_user",
        created_by="seed",
        updated_by="seed"
    )

    found = repo.get_by_user_id("user_demo_001")
    print(found.display_name if found else None)
```

---

## 13. Lambda handler pattern

```python
from app.database.session import SessionLocal
from app.repositories.user_repository import UserRepository


def handler(event, context):
    user_id = event["pathParameters"]["user_id"]

    with SessionLocal() as db:
        repo = UserRepository(db)
        user = repo.get_by_user_id(user_id)

        if not user:
            return {
                "statusCode": 404,
                "body": "User not found"
            }

        return {
            "statusCode": 200,
            "body": {
                "user_id": user.user_id,
                "display_name": user.display_name,
                "primary_email": user.primary_email
            }
        }
```

---

## 14. What to remember

* `Base` defines your ORM base class
* `engine` connects to Postgres
* `SessionLocal()` gives you a DB session
* `mapped_column()` defines columns
* `select(User)` is the normal query style in SQLAlchemy 2.x
* `db.scalar(stmt)` gets one row
* `db.scalars(stmt).all()` gets many rows
* `db.add()` + `db.commit()` persists changes
* `db.refresh()` reloads DB-generated values

---

## 15. Minimal mental mapping from Hibernate

* `@Entity` -> SQLAlchemy model class
* `@Table` -> `__tablename__`
* `@Column` -> `mapped_column(...)`
* `EntityManager` / Session -> SQLAlchemy `Session`
* JPQL / Criteria -> `select(...)`
* persist -> `db.add(...)`
* flush/commit -> `db.commit()`

---
