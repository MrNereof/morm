from __future__ import annotations

import functools
import typing
from contextlib import asynccontextmanager

import bson
import gridfs
import pymongo
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, create_model
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from pymongo.asynchronous.database import AsyncCollection, AsyncDatabase

from morm.utils import recursive_diff


class ObjectIdAnnotation:
    @classmethod
    def validate_object_id(cls, v: typing.Any, handler) -> bson.ObjectId:
        if isinstance(v, bson.ObjectId):
            return v

        s = handler(v)
        if bson.ObjectId.is_valid(s):
            return bson.ObjectId(s)

        raise ValueError("Invalid ObjectId")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, _handler
    ) -> core_schema.CoreSchema:
        assert source_type is bson.ObjectId
        return core_schema.no_info_wrap_validator_function(
            cls.validate_object_id,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema, handler) -> JsonSchemaValue:
        return handler(core_schema.str_schema())


ObjectId = typing.Annotated[bson.ObjectId, ObjectIdAnnotation]


class DatabaseException(Exception):
    pass


class DoesNotExist(DatabaseException):
    def __init__(self):
        super().__init__("Object of model does not exist")


class AlreadyExists(DatabaseException):
    def __init__(self):
        super().__init__("Object of model already exists")


class Database:
    def __init__(self, *args, name: typing.Optional[str] = None, **kwargs):
        self.client = pymongo.AsyncMongoClient(*args, **kwargs)
        self.db = self.client.get_database(name)

        self._jobs = []
        self._grid_fs = None

    def __call__(self, cls: typing.Type[Model]):
        if not issubclass(cls, Model):
            raise TypeError("Provided class must be subclass of Model")

        cls._db = self.db
        for i in cls.indexes():
            self.register_job(i.create_index(cls))

        return cls

    async def setup(self):
        for coro in self._jobs:
            await coro

    def register_job(self, coro: typing.Coroutine):
        self._jobs.append(coro)

    @asynccontextmanager
    async def transaction(self):
        async with self.client.start_session() as s:
            async with await s.start_transaction():
                yield

    def atomic(self, func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with self.transaction():
                return await func(*args, **kwargs)

        return wrapper

    @property
    def grid_fs(self) -> gridfs.AsyncGridFS:
        if self._grid_fs is None:
            self._grid_fs = gridfs.AsyncGridFS(self.db)
        return self._grid_fs


class Index:
    def __init__(self, *indexes: str | tuple[str, typing.Any], **params):
        if len(indexes) == 1 and isinstance(indexes[0], str):
            self.indexes = indexes[0]
        else:
            self.indexes = list(indexes)
        self.params = params

    async def create_index(self, model: Model):
        await model.collection().create_index(self.indexes, **self.params)


class Model(BaseModel):
    class Meta:
        COLLECTION_NAME: str
        INDEXES: list[Index]

    _db: typing.ClassVar[AsyncDatabase]
    _collection: typing.ClassVar[AsyncCollection]

    _base_model: typing.ClassVar[typing.Type[BaseModel]]

    _state_snapshot: typing.Optional[dict[str, typing.Any]] = PrivateAttr(default=None)

    id: typing.Optional[ObjectId] = Field(alias="_id", default=None)

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._take_snapshot()

    def _make_dump(self):
        return self.model_dump(by_alias=True, exclude={"id"})

    def _take_snapshot(self):
        self._state_snapshot = self._make_dump()

    def _get_state_diff(self):
        state = self._make_dump()
        return recursive_diff(self._state_snapshot, state)

    @classmethod
    def collection_name(cls) -> str:
        return getattr(cls.Meta, "COLLECTION_NAME", None) or cls.__name__.lower()

    @classmethod
    def db(cls) -> AsyncDatabase:
        if hasattr(cls, "_db"):
            return cls._db

        raise RuntimeError("No Database connected!")

    @classmethod
    def collection(cls) -> AsyncCollection:
        if not hasattr(cls, "_collection"):
            cls._collection = cls.db().get_collection(cls.collection_name())

        return cls._collection

    @classmethod
    def indexes(cls):
        return getattr(cls.Meta, "INDEXES", [])

    @classmethod
    def as_base_cls(cls) -> typing.Type[BaseModel]:
        if not hasattr(cls, "_base_model"):
            fields = {
                name: (field.annotation, field.default if field.default is not None else ...)
                for name, field in cls.model_fields.items()
                if name != 'id'
            }

            base_cls = create_model(f"{cls.__name__}Base", **fields)
            base_cls._original_model = cls

            @classmethod
            def as_model_cls(clss):
                return cls

            def as_model(self):
                return cls(**self.model_dump())

            base_cls.as_model_cls = as_model_cls
            base_cls.as_model = as_model

            cls._base_model = base_cls
        return cls._base_model

    def as_base(self):
        base_cls = self.as_base_cls()
        return base_cls(**{
            k: v for k, v in self.model_dump().items()
            if k != "id"
        })

    def __hash__(self):
        return self.id.__hash__()

    @classmethod
    async def get(cls, **params) -> typing.Optional[typing.Self]:
        _id = params.pop("id", None)

        if _id is not None:
            params["_id"] = _id

        obj = await cls.collection().find_one(params)
        if not obj:
            raise DoesNotExist

        return cls(**obj)

    @classmethod
    async def get_many(cls, _filter=None, **params) -> list[typing.Self]:
        cursor = cls.collection().find(params)
        if _filter is not None:
            _filter(cursor)

        return [cls(**e) async for e in cursor]

    @classmethod
    async def count(cls, **params) -> int:
        return await cls.collection().count_documents(params)

    async def create(self) -> typing.Self:
        if self.id:
            raise AlreadyExists

        data = self._make_dump()
        new = await self.collection().insert_one(data)
        self.id = new.inserted_id

        self._take_snapshot()

        return self

    async def save(self) -> typing.Self:
        try:
            return await self.create()
        except AlreadyExists:
            return await self.push_update()

    async def push_update(self):
        if not self.id:
            raise DoesNotExist

        diff = self._get_state_diff()
        await self.collection().update_one({"_id": self.id}, {"$set": diff})

        self._take_snapshot()

        return self

    async def replace(self):
        if not self.id:
            raise DoesNotExist

        await self.collection().replace_one({"_id": self.id}, self._make_dump())

        self._take_snapshot()

        return self

    async def update(self, params) -> typing.Self:
        if not self.id:
            raise DoesNotExist

        await self.collection().update_one({"_id": self.id}, params)
        return await self.get(_id=self.id)

    @classmethod
    async def update_many(cls, params, update):
        await cls.collection().update_many(params, update)

    async def delete(self):
        if not self.id:
            raise DoesNotExist

        await self.collection().delete_one({"_id": self.id})
        self.id = None

    @classmethod
    async def delete_many(cls, **params):
        await cls.collection().delete_many(params)

    @classmethod
    async def get_or_create(cls, params, others) -> (typing.Self, bool):
        try:
            obj = await cls.get(**params)
            created = False
        except DoesNotExist:
            obj = await cls(**params, **others).create()
            created = True

        return obj, created
