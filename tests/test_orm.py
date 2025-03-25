import asyncio

from mongomock_motor import AsyncMongoMockClient
import pytest

from morm import Database, Model, Index, DoesNotExist, AlreadyExists, DuplicateKeyError, ASC


@pytest.fixture()
def mock_mongoclient(mocker):
    mocker.patch('motor.motor_asyncio.AsyncIOMotorClient', AsyncMongoMockClient)


def test_database_init(mocker):
    mock_client_called = mocker.Mock()
    mock_client = mocker.patch('motor.motor_asyncio.AsyncIOMotorClient', return_value=mock_client_called)

    Database("test", name="fd", hello="World!")

    mock_client.assert_called_once_with("test", hello="World!")
    mock_client_called.get_database.assert_called_once_with("fd")


def test_database_decorator(mocker, mock_mongoclient):
    mock_db = mocker.Mock()
    mocker.patch('motor.motor_asyncio.AsyncIOMotorClient.get_database', return_value=mock_db)

    db = Database("test", name="fd", hello="World!")

    @db
    class Test(Model):
        pass

    assert hasattr(Test, "_db")
    assert getattr(Test, "_db", None) == mock_db


async def setup_database(db: Database):
    @db
    class TestModel(Model):
        name: str
        num: int

    await TestModel(name="Test", num=1).create()
    await TestModel(name="Hello World", num=2).create()
    await TestModel(name="John Doe", num=2).create()

    return TestModel


def test_model_collection_name_set():
    class TestModel(Model):
        class Meta:
            COLLECTION_NAME = "helloworld"

    assert TestModel.collection_name() == "helloworld"


def test_model_collection_name_unset():
    class TestModel(Model):
        pass

    assert TestModel.collection_name() == "testmodel"


def test_model_with_db(mocker, mock_mongoclient):
    mock_db = mocker.Mock()

    db = Database(name="test")
    db.db = mock_db

    @db
    class TestModel(Model):
        pass

    assert TestModel.db() == mock_db


def test_model_without_db():
    class TestModel(Model):
        pass

    with pytest.raises(RuntimeError):
        TestModel.db()


def test_model_collection(mocker, mock_mongoclient):
    mock_collection = mocker.Mock()
    mock_get_collection = mocker.Mock(return_value=mock_collection)

    db = Database(name="test")
    db.db.get_collection = mock_get_collection

    @db
    class TestModel(Model):
        pass

    assert TestModel.collection() == mock_collection

    mock_get_collection.assert_called_once_with("testmodel")


@pytest.mark.asyncio
async def test_model_get(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    await TestModel(name="Test", num=1).create()
    await TestModel(name="Hello World", num=2).create()
    await TestModel(name="John Doe", num=2).create()

    obj1 = await TestModel.get(num=1)
    obj2 = await TestModel.get(name="Hello World", num=2)
    obj3 = await TestModel.get(name="John Doe")

    assert obj1.name == "Test" and obj1.num == 1
    assert obj2.name == "Hello World" and obj2.num == 2
    assert obj3.name == "John Doe" and obj3.num == 2

    obj_new = await TestModel(name="MORM", num=3).create()
    obj4 = await TestModel.get(id=obj_new.id)

    assert obj4.name == "MORM" and obj4.num == 3


@pytest.mark.asyncio
async def test_model_get_doesnt_exist(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        pass

    with pytest.raises(DoesNotExist):
        await TestModel.get(id="e" * 24)


@pytest.mark.asyncio
async def test_model_get_many(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    await TestModel(name="Test", num=1).create()
    obj1 = await TestModel(name="Hello World", num=2).create()
    obj2 = await TestModel(name="John Doe", num=2).create()

    assert await TestModel.get_many(num=2) == [obj1, obj2]


@pytest.mark.asyncio
async def test_model_count(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    await TestModel(name="Test", num=1).create()
    await TestModel(name="Hello World", num=2).create()
    await TestModel(name="John Doe", num=2).create()

    assert await TestModel.count(num=2) == 2


@pytest.mark.asyncio
async def test_model_create_already_exists(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj = await TestModel(name="Test", num=1).create()

    with pytest.raises(AlreadyExists):
        await obj.create()


@pytest.mark.asyncio
async def test_model_save_not_exists(mocker, mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    mock_create = mocker.AsyncMock()
    TestModel.create = mock_create

    await TestModel(name="Test", num=1).save()

    mock_create.assert_awaited_once()


@pytest.mark.asyncio
async def test_model_save_already_exists(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj = await TestModel(name="Test", num=1).create()

    obj.name = "Hello World"
    await obj.save()

    obj_get = await TestModel.get(name="Hello World")
    assert obj_get.id == obj.id


@pytest.mark.asyncio
async def test_model_update(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj = await TestModel(name="Test", num=1).create()

    await obj.update({"$set": {"name": "Hello World"}})

    obj_get = await TestModel.get(name="Hello World")
    assert obj_get.id == obj.id


@pytest.mark.asyncio
async def test_model_update_many(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj1 = await TestModel(name="Hello", num=1).create()
    obj2 = await TestModel(name="World", num=1).create()
    await TestModel(name="John Doe", num=2).create()

    await TestModel.update_many({"num": 1}, {"$set": {"name": "Hello World"}})

    obj_get = await TestModel.get_many(name="Hello World")
    assert obj_get[0].id == obj1.id
    assert obj_get[1].id == obj2.id


@pytest.mark.asyncio
async def test_model_delete(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj = await TestModel(name="Test", num=1).create()

    await obj.delete()

    with pytest.raises(DoesNotExist):
        await TestModel.get(num=1)


@pytest.mark.asyncio
async def test_model_delete_many(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    await TestModel(name="Hello", num=1).create()
    await TestModel(name="World", num=1).create()
    obj = await TestModel(name="John Doe", num=2).create()

    await TestModel.delete_many(num=1)

    assert await TestModel.get_many() == [obj]


@pytest.mark.asyncio
async def test_model_get_or_create_get(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj = await TestModel(name="Test", num=1).create()

    obj_get, created = await TestModel.get_or_create(dict(name="Test"), dict(num=1))

    assert created is False
    assert obj == obj_get


@pytest.mark.asyncio
async def test_model_get_or_create_create(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        name: str
        num: int

    obj, created = await TestModel.get_or_create(dict(name="Test"), dict(num=1))

    assert created is True
    assert obj.name == "Test" and obj.num == 1


def test_model_indexes(mock_mongoclient):
    db = Database(name="test")

    @db
    class TestModel(Model):
        class Meta:
            INDEXES = [Index(("name", ASC), unique=True)]

        name: str

    with pytest.raises(DuplicateKeyError):
        asyncio.run(TestModel(name="Test").create())
        asyncio.run(TestModel(name="Test").create())

    assert asyncio.run(TestModel.collection().index_information()) == {'_id_': {'key': [('_id', 1)], 'v': 2}, 'name_1': {'key': (('name', 1),), 'unique': True, 'v': 2}}
