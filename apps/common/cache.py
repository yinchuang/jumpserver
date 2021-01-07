import json
from django.core.cache import cache

from common.utils import lazyproperty
from common.utils import get_logger

logger = get_logger(__file__)


class CacheFieldBase:
    field_type = str

    def __init__(self, timeout=None, compute_func_name=None):
        self.timeout = timeout
        self.compute_func_name = compute_func_name


class StringField(CacheFieldBase):
    field_type = str


class IntegerField(CacheFieldBase):
    field_type = int


class CacheBase(type):
    def __new__(cls, name, bases, attrs: dict):
        to_update = {}
        field_desc_mapper = {}

        for k, v in attrs.items():
            if isinstance(v, CacheFieldBase):
                desc = CacheValueDesc(k, v)
                to_update[k] = desc
                field_desc_mapper[k] = desc

        attrs.update(to_update)
        attrs['field_desc_mapper'] = field_desc_mapper
        return type.__new__(cls, name, bases, attrs)


class Cache(metaclass=CacheBase):
    field_desc_mapper: dict

    def __init__(self):
        self._data = None

    @lazyproperty
    def key_suffix(self):
        return self.get_key_suffix()

    @property
    def key_prefix(self):
        clz = self.__class__
        return f'cache.{clz.__module__}.{clz.__name__}'

    @property
    def key(self):
        return f'{self.key_prefix}.{self.key_suffix}'

    @property
    def data(self):
        if self._data is None:
            data = self.get_data()
            if data is None:
                # 缓存中没有数据时，去数据库获取
                data = self._compute_data()
                self.set_data(data)
            self._data = data
        return self._data

    def get_data(self) -> dict:
        data = cache.get(self.key)
        logger.debug(f'CACHE: get {self.key} = {data}')
        if data is not None:
            data = json.loads(data)
        return data

    def set_data(self, data):
        to_json = json.dumps(data)
        logger.info(f'CACHE: set {self.key} = {to_json}')
        cache.set(self.key, to_json)

    def _compute_data(self, *fields):
        field_descs = []
        if not fields:
            field_descs = self.field_desc_mapper.values()
        else:
            for field in fields:
                assert field in self.field_desc_mapper, f'{field} is not a valid field'
                field_descs.append(self.field_desc_mapper[field])
        data = {
            field_desc.field_name: field_desc.compute_value(self)
            for field_desc in self.field_desc_mapper.values()
        }
        return data

    def refresh(self, *fields):
        data = self.get_data()
        if data is None:
            data = self._compute_data()
        else:
            refresh_data = self._compute_data(*fields)
            data.update(refresh_data)
        self.set_data(data)
        self._data = data

    def get_key_suffix(self):
        raise NotImplementedError

    def reload(self):
        self._data = None


class CacheValueDesc:
    def __init__(self, field_name, field_type: CacheFieldBase):
        self.field_name = field_name
        self.field_type = field_type
        self._data = None

    def __repr__(self):
        clz = self.__class__
        return f'<{clz.__name__} {self.field_name} {self.field_type}>'

    def __get__(self, instance: Cache, owner):
        if instance is None:
            return self
        value = instance.data[self.field_name]
        return value

    def compute_value(self, instance: Cache):
        compute_func_name = self.field_type.compute_func_name
        if not compute_func_name:
            compute_func_name = f'compute_{self.field_name}'
        compute_func = getattr(instance, compute_func_name, None)
        assert compute_func is not None, \
            f'Define `{compute_func_name}` method in {instance.__class__}'
        new_value = compute_func()
        new_value = self.field_type.field_type(new_value)
        logger.info(f'CACHE: compute {instance.key}.{self.field_name} = {new_value}')
        return new_value
