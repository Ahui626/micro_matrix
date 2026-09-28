from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateOrderRequest(_message.Message):
    __slots__ = ("request_id", "user_id", "sku", "quantity")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    user_id: str
    sku: str
    quantity: int
    def __init__(self, request_id: _Optional[str] = ..., user_id: _Optional[str] = ..., sku: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class CreateOrderResponse(_message.Message):
    __slots__ = ("order", "deduplicated")
    ORDER_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    order: Order
    deduplicated: bool
    def __init__(self, order: _Optional[_Union[Order, _Mapping]] = ..., deduplicated: _Optional[bool] = ...) -> None: ...

class Order(_message.Message):
    __slots__ = ("id", "request_id", "user_id", "sku", "quantity", "status", "created_at_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    id: int
    request_id: str
    user_id: str
    sku: str
    quantity: int
    status: str
    created_at_ms: int
    def __init__(self, id: _Optional[int] = ..., request_id: _Optional[str] = ..., user_id: _Optional[str] = ..., sku: _Optional[str] = ..., quantity: _Optional[int] = ..., status: _Optional[str] = ..., created_at_ms: _Optional[int] = ...) -> None: ...

class GetOrderRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...
