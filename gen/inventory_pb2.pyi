from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ReserveRequest(_message.Message):
    __slots__ = ("request_id", "sku", "quantity")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    sku: str
    quantity: int
    def __init__(self, request_id: _Optional[str] = ..., sku: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class ReserveResponse(_message.Message):
    __slots__ = ("ok", "remaining", "reason", "reservation_id")
    OK_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RESERVATION_ID_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    remaining: int
    reason: str
    reservation_id: str
    def __init__(self, ok: _Optional[bool] = ..., remaining: _Optional[int] = ..., reason: _Optional[str] = ..., reservation_id: _Optional[str] = ...) -> None: ...

class ReleaseRequest(_message.Message):
    __slots__ = ("request_id", "sku", "quantity")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    sku: str
    quantity: int
    def __init__(self, request_id: _Optional[str] = ..., sku: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...

class ReleaseResponse(_message.Message):
    __slots__ = ("ok", "remaining")
    OK_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    remaining: int
    def __init__(self, ok: _Optional[bool] = ..., remaining: _Optional[int] = ...) -> None: ...

class GetStockRequest(_message.Message):
    __slots__ = ("sku",)
    SKU_FIELD_NUMBER: _ClassVar[int]
    sku: str
    def __init__(self, sku: _Optional[str] = ...) -> None: ...

class Stock(_message.Message):
    __slots__ = ("sku", "available", "version")
    SKU_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    sku: str
    available: int
    version: int
    def __init__(self, sku: _Optional[str] = ..., available: _Optional[int] = ..., version: _Optional[int] = ...) -> None: ...
