# app/sync/convert.py
from datetime import date, datetime as dt
from typing import Any, Dict

from decimal import Decimal, ROUND_DOWN
from bson.decimal128 import Decimal128
from bson import ObjectId


class Converter:
    def __init__(self, pk_field: str, use_pk_as_mongo_id: bool, dec_scale: int = 18):
        self.pk_field = pk_field
        self.use_pk_as_mongo_id = use_pk_as_mongo_id
        self._pk_lower = pk_field.lower()
        self.DEC_SCALE = dec_scale
        self.DEC_Q = Decimal("1").scaleb(-self.DEC_SCALE)

    def convert_value(self, obj: Any):
        if isinstance(obj, Decimal):
            dq = obj.quantize(self.DEC_Q, rounding=ROUND_DOWN)
            return Decimal128(dq)
        if isinstance(obj, dt):
            return obj
        if isinstance(obj, date):
            return dt(obj.year, obj.month, obj.day)
        if isinstance(obj, dict):
            return {k: self.convert_value(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.convert_value(v) for v in obj]
        return obj

    def row_to_base_doc(self, row: Dict[str, Any]) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                dq = v.quantize(self.DEC_Q, rounding=ROUND_DOWN)
                doc[k] = Decimal128(dq)
                doc[f"{k}_str"] = format(dq, "f")
            else:
                doc[k] = self.convert_value(v)

        if self.use_pk_as_mongo_id:
            for kk, vv in row.items():
                if isinstance(kk, str) and kk.lower() == self._pk_lower:
                    doc["_id"] = self.convert_value(vv)
                    break
        return doc

    def row_to_version_doc(self, row: Dict[str, Any], pk_val: Any, base_id: Any) -> Dict[str, Any]:
        doc: Dict[str, Any] = {}
        for k, v in row.items():
            doc[k] = self.convert_value(v)

        doc["_id"] = ObjectId()
        doc[self.pk_field] = pk_val
        doc["_base_id"] = base_id
        doc["_is_version"] = True
        doc["_op"] = "update"
        doc["_ts"] = dt.utcnow()
        return doc
