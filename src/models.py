from dataclasses import dataclass, field, asdict
from typing import Optional
import json


@dataclass
class Property:
    source: str
    source_id: str
    address: str
    county: str = "luzerne"
    state: str = "PA"
    parcel_id: Optional[str] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    owner_name: Optional[str] = None
    owner_address: Optional[str] = None
    listing_price: Optional[float] = None
    property_type: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    raw: dict = field(default_factory=dict)

    def to_db_row(self) -> dict:
        d = asdict(self)
        d["raw"] = json.dumps(d["raw"], default=str)
        return d
