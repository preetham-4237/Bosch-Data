"""Pydantic models matching schema_v4.json for Bosch product catalog."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SourceSystem(str, Enum):
    PIM = "PIM"
    WEB = "WEB"
    MANUAL = "MANUAL"
    MERGED = "MERGED"


class Source(BaseModel):
    system: SourceSystem
    sourceProductId: Optional[str] = None
    ingestedAt: Optional[str] = None
    raw: Optional[dict] = None


class Ids(BaseModel):
    productNumber: str
    pimProductId: Optional[str] = None
    manufacturer: str = "BOSCH"
    bareToolNumber: Optional[str] = None
    skus: Optional[list[str]] = None
    gtins: Optional[list[str]] = None


class Name(BaseModel):
    display: str
    alternativeNames: Optional[list[str]] = None
    localized: Optional[dict[str, str]] = None


class LifecycleStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DISCONTINUED = "DISCONTINUED"
    UNKNOWN = "UNKNOWN"


class Lifecycle(BaseModel):
    status: Optional[LifecycleStatus] = None
    releasedAt: Optional[str] = None
    discontinuedAt: Optional[str] = None


class Classification(BaseModel):
    productTypeId: Optional[str] = None
    categoryIds: Optional[list[str]] = None
    categoryPath: Optional[list[str]] = None
    segment: Optional[str] = None
    family: Optional[str] = None
    platform: Optional[str] = None


class ArticleType(str, Enum):
    BARE_TOOL = "BARE_TOOL"
    KIT = "KIT"
    BATTERY = "BATTERY"
    CHARGER = "CHARGER"
    ACCESSORY = "ACCESSORY"
    UNKNOWN = "UNKNOWN"


class ArticleFlags(BaseModel):
    heavyDuty: Optional[bool] = None


class RawAttribute(BaseModel):
    key: str
    textValue: Optional[str] = None
    valueList: Optional[list[str]] = None


class Article(BaseModel):
    articleType: ArticleType
    sku: Optional[str] = None
    gtin: Optional[str] = None
    flags: Optional[ArticleFlags] = None
    skuContents: Optional[list[str]] = None
    skuDescription: Optional[str] = None
    rawAttributes: Optional[list[RawAttribute]] = None


class Dimensions(BaseModel):
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None


class TechSpecs(BaseModel):
    powerSource: Optional[str] = None
    voltageV: Optional[float] = None
    batteryAh: Optional[float] = None
    rpm: Optional[list[float]] = None
    torqueNm: Optional[float] = None
    impactRateBpm: Optional[float] = None
    weightKg: Optional[float] = None
    dimensionsMm: Optional[Dimensions] = None
    noiseDb: Optional[float] = None
    vibration: Optional[float] = None
    materials: Optional[list[str]] = None


class Compatibility(BaseModel):
    batteryPlatform: Optional[str] = None
    supportedBatteries: Optional[list[str]] = None
    supportedChargers: Optional[list[str]] = None
    systemCompatibility: Optional[list[str]] = None


class DocumentType(str, Enum):
    MANUAL = "MANUAL"
    DATASHEET = "DATASHEET"
    EXPLODED_VIEW = "EXPLODED_VIEW"
    CERTIFICATE = "CERTIFICATE"
    OTHER = "OTHER"


class Document(BaseModel):
    type: DocumentType
    ref: str


class Media(BaseModel):
    primaryImage: Optional[str] = None
    images: Optional[list[str]] = None
    videos: Optional[list[str]] = None
    documents: Optional[list[Document]] = None


class Availability(str, Enum):
    IN_STOCK = "IN_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    UNKNOWN = "UNKNOWN"


class Commercial(BaseModel):
    msrp: Optional[float] = None
    currency: Optional[str] = None
    availability: Optional[Availability] = None


class ML(BaseModel):
    features: Optional[dict] = None
    embedding: Optional[list[float]] = None


class Product(BaseModel):
    source: Source
    ids: Ids
    name: Name
    updatedAt: str
    lifecycle: Optional[Lifecycle] = None
    classification: Optional[Classification] = None
    articles: list[Article] = Field(min_length=1)
    techSpecs: Optional[TechSpecs] = None
    compatibility: Optional[Compatibility] = None
    targetUsers: Optional[list[str]] = None
    features: Optional[list[str]] = None
    media: Optional[Media] = None
    commercial: Optional[Commercial] = None
    ml: Optional[ML] = None
