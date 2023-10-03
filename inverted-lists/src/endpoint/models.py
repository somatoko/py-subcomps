from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from typing import Optional
import datetime

from src.endpoint.extensions import db


class Document(db.Model):
    __tablename__ = 'documents'

    id: Mapped[int] = mapped_column(primary_key=True)
    src: Mapped[str] = mapped_column(index=True, unique=True)
    name: Mapped[Optional[str]]
    notes: Mapped[Optional[str]]
    is_favourite: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        insert_default=func.now())
