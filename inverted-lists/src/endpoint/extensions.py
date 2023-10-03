from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from typing import Optional
import datetime


class Base(DeclarativeBase):
    pass


db = SQLAlchemy(model_class=Base)
