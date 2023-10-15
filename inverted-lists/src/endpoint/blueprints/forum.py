import base64
from flask import Blueprint, render_template, redirect, request, url_for

from src.dataset import Dataset
from src.endpoint.extensions import db
from src.endpoint.models import Document
from src.dss import System, RecordStorage
from src.private import club

from tailor import TorrentsTailor, BooksTailor, CoursesTailor
from itertools import chain
from sqlalchemy import select


forum = Blueprint('forum', __name__)


@forum.route('/', methods=['GET'])
def listing_page():
    return render_template('forum/index.html', topics=latest_forum_entries_gen())

@forum.route('/<id>', methods=['GET'])
def details(id):
    dss = System('barley', data_root='./_tmp/dss')
    forum_topics = dss.get_record_storage('forum_topics')
    topic_id_index = dss.open_id_index('forum_topics')

    record_id = topic_id_index.find(int(id))
    topic_bytes = forum_topics.find_by_id(record_id)
    topic = club.PostEntry.deserialize(topic_bytes)
    dss._cleanup()

    return render_template('forum/details.html', entry=topic)


def latest_forum_entries_gen():
    dss = System('barley', data_root='./_tmp/dss')
    forum_topics = dss.get_record_storage('forum_topics')
    topic_id_index = dss.open_id_index('forum_topics')
    sorted_by_ts = []
    for topic_id, record_id in topic_id_index.iterate():
        topic_bytes = forum_topics.find_by_id(record_id)
        topic = club.PostEntry.deserialize(topic_bytes)
        sorted_by_ts.append((topic.last_msg_timestamp, record_id))
    sorted_by_ts.sort(key=lambda u: u[0], reverse=True)
    
    # --- Listing entries with descending timestamps
    for ts, record_id in sorted_by_ts:
        topic_bytes = forum_topics.find_by_id(record_id)
        topic = club.PostEntry.deserialize(topic_bytes)
        yield topic

    dss._cleanup()