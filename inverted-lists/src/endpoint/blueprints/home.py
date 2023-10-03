import base64
from flask import Blueprint, render_template, redirect, request, url_for

from src.dataset import Dataset
from src.endpoint.extensions import db
from src.endpoint.models import Document
from tailor import TorrentsTailor, BooksTailor, CoursesTailor
from itertools import chain
from sqlalchemy import select


home = Blueprint('home', __name__)


@home.route('/', methods=['GET'])
def home_page():
    args = request.args
    q = args.get('q', default=None)

    if q is None:
        return render_template('home/index.html')
    else:
        torrents = TorrentsTailor()
        torrent_gen = torrents.search(q)
        books = BooksTailor()
        books_gen = books.search(q)
        courses = CoursesTailor()
        courses_gen = courses.search(q)

        entries = chain(
            torrent_gen,
            courses_gen,
            books_gen,
        )

        return render_template('home/index_results.html', q=q, entries=entries)


@home.route('/entry/<src>', methods=['GET'])
def details(src):
    src_decoded = base64.urlsafe_b64decode(bytes(src, 'utf-8')).decode()
    data_src, id = src_decoded.split(':')
    subdir, name = data_src.split('/')

    ds = Dataset(name, None, subdir)
    entry = ds.find_by_id(int(id))

    def get_doc():
        return db.session.execute(
            db.select(Document).where(Document.src == src_decoded)).scalar()
        # db.select(Document).where(Document.src == src_decoded)).scalar_one()
        # select(Document)).first()

    db_doc = get_doc()
    if db_doc is None:
        db_doc = Document(
            src=src_decoded,
            name=entry['name'],
        )
        db.session.add(db_doc)
        db.session.commit()
        db_doc = get_doc()

    print('- doc 0', db_doc.__dict__)

    return render_template('home/details.html', entry=entry, db_doc=db_doc)


@home.route('/entry/<src>', methods=['POST'])
def details_update(src):
    src_decoded = base64.urlsafe_b64decode(bytes(src, 'utf-8')).decode()
    # doc = db.get_or_404(Document, id)
    doc = db.session.execute(
        db.select(Document).where(Document.src == src_decoded)).scalar_one()
    if doc is not None:
        notes = request.form.get('notes', None)
        is_fav = request.form.get('is_favourite', 'off')
        is_fav = True if is_fav == 'on' else False

        doc.notes = notes
        doc.is_favourite = is_fav
        db.session.commit()
    return redirect(url_for('home.details', src=src))


@home.route('/favourites')
def favourites():
    docs = db.session.execute(
        db.select(Document).where(Document.is_favourite == True)).scalars()
    return render_template('home/favourites.html', docs=docs)


@home.route('/recents')
def recents():
    return render_template('home/recents.html')


@home.route('/about')
def about():
    return render_template('home/about.html')
