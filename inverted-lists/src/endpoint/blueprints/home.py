import base64
from flask import Blueprint, render_template, redirect, request, url_for

from src.dataset import Dataset
from tailor import TorrentsTailor, BooksTailor, CoursesTailor
from itertools import chain


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

    return render_template('home/details.html', entry=entry)


@home.route('/about')
def about():
    return render_template('home/about.html')
