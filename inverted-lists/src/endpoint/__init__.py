from flask import Flask, render_template
from random import randint
import time
import datetime

from src.endpoint.extensions import db
from src.endpoint.blueprints.home import home
from src.endpoint.blueprints.forum import forum

# To launch from shell: FLASK_APP=src.endpoint FLASK_DEBUG=on flask run


def create_app(environment_name='dev'):
    app = Flask(__name__)

    # custom config
    # app.config.from_object(configs[environment_name])

    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite+pysqlite:///data.sqlite3"
    db.init_app(app)

    with app.app_context():
        db.create_all()

    @app.errorhandler(500)
    def handle_error(exception):
        return render_template('500.html')  # pragma: no cover

    @app.template_filter()
    def format_datetime(value, format='medium'):
        if isinstance(value, datetime.datetime):
            return value.strftime("%d %b %Y at %H:%M")
        elif isinstance(value, int):
            return time.strftime("%d %b %Y at %H:%M", time.gmtime(value))

        # old formatter
        '''
        if format == 'full':
            format = "EEEE, d. MMMM y 'at' HH:mm"
        elif format == 'medium':
            format = "EE dd.MM.y HH:mm"
        # return babel.dates.format_datetime(value, format)
        # return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(value))
        return time.strftime("%d %b %Y at %H:%M", time.gmtime(value))
        # return time.ctime(value)
        '''

    @app.template_filter()
    def forum_title(value):
        if '|' in value:
            parts = value.split('|')
            return f'{parts[0]}\n{parts[1]}'

    app.register_blueprint(home, url_prefix='/')
    app.register_blueprint(forum, url_prefix='/forum')
    return app
